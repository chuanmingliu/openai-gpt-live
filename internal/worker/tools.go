package worker

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/reasoner"
)

var (
	ErrNonceReuse  = errors.New("worker argument nonce reused")
	ErrNonceSource = errors.New("worker argument nonce source failed")
)

const maxSealsPerArgumentKey = 1 << 16

type ToolExecutionRequest struct {
	InvocationID   string
	IdempotencyKey string
	Arguments      json.RawMessage
}

type ToolExecutor interface {
	Execute(context.Context, ToolExecutionRequest) (json.RawMessage, error)
}

type RegisteredTool struct {
	Definition        reasoner.ToolDefinition
	SideEffecting     bool
	ReadOnly          bool
	ParallelSafe      bool
	StableIdempotency bool
	Executor          ToolExecutor
}

type ToolRegistry interface {
	ToolGate
	Registered(context.Context, string, string) bool
	Lookup(version, name string) (RegisteredTool, bool)
	Definitions(version string, allowlist []string) ([]reasoner.ToolDefinition, error)
}

type immutableToolRegistry struct {
	versions map[string]map[string]RegisteredTool
}

func NewToolRegistry(versions map[string][]RegisteredTool) (ToolRegistry, error) {
	owned := &immutableToolRegistry{versions: make(map[string]map[string]RegisteredTool, len(versions))}
	for version, tools := range versions {
		if strings.TrimSpace(version) == "" || len(version) > 256 || !utf8.ValidString(version) {
			return nil, errors.New("worker tool registry version is invalid")
		}
		byName := make(map[string]RegisteredTool, len(tools))
		for _, tool := range tools {
			if nilValue(tool.Executor) || (tool.SideEffecting && (!tool.StableIdempotency || tool.ReadOnly)) || (tool.ParallelSafe && (!tool.ReadOnly || tool.SideEffecting)) {
				return nil, errors.New("worker tool metadata is invalid")
			}
			definition := tool.Definition.Clone()
			probe := reasoner.Request{Model: "registry-validation", Messages: []reasoner.Message{{Role: reasoner.RoleUser, Content: "validate"}}, Tools: []reasoner.ToolDefinition{definition}, MaxOutputTokens: 1}
			if err := probe.Validate(); err != nil {
				return nil, fmt.Errorf("worker tool definition is invalid: %w", err)
			}
			if _, duplicate := byName[definition.Name]; duplicate {
				return nil, errors.New("worker tool registration is duplicated")
			}
			tool.Definition = definition
			byName[definition.Name] = tool
		}
		owned.versions[version] = byName
	}
	return owned, nil
}

func (r *immutableToolRegistry) Registered(ctx context.Context, version, name string) bool {
	_, ok := r.Metadata(ctx, version, name)
	return ok
}

func (r *immutableToolRegistry) Metadata(ctx context.Context, version, name string) (ToolGateMetadata, bool) {
	if r == nil || ctx == nil || ctx.Err() != nil {
		return ToolGateMetadata{}, false
	}
	tool, ok := r.Lookup(version, name)
	if !ok {
		return ToolGateMetadata{}, false
	}
	return ToolGateMetadata{SideEffecting: tool.SideEffecting}, true
}

func (r *immutableToolRegistry) Lookup(version, name string) (RegisteredTool, bool) {
	if r == nil {
		return RegisteredTool{}, false
	}
	tool, ok := r.versions[version][name]
	if !ok {
		return RegisteredTool{}, false
	}
	tool.Definition = tool.Definition.Clone()
	return tool, true
}

func (r *immutableToolRegistry) Definitions(version string, allowlist []string) ([]reasoner.ToolDefinition, error) {
	if r == nil {
		return nil, ErrToolNotRegistered
	}
	seen := make(map[string]struct{}, len(allowlist))
	definitions := make([]reasoner.ToolDefinition, 0, len(allowlist))
	for _, name := range allowlist {
		if _, duplicate := seen[name]; duplicate {
			return nil, ErrToolNotAllowed
		}
		seen[name] = struct{}{}
		tool, ok := r.Lookup(version, name)
		if !ok {
			return nil, ErrToolNotRegistered
		}
		definitions = append(definitions, tool.Definition.Clone())
	}
	return definitions, nil
}

type SealedArguments struct {
	Ciphertext []byte
	Nonce      []byte
	KeyID      string
}

type ToolArgumentSealer interface {
	Seal([]byte) (SealedArguments, error)
	Close() error
}

type aesArgumentSealer struct {
	keyID  string
	key    []byte
	nonces io.Reader
	mu     sync.Mutex
	used   map[string]struct{}
	closed bool
}

func NewAESArgumentSealer(keyID string, key []byte, nonces io.Reader) (ToolArgumentSealer, error) {
	if strings.TrimSpace(keyID) == "" || len(keyID) > 256 || len(key) != 32 || nilValue(nonces) {
		return nil, errors.New("worker AES argument sealer configuration is invalid")
	}
	return &aesArgumentSealer{keyID: keyID, key: append([]byte(nil), key...), nonces: nonces, used: make(map[string]struct{})}, nil
}

func (s *aesArgumentSealer) Seal(plaintext []byte) (SealedArguments, error) {
	if s == nil || len(plaintext) == 0 || len(plaintext) > 1<<20 {
		return SealedArguments{}, errors.New("worker tool arguments are invalid")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return SealedArguments{}, errors.New("worker argument sealer is closed")
	}
	nonce := make([]byte, 12)
	if _, err := io.ReadFull(s.nonces, nonce); err != nil {
		clear(nonce)
		return SealedArguments{}, fmt.Errorf("%w", ErrNonceSource)
	}
	nonceKey := hex.EncodeToString(nonce)
	if len(s.used) >= maxSealsPerArgumentKey {
		clear(nonce)
		return SealedArguments{}, errors.New("worker argument key seal limit reached")
	}
	if _, exists := s.used[nonceKey]; exists {
		clear(nonce)
		return SealedArguments{}, ErrNonceReuse
	}
	s.used[nonceKey] = struct{}{}
	block, err := aes.NewCipher(s.key)
	if err != nil {
		return SealedArguments{}, errors.New("worker argument cipher failed")
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return SealedArguments{}, errors.New("worker argument cipher failed")
	}
	ciphertext := gcm.Seal(nil, nonce, plaintext, nil)
	return SealedArguments{Ciphertext: ciphertext, Nonce: nonce, KeyID: s.keyID}, nil
}

func (s *aesArgumentSealer) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	clear(s.key)
	s.key = nil
	clear(s.used)
	s.used = nil
	s.closed = true
	return nil
}

type canonicalObject map[string]any
type canonicalNumber string

func canonicalToolArguments(raw json.RawMessage, limit int) (json.RawMessage, error) {
	if limit <= 0 || len(raw) == 0 || len(raw) > limit || !utf8.Valid(raw) {
		return nil, errors.New("worker tool arguments exceed bounds")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	value, err := readCanonicalValue(decoder, 0)
	if err != nil {
		return nil, err
	}
	if _, ok := value.(canonicalObject); !ok {
		return nil, errors.New("worker tool arguments must be an object")
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("worker tool arguments contain trailing data")
	}
	var output bytes.Buffer
	if err := writeCanonicalValue(&output, value); err != nil || output.Len() > limit {
		return nil, errors.New("worker canonical tool arguments exceed bounds")
	}
	return append(json.RawMessage(nil), output.Bytes()...), nil
}

func readCanonicalValue(decoder *json.Decoder, depth int) (any, error) {
	if depth > 64 {
		return nil, errors.New("worker tool arguments are too deep")
	}
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	switch value := token.(type) {
	case json.Delim:
		switch value {
		case '{':
			object := canonicalObject{}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return nil, err
				}
				key, ok := keyToken.(string)
				if !ok {
					return nil, errors.New("worker tool argument key is invalid")
				}
				if _, duplicate := object[key]; duplicate {
					return nil, errors.New("worker tool argument key is duplicated")
				}
				child, err := readCanonicalValue(decoder, depth+1)
				if err != nil {
					return nil, err
				}
				object[key] = child
			}
			if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
				return nil, errors.New("worker tool argument object is unterminated")
			}
			return object, nil
		case '[':
			var array []any
			for decoder.More() {
				child, err := readCanonicalValue(decoder, depth+1)
				if err != nil {
					return nil, err
				}
				array = append(array, child)
			}
			if end, err := decoder.Token(); err != nil || end != json.Delim(']') {
				return nil, errors.New("worker tool argument array is unterminated")
			}
			return array, nil
		default:
			return nil, errors.New("worker tool argument delimiter is invalid")
		}
	case json.Number:
		normalized, err := normalizeToolNumber(value.String())
		return canonicalNumber(normalized), err
	case string, bool, nil:
		return value, nil
	default:
		return nil, errors.New("worker tool argument value is invalid")
	}
}

func writeCanonicalValue(out *bytes.Buffer, value any) error {
	switch value := value.(type) {
	case canonicalObject:
		keys := make([]string, 0, len(value))
		for key := range value {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		out.WriteByte('{')
		for index, key := range keys {
			if index > 0 {
				out.WriteByte(',')
			}
			encoded, _ := json.Marshal(key)
			out.Write(encoded)
			out.WriteByte(':')
			if err := writeCanonicalValue(out, value[key]); err != nil {
				return err
			}
		}
		out.WriteByte('}')
	case []any:
		out.WriteByte('[')
		for index, child := range value {
			if index > 0 {
				out.WriteByte(',')
			}
			if err := writeCanonicalValue(out, child); err != nil {
				return err
			}
		}
		out.WriteByte(']')
	case canonicalNumber:
		out.WriteString(string(value))
	case string, bool, nil:
		encoded, _ := json.Marshal(value)
		out.Write(encoded)
	default:
		return errors.New("worker canonical tool value is invalid")
	}
	return nil
}

func normalizeToolNumber(value string) (string, error) {
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsInf(parsed, 0) || math.IsNaN(parsed) {
		return "", errors.New("worker tool number is invalid")
	}
	negative := strings.HasPrefix(value, "-")
	if negative {
		value = value[1:]
	}
	exponent := 0
	if at := strings.IndexAny(value, "eE"); at >= 0 {
		exponent, err = strconv.Atoi(value[at+1:])
		if err != nil || exponent < -10000 || exponent > 10000 {
			return "", errors.New("worker tool number exponent is invalid")
		}
		value = value[:at]
	}
	decimalAt := len(value) + exponent
	if point := strings.IndexByte(value, '.'); point >= 0 {
		decimalAt = point + exponent
		value = value[:point] + value[point+1:]
	}
	trimmed := strings.TrimLeft(value, "0")
	if trimmed == "" {
		return "0", nil
	}
	decimalAt -= len(value) - len(trimmed)
	value = trimmed
	var result string
	switch {
	case decimalAt <= 0:
		result = "0." + strings.Repeat("0", -decimalAt) + value
	case decimalAt >= len(value):
		result = value + strings.Repeat("0", decimalAt-len(value))
	default:
		result = value[:decimalAt] + "." + value[decimalAt:]
	}
	if strings.Contains(result, ".") {
		result = strings.TrimRight(strings.TrimRight(result, "0"), ".")
	}
	if negative && result != "0" {
		result = "-" + result
	}
	return result, nil
}
