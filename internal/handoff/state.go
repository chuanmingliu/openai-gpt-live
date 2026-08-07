package handoff

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	maxUserRequestBytes  = 16 * 1024
	maxObjectiveBytes    = 8 * 1024
	maxContextRefBytes   = 2 * 1024
	maxJSONBytes         = 64 * 1024
	maxToolNames         = 64
	maxToolNameBytes     = 256
	maxIdentityBytes     = 256
	maxContextHashBytes  = 512
	maxPresentationBytes = 2 * 1024
	maxReasonBytes       = 2 * 1024
	maxSummaryBytes      = 16 * 1024
	maxJSONDepth         = 64
	maxPriority          = 1000
)

func ValidateTransition(from, to State) error {
	if !validState(from) || !validState(to) || from == to {
		if terminalState(from) {
			return errors.Join(ErrInvalidTransition, ErrTerminalState)
		}
		return ErrInvalidTransition
	}
	if terminalState(from) {
		return errors.Join(ErrInvalidTransition, ErrTerminalState)
	}
	allowed := false
	switch from {
	case StatePending:
		allowed = to == StateLeased || to == StateCancelled || to == StateExpired
	case StateLeased:
		allowed = to == StateRunning || to == StateCancelled || to == StateExpired
	case StateRunning:
		allowed = to == StateNeedsInput || to == StateSucceeded || to == StateFailed || to == StateCancelled || to == StateExpired
	case StateNeedsInput:
		allowed = to == StateRunning || to == StateCancelled || to == StateExpired
	}
	if !allowed {
		return ErrInvalidTransition
	}
	return nil
}

func validState(state State) bool {
	switch state {
	case StatePending, StateLeased, StateRunning, StateNeedsInput, StateSucceeded, StateFailed, StateCancelled, StateExpired:
		return true
	default:
		return false
	}
}

func terminalState(state State) bool {
	return state == StateSucceeded || state == StateFailed || state == StateCancelled || state == StateExpired
}

func ValidateCreateRequest(request CreateRequest, now time.Time) (CreateRequest, error) {
	return normalizeCreateRequest(request, now, true)
}

func normalizeCreateRequest(request CreateRequest, now time.Time, checkFuture bool) (CreateRequest, error) {
	request = cloneRequest(request)
	if err := validateScope(request.Scope); err != nil {
		return CreateRequest{}, err
	}
	if err := boundedRequired("session id", string(request.SessionID), maxIdentityBytes); err != nil {
		return CreateRequest{}, err
	}
	if err := boundedRequired("source turn id", request.SourceTurn.ID, maxIdentityBytes); err != nil || request.SourceTurn.Revision == 0 {
		return CreateRequest{}, invalid("source turn", err)
	}
	if err := boundedRequired("user request", request.UserRequest, maxUserRequestBytes); err != nil {
		return CreateRequest{}, err
	}
	if err := boundedRequired("objective", request.Objective, maxObjectiveBytes); err != nil {
		return CreateRequest{}, err
	}
	if err := boundedRequired("context ref", request.ContextRef, maxContextRefBytes); err != nil {
		return CreateRequest{}, err
	}
	if err := boundedRequired("context hash", request.ContextHash, maxContextHashBytes); err != nil {
		return CreateRequest{}, err
	}
	if request.Deadline.IsZero() || (checkFuture && !request.Deadline.After(now)) {
		return CreateRequest{}, invalid("deadline must be in the future", nil)
	}
	request.Deadline = request.Deadline.UTC()
	if request.TokenBudget <= 0 {
		return CreateRequest{}, invalid("token budget must be positive", nil)
	}
	if request.Priority < -maxPriority || request.Priority > maxPriority {
		return CreateRequest{}, invalid("priority out of range", nil)
	}
	if len(request.ToolAllowlist) > maxToolNames {
		return CreateRequest{}, invalid("too many tool names", nil)
	}
	seen := make(map[string]struct{}, len(request.ToolAllowlist))
	for _, name := range request.ToolAllowlist {
		if err := boundedRequired("tool name", name, maxToolNameBytes); err != nil {
			return CreateRequest{}, err
		}
		if _, duplicate := seen[name]; duplicate {
			return CreateRequest{}, invalid("duplicate tool name", nil)
		}
		seen[name] = struct{}{}
	}
	sort.Strings(request.ToolAllowlist)
	if len(request.ApprovalPolicy) == 0 {
		return CreateRequest{}, invalid("approval policy is required", nil)
	}
	policy, err := canonicalJSON(request.ApprovalPolicy, maxJSONBytes, maxJSONDepth)
	if err != nil {
		return CreateRequest{}, invalid("approval policy", err)
	}
	request.ApprovalPolicy = policy
	for name, value := range map[string]string{
		"model version": request.Versions.Model, "prompt version": request.Versions.Prompt,
		"tool registry version": request.Versions.ToolRegistry, "config version": request.Versions.Config,
	} {
		if err := boundedRequired(name, value, maxIdentityBytes); err != nil {
			return CreateRequest{}, err
		}
	}
	if err := boundedOptional("presentation hint", request.PresentationHint, maxPresentationBytes); err != nil {
		return CreateRequest{}, err
	}
	if err := boundedRequired("idempotency key", request.IdempotencyKey, maxIdentityBytes); err != nil {
		return CreateRequest{}, err
	}
	return request, nil
}

func CanonicalRequestHash(request CreateRequest) (string, error) {
	normalized, err := normalizeCreateRequest(request, time.Time{}, false)
	if err != nil {
		return "", err
	}
	canonical := struct {
		Scope            Scope           `json:"scope"`
		SessionID        string          `json:"session_id"`
		SourceTurnID     string          `json:"source_turn_id"`
		SourceRevision   uint32          `json:"source_turn_revision"`
		UserRequest      string          `json:"user_request"`
		Objective        string          `json:"objective"`
		ContextRef       string          `json:"context_ref"`
		ContextHash      string          `json:"context_hash"`
		Deadline         string          `json:"deadline"`
		TokenBudget      int             `json:"token_budget"`
		Priority         int16           `json:"priority"`
		ToolAllowlist    []string        `json:"tool_allowlist"`
		ApprovalPolicy   json.RawMessage `json:"approval_policy"`
		Versions         Versions        `json:"versions"`
		PresentationHint string          `json:"presentation_hint"`
	}{
		Scope: normalized.Scope, SessionID: string(normalized.SessionID), SourceTurnID: normalized.SourceTurn.ID,
		SourceRevision: normalized.SourceTurn.Revision, UserRequest: normalized.UserRequest, Objective: normalized.Objective,
		ContextRef: normalized.ContextRef, ContextHash: normalized.ContextHash, Deadline: normalized.Deadline.Format(time.RFC3339Nano),
		TokenBudget: normalized.TokenBudget, Priority: normalized.Priority, ToolAllowlist: normalized.ToolAllowlist,
		ApprovalPolicy: normalized.ApprovalPolicy, Versions: normalized.Versions, PresentationHint: normalized.PresentationHint,
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", invalid("canonical request", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func ValidateResult(result Result) (Result, error) {
	if err := boundedRequired("mutation id", result.MutationID, maxIdentityBytes); err != nil {
		return Result{}, err
	}
	if err := boundedRequired("result summary", result.Summary, maxSummaryBytes); err != nil {
		return Result{}, err
	}
	structured, err := canonicalJSON(result.Structured, maxJSONBytes, maxJSONDepth)
	if err != nil {
		return Result{}, invalid("structured result", err)
	}
	usage, err := canonicalJSON(result.Usage, maxJSONBytes, maxJSONDepth)
	if err != nil {
		return Result{}, invalid("result usage", err)
	}
	result.Structured = structured
	result.Usage = usage
	return result, nil
}

func ValidateFailure(failure Failure) (Failure, error) {
	if err := boundedRequired("mutation id", failure.MutationID, maxIdentityBytes); err != nil {
		return Failure{}, err
	}
	if err := boundedRequired("failure code", failure.Code, maxIdentityBytes); err != nil {
		return Failure{}, err
	}
	if err := boundedRequired("failure message", failure.Message, maxSummaryBytes); err != nil {
		return Failure{}, err
	}
	return failure, nil
}

func validateScope(scope Scope) error {
	if err := boundedRequired("tenant id", scope.TenantID, maxIdentityBytes); err != nil {
		return err
	}
	if err := boundedRequired("user id", scope.UserID, maxIdentityBytes); err != nil {
		return err
	}
	return boundedRequired("conversation id", string(scope.ConversationID), maxIdentityBytes)
}

func boundedRequired(name, value string, limit int) error {
	if strings.TrimSpace(value) == "" {
		return invalid(name+" is required", nil)
	}
	return boundedOptional(name, value, limit)
}

func boundedOptional(name, value string, limit int) error {
	if !utf8.ValidString(value) || len(value) > limit {
		return invalid(name+" is invalid or too large", nil)
	}
	return nil
}

func invalid(message string, cause error) error {
	if cause == nil {
		return fmt.Errorf("%w: %s", ErrInvalidRequest, message)
	}
	return fmt.Errorf("%w: %s: %v", ErrInvalidRequest, message, cause)
}

type jsonObject map[string]any
type jsonArray []any
type jsonNumber string

func canonicalJSON(raw []byte, maxBytes, maxDepth int) (json.RawMessage, error) {
	if len(raw) == 0 || len(raw) > maxBytes || !utf8.Valid(raw) {
		return nil, errors.New("invalid JSON size or encoding")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	value, err := readJSONValue(decoder, 1, maxDepth)
	if err != nil {
		return nil, err
	}
	if token, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("trailing JSON token %v", token)
	}
	var out bytes.Buffer
	if err := writeCanonicalJSON(&out, value); err != nil {
		return nil, err
	}
	if out.Len() > maxBytes {
		return nil, errors.New("canonical JSON too large")
	}
	return cloneBytes(json.RawMessage(out.Bytes())), nil
}

func readJSONValue(decoder *json.Decoder, depth, maxDepth int) (any, error) {
	if depth > maxDepth {
		return nil, errors.New("JSON nesting too deep")
	}
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	switch value := token.(type) {
	case json.Delim:
		switch value {
		case '{':
			object := jsonObject{}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return nil, err
				}
				key, ok := keyToken.(string)
				if !ok {
					return nil, errors.New("JSON object key is not a string")
				}
				if _, exists := object[key]; exists {
					return nil, fmt.Errorf("duplicate JSON field %q", key)
				}
				child, err := readJSONValue(decoder, depth+1, maxDepth)
				if err != nil {
					return nil, err
				}
				object[key] = child
			}
			if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
				return nil, errors.New("unterminated JSON object")
			}
			return object, nil
		case '[':
			array := jsonArray{}
			for decoder.More() {
				child, err := readJSONValue(decoder, depth+1, maxDepth)
				if err != nil {
					return nil, err
				}
				array = append(array, child)
			}
			if end, err := decoder.Token(); err != nil || end != json.Delim(']') {
				return nil, errors.New("unterminated JSON array")
			}
			return array, nil
		default:
			return nil, errors.New("unexpected JSON delimiter")
		}
	case json.Number:
		normalized, err := normalizeJSONNumber(value.String())
		if err != nil {
			return nil, err
		}
		return jsonNumber(normalized), nil
	case string, bool, nil:
		return value, nil
	default:
		return nil, errors.New("unexpected JSON value")
	}
}

func writeCanonicalJSON(out *bytes.Buffer, value any) error {
	switch value := value.(type) {
	case jsonObject:
		keys := make([]string, 0, len(value))
		for key := range value {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		out.WriteByte('{')
		for i, key := range keys {
			if i > 0 {
				out.WriteByte(',')
			}
			encodedKey, _ := json.Marshal(key)
			out.Write(encodedKey)
			out.WriteByte(':')
			if err := writeCanonicalJSON(out, value[key]); err != nil {
				return err
			}
		}
		out.WriteByte('}')
	case jsonArray:
		out.WriteByte('[')
		for i, item := range value {
			if i > 0 {
				out.WriteByte(',')
			}
			if err := writeCanonicalJSON(out, item); err != nil {
				return err
			}
		}
		out.WriteByte(']')
	case jsonNumber:
		out.WriteString(string(value))
	case string:
		encoded, _ := json.Marshal(value)
		out.Write(encoded)
	case bool:
		if value {
			out.WriteString("true")
		} else {
			out.WriteString("false")
		}
	case nil:
		out.WriteString("null")
	default:
		return errors.New("unsupported canonical JSON value")
	}
	return nil
}

func normalizeJSONNumber(value string) (string, error) {
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsInf(parsed, 0) || math.IsNaN(parsed) {
		return "", errors.New("nonfinite or invalid JSON number")
	}
	negative := strings.HasPrefix(value, "-")
	if negative {
		value = value[1:]
	}
	exponent := 0
	if at := strings.IndexAny(value, "eE"); at >= 0 {
		exponent, err = strconv.Atoi(value[at+1:])
		if err != nil || exponent < -10000 || exponent > 10000 {
			return "", errors.New("JSON number exponent out of range")
		}
		value = value[:at]
	}
	point := strings.IndexByte(value, '.')
	integerDigits := len(value)
	if point >= 0 {
		integerDigits = point
		value = value[:point] + value[point+1:]
	}
	decimalAt := integerDigits + exponent
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
		result = strings.TrimRight(result, "0")
		result = strings.TrimRight(result, ".")
	}
	if negative && result != "0" {
		result = "-" + result
	}
	return result, nil
}
