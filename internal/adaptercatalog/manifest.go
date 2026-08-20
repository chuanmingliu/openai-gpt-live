// Package adaptercatalog validates the server-owned, closed-world adapter
// authority compiled into an asynchronous worker.
package adaptercatalog

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/reasoner"
)

var (
	ErrInvalidManifest   = errors.New("adapter manifest is invalid")
	ErrBindingExpansion  = errors.New("adapter binding expands authority")
	ErrInstalledMismatch = errors.New("installed adapter identity mismatch")
)

const (
	ApprovalNone     = "none"
	ApprovalRequired = "required"
	TransportNative  = "native"
	TransportMCP     = "mcp"
	HealthNone       = "none"
	HealthRequired   = "required"
)

const (
	maxIDBytes      = 256
	maxSchemaBytes  = 64 << 10
	maxPayloadBytes = 256 << 10
	minTimeout      = 100 * time.Millisecond
	maxTimeout      = 30 * time.Second
	maxConcurrency  = 64
	maxSecretRefs   = 64
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$`)

// Manifest is reviewed server metadata. It deliberately has no command,
// executable, URL, or arbitrary transport configuration.
type Manifest struct {
	AdapterID         string                  `json:"adapter_id"`
	AdapterVersion    string                  `json:"adapter_version"`
	RegistryVersion   string                  `json:"registry_version"`
	Tool              reasoner.ToolDefinition `json:"tool"`
	ReadOnly          bool                    `json:"read_only"`
	SideEffecting     bool                    `json:"side_effecting"`
	ParallelSafe      bool                    `json:"parallel_safe"`
	StableIdempotency bool                    `json:"stable_idempotency"`
	Approval          string                  `json:"approval"`
	Timeout           time.Duration           `json:"-"`
	MaxConcurrency    int                     `json:"max_concurrency"`
	MaxArgumentBytes  int                     `json:"max_argument_bytes"`
	MaxResultBytes    int                     `json:"max_result_bytes"`
	DestinationID     string                  `json:"destination_id"`
	TransportClass    string                  `json:"transport_class"`
	SecretRefs        []string                `json:"secret_refs,omitempty"`
	HealthClass       string                  `json:"health_class"`
}

func (manifest Manifest) clone() Manifest {
	manifest.Tool = manifest.Tool.Clone()
	manifest.SecretRefs = append([]string(nil), manifest.SecretRefs...)
	return manifest
}

// Binding can only disable or narrow reviewed manifest limits. Zero values on
// an enabled binding mean "use the reviewed manifest limit".
type Binding struct {
	Enabled        bool          `json:"enabled"`
	Timeout        time.Duration `json:"timeout"`
	MaxConcurrency int           `json:"max_concurrency"`
	MaxResultBytes int           `json:"max_result_bytes"`
}

func ValidateBinding(manifest Manifest, binding Binding) (Binding, error) {
	if !binding.Enabled {
		return Binding{}, nil
	}
	if binding.Timeout == 0 {
		binding.Timeout = manifest.Timeout
	}
	if binding.MaxConcurrency == 0 {
		binding.MaxConcurrency = manifest.MaxConcurrency
	}
	if binding.MaxResultBytes == 0 {
		binding.MaxResultBytes = manifest.MaxResultBytes
	}
	if binding.Timeout < minTimeout || binding.Timeout > manifest.Timeout || binding.MaxConcurrency < 1 || binding.MaxConcurrency > manifest.MaxConcurrency || binding.MaxResultBytes < 1 || binding.MaxResultBytes > manifest.MaxResultBytes {
		return Binding{}, ErrBindingExpansion
	}
	return binding, nil
}

type manifestWire struct {
	AdapterID         string                  `json:"adapter_id"`
	AdapterVersion    string                  `json:"adapter_version"`
	RegistryVersion   string                  `json:"registry_version"`
	Tool              reasoner.ToolDefinition `json:"tool"`
	ReadOnly          bool                    `json:"read_only"`
	SideEffecting     bool                    `json:"side_effecting"`
	ParallelSafe      bool                    `json:"parallel_safe"`
	StableIdempotency bool                    `json:"stable_idempotency"`
	Approval          string                  `json:"approval"`
	TimeoutMS         int64                   `json:"timeout_ms"`
	MaxConcurrency    int                     `json:"max_concurrency"`
	MaxArgumentBytes  int                     `json:"max_argument_bytes"`
	MaxResultBytes    int                     `json:"max_result_bytes"`
	DestinationID     string                  `json:"destination_id"`
	TransportClass    string                  `json:"transport_class"`
	SecretRefs        []string                `json:"secret_refs,omitempty"`
	HealthClass       string                  `json:"health_class"`
}

func manifestToWire(manifest Manifest) manifestWire {
	return manifestWire{
		AdapterID: manifest.AdapterID, AdapterVersion: manifest.AdapterVersion,
		RegistryVersion: manifest.RegistryVersion, Tool: manifest.Tool.Clone(),
		ReadOnly: manifest.ReadOnly, SideEffecting: manifest.SideEffecting,
		ParallelSafe: manifest.ParallelSafe, StableIdempotency: manifest.StableIdempotency,
		Approval: manifest.Approval, TimeoutMS: manifest.Timeout.Milliseconds(),
		MaxConcurrency: manifest.MaxConcurrency, MaxArgumentBytes: manifest.MaxArgumentBytes,
		MaxResultBytes: manifest.MaxResultBytes, DestinationID: manifest.DestinationID,
		TransportClass: manifest.TransportClass, SecretRefs: append([]string(nil), manifest.SecretRefs...),
		HealthClass: manifest.HealthClass,
	}
}

func DecodeManifest(raw []byte) (Manifest, error) {
	if len(raw) == 0 || len(raw) > maxPayloadBytes || !utf8.Valid(raw) || !json.Valid(raw) {
		return Manifest{}, ErrInvalidManifest
	}
	if err := rejectDuplicateKeys(raw); err != nil {
		return Manifest{}, ErrInvalidManifest
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var wire manifestWire
	if err := decoder.Decode(&wire); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return Manifest{}, ErrInvalidManifest
	}
	manifest := Manifest{
		AdapterID: wire.AdapterID, AdapterVersion: wire.AdapterVersion, RegistryVersion: wire.RegistryVersion,
		Tool: wire.Tool, ReadOnly: wire.ReadOnly, SideEffecting: wire.SideEffecting,
		ParallelSafe: wire.ParallelSafe, StableIdempotency: wire.StableIdempotency,
		Approval: wire.Approval, Timeout: time.Duration(wire.TimeoutMS) * time.Millisecond,
		MaxConcurrency: wire.MaxConcurrency, MaxArgumentBytes: wire.MaxArgumentBytes,
		MaxResultBytes: wire.MaxResultBytes, DestinationID: wire.DestinationID,
		TransportClass: wire.TransportClass, SecretRefs: append([]string(nil), wire.SecretRefs...),
		HealthClass: wire.HealthClass,
	}
	if err := validateManifest(manifest, nil, false); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func validateManifest(manifest Manifest, knownSecrets map[string]struct{}, checkSecrets bool) error {
	if !validID(manifest.AdapterID) || !validID(manifest.AdapterVersion) || !validID(manifest.RegistryVersion) || !validID(manifest.DestinationID) || len(manifest.Tool.Parameters) == 0 || len(manifest.Tool.Parameters) > maxSchemaBytes {
		return ErrInvalidManifest
	}
	if manifest.Approval != ApprovalNone && manifest.Approval != ApprovalRequired || manifest.TransportClass != TransportNative && manifest.TransportClass != TransportMCP || manifest.HealthClass != HealthNone && manifest.HealthClass != HealthRequired {
		return ErrInvalidManifest
	}
	if manifest.Timeout < minTimeout || manifest.Timeout > maxTimeout || manifest.MaxConcurrency < 1 || manifest.MaxConcurrency > maxConcurrency || manifest.MaxArgumentBytes < 1 || manifest.MaxArgumentBytes > maxPayloadBytes || manifest.MaxResultBytes < 1 || manifest.MaxResultBytes > maxPayloadBytes {
		return ErrInvalidManifest
	}
	if manifest.ReadOnly == manifest.SideEffecting || manifest.ParallelSafe && !manifest.ReadOnly || manifest.SideEffecting && (!manifest.StableIdempotency || manifest.Approval != ApprovalRequired) {
		return ErrInvalidManifest
	}
	probe := reasoner.Request{Model: "adapter-catalog-validation", Messages: []reasoner.Message{{Role: reasoner.RoleUser, Content: "validate"}}, Tools: []reasoner.ToolDefinition{manifest.Tool.Clone()}, MaxOutputTokens: 1}
	if err := probe.Validate(); err != nil {
		return fmt.Errorf("%w: tool schema", ErrInvalidManifest)
	}
	if len(manifest.SecretRefs) > maxSecretRefs {
		return ErrInvalidManifest
	}
	seen := make(map[string]struct{}, len(manifest.SecretRefs))
	for _, ref := range manifest.SecretRefs {
		if !validID(ref) {
			return ErrInvalidManifest
		}
		if _, duplicate := seen[ref]; duplicate {
			return ErrInvalidManifest
		}
		seen[ref] = struct{}{}
		if checkSecrets {
			if _, ok := knownSecrets[ref]; !ok {
				return ErrInvalidManifest
			}
		}
	}
	return nil
}

func validID(value string) bool {
	return len(value) > 0 && len(value) <= maxIDBytes && utf8.ValidString(value) && identifierPattern.MatchString(value)
}

func rejectDuplicateKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	return scanJSONValue(decoder)
}

func scanJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return ErrInvalidManifest
			}
			if _, duplicate := seen[key]; duplicate {
				return ErrInvalidManifest
			}
			seen[key] = struct{}{}
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
	default:
		return ErrInvalidManifest
	}
	closing, err := decoder.Token()
	if err != nil {
		return err
	}
	if closing != json.Delim(map[json.Delim]rune{'{': '}', '[': ']'}[delimiter]) {
		return ErrInvalidManifest
	}
	return nil
}

func sortedManifestClones(values []Manifest) []Manifest {
	clones := make([]Manifest, len(values))
	for index := range values {
		clones[index] = values[index].clone()
		sort.Strings(clones[index].SecretRefs)
	}
	sort.Slice(clones, func(i, j int) bool {
		return strings.Join([]string{clones[i].AdapterID, clones[i].AdapterVersion}, "\x00") < strings.Join([]string{clones[j].AdapterID, clones[j].AdapterVersion}, "\x00")
	})
	return clones
}
