package adaptercatalog

import (
	"errors"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/reasoner"
)

func TestBindingMayTightenButNeverWidenManifest(t *testing.T) {
	manifest := testManifest()
	if _, err := ValidateBinding(manifest, Binding{Enabled: true, Timeout: 6 * time.Second, MaxResultBytes: 64 << 10, MaxConcurrency: 8}); !errors.Is(err, ErrBindingExpansion) {
		t.Fatalf("widen timeout: got %v, want %v", err, ErrBindingExpansion)
	}
	got, err := ValidateBinding(manifest, Binding{Enabled: true, Timeout: time.Second, MaxResultBytes: 8 << 10, MaxConcurrency: 2})
	if err != nil {
		t.Fatalf("tighten binding: %v", err)
	}
	if got.Timeout != time.Second || got.MaxResultBytes != 8<<10 || got.MaxConcurrency != 2 {
		t.Fatalf("unexpected tightened binding: %#v", got)
	}
}

func TestDecodeManifestIsStrictAndRejectsExecutableConfiguration(t *testing.T) {
	for name, raw := range map[string]string{
		"command":   `{"adapter_id":"x","command":"npx"}`,
		"url":       `{"adapter_id":"x","url":"https://example.invalid"}`,
		"duplicate": `{"adapter_id":"x","adapter_id":"y"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeManifest([]byte(raw)); err == nil {
				t.Fatal("expected strict manifest rejection")
			}
		})
	}
}

func TestManifestRejectsUnsafeMetadataAndUnknownSecret(t *testing.T) {
	manifest := testManifest()
	manifest.SideEffecting = true
	manifest.ReadOnly = false
	manifest.StableIdempotency = false
	manifest.Approval = ApprovalNone
	if _, err := New("registry-v1", []Manifest{manifest}, []string{"crm_token"}); err == nil {
		t.Fatal("expected unsafe side-effect rejection")
	}
	manifest = testManifest()
	manifest.SecretRefs = []string{"unreviewed_secret"}
	if _, err := New("registry-v1", []Manifest{manifest}, []string{"crm_token"}); err == nil {
		t.Fatal("expected unknown secret rejection")
	}
}

func testManifest() Manifest {
	return Manifest{
		AdapterID:        "knowledge_search",
		AdapterVersion:   "v1",
		RegistryVersion:  "registry-v1",
		Tool:             reasoner.ToolDefinition{Name: "knowledge_search", Description: "Search reviewed knowledge.", Parameters: []byte(`{"type":"object","additionalProperties":false}`)},
		ReadOnly:         true,
		ParallelSafe:     true,
		Approval:         ApprovalNone,
		Timeout:          5 * time.Second,
		MaxConcurrency:   8,
		MaxArgumentBytes: 16 << 10,
		MaxResultBytes:   64 << 10,
		DestinationID:    "knowledge_store",
		TransportClass:   TransportNative,
		HealthClass:      HealthRequired,
		SecretRefs:       []string{"crm_token"},
	}
}
