package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestReadDocumentsFindsNamespace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "render.yaml")
	if err := os.WriteFile(path, []byte("apiVersion: v1\nkind: Namespace\nmetadata:\n  name: gpt-live\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	docs, err := readDocuments(path)
	if err != nil {
		t.Fatal(err)
	}
	if found := find(docs, "Namespace", "gpt-live"); found == nil {
		t.Fatal("namespace not found")
	}
}

func TestDecodedIdentityRejectsEquivalentQuotedDuplicate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "duplicate.yaml")
	raw := "apiVersion: v1\nkind: Namespace\nmetadata:\n  name: gpt-live\n---\napiVersion: v1\nkind: Namespace\nmetadata:\n  name: \"gpt-\\x6cive\" # equivalent quoted scalar\n"
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	docs, err := readDocuments(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateObjectSet(docs, "local"); err == nil || !strings.Contains(err.Error(), "duplicate Kubernetes object identity") {
		t.Fatalf("equivalent quoted duplicate error = %v", err)
	}
}

func TestReadDocumentsRejectsDuplicateYAMLKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "duplicate-key.yaml")
	raw := "apiVersion: v1\nkind: Namespace\nmetadata:\n  name: gpt-live\n  name: shadow\n"
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readDocuments(path); err == nil {
		t.Fatal("duplicate YAML mapping key was accepted")
	}
}

func TestReadDocumentsRejectsAmbiguousYAMLAlias(t *testing.T) {
	path := filepath.Join(t.TempDir(), "alias.yaml")
	raw := "apiVersion: v1\nkind: Namespace\nbase: &metadata\n  name: gpt-live\nmetadata: *metadata\n"
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readDocuments(path); err == nil {
		t.Fatal("ambiguous YAML alias was accepted")
	}
}

func setValidProductionInputs(t *testing.T) {
	t.Helper()
	values := map[string]string{
		"LIVE_PRODUCTION_HOST":               "gpt-live.acme.com",
		"LIVE_PRODUCTION_TLS_SECRET":         "gpt-live-tls-v7",
		"LIVE_PRODUCTION_TRUSTED_PROXY_CIDR": "10.10.0.0/16",
		"LIVE_PRODUCTION_EGRESS_PROXY_CIDR":  "10.20.0.0/16",
		"LIVE_PRODUCTION_VOICE_URL":          "wss://voice.acme.com/v1/realtime",
		"LIVE_PRODUCTION_OIDC_ISSUER":        "https://oidc.acme.com",
		"LIVE_PRODUCTION_GATEWAY_IMAGE":      "registry.acme.com/gateway@sha256:4d1b72371c42a69bd949048883897aa673059f2e7b1cc992a650584132aae8f3",
		"LIVE_PRODUCTION_WORKER_IMAGE":       "registry.acme.com/worker@sha256:49e907719fa53f1b00e43d8721620f42979c31117ed46daa3fadf320aef94b4e",
		"LIVE_PRODUCTION_MIGRATE_IMAGE":      "registry.acme.com/migrate@sha256:1488a529509b2d9d0135713e75afdd3b3730b2884add211c6267980c87275078",
	}
	for name, value := range values {
		t.Setenv(name, value)
	}
}

func TestValidateProductionInputsRejectsObviousAndNonImmutableValues(t *testing.T) {
	tests := []struct {
		name, variable, value string
	}{
		{"missing host", "LIVE_PRODUCTION_HOST", ""},
		{"example host", "LIVE_PRODUCTION_HOST", "gpt-live.example.com"},
		{"reserved CIDR", "LIVE_PRODUCTION_EGRESS_PROXY_CIDR", "192.0.2.0/24"},
		{"repeated digest", "LIVE_PRODUCTION_GATEWAY_IMAGE", "registry.acme.com/gateway@sha256:" + strings.Repeat("a", 64)},
		{"dummy periodic digest", "LIVE_PRODUCTION_GATEWAY_IMAGE", "registry.acme.com/gateway@sha256:" + strings.Repeat("deadbeef", 8)},
		{"obvious repository", "LIVE_PRODUCTION_GATEWAY_IMAGE", "registry.example.com/gateway@sha256:4d1b72371c42a69bd949048883897aa673059f2e7b1cc992a650584132aae8f3"},
		{"URL scheme", "LIVE_PRODUCTION_GATEWAY_IMAGE", "https://registry.acme.com/gateway@sha256:4d1b72371c42a69bd949048883897aa673059f2e7b1cc992a650584132aae8f3"},
		{"leading whitespace", "LIVE_PRODUCTION_GATEWAY_IMAGE", " registry.acme.com/gateway@sha256:4d1b72371c42a69bd949048883897aa673059f2e7b1cc992a650584132aae8f3"},
		{"tag and digest", "LIVE_PRODUCTION_GATEWAY_IMAGE", "registry.acme.com/gateway:v1@sha256:4d1b72371c42a69bd949048883897aa673059f2e7b1cc992a650584132aae8f3"},
		{"empty path component", "LIVE_PRODUCTION_GATEWAY_IMAGE", "registry.acme.com/team//gateway@sha256:4d1b72371c42a69bd949048883897aa673059f2e7b1cc992a650584132aae8f3"},
		{"implicit registry", "LIVE_PRODUCTION_GATEWAY_IMAGE", "gateway@sha256:4d1b72371c42a69bd949048883897aa673059f2e7b1cc992a650584132aae8f3"},
		{"ambiguous registry component", "LIVE_PRODUCTION_GATEWAY_IMAGE", "team/gateway@sha256:4d1b72371c42a69bd949048883897aa673059f2e7b1cc992a650584132aae8f3"},
		{"uppercase repository", "LIVE_PRODUCTION_GATEWAY_IMAGE", "registry.acme.com/Gateway@sha256:4d1b72371c42a69bd949048883897aa673059f2e7b1cc992a650584132aae8f3"},
		{"uppercase digest", "LIVE_PRODUCTION_GATEWAY_IMAGE", "registry.acme.com/gateway@sha256:4D1b72371c42a69bd949048883897aa673059f2e7b1cc992a650584132aae8f3"},
		{"invalid port", "LIVE_PRODUCTION_GATEWAY_IMAGE", "registry.acme.com:70000/gateway@sha256:4d1b72371c42a69bd949048883897aa673059f2e7b1cc992a650584132aae8f3"},
		{"duplicate digest marker", "LIVE_PRODUCTION_GATEWAY_IMAGE", "registry.acme.com/gateway@sha256:4d1b72371c42a69bd949048883897aa673059f2e7b1cc992a650584132aae8f3@sha256:4d1b72371c42a69bd949048883897aa673059f2e7b1cc992a650584132aae8f3"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setValidProductionInputs(t)
			t.Setenv(test.variable, test.value)
			if err := validateProductionInputs("deploy"); err == nil {
				t.Fatal("invalid production inputs were accepted")
			}
		})
	}
}

func TestValidateProductionInputsAcceptsReviewedValues(t *testing.T) {
	setValidProductionInputs(t)
	if err := validateProductionInputs("deploy"); err != nil {
		t.Fatal(err)
	}
}

func TestImageRepositoryLengthBoundary(t *testing.T) {
	for _, test := range []struct {
		length int
		want   bool
	}{{255, true}, {256, false}} {
		t.Run(strconv.Itoa(test.length), func(t *testing.T) {
			_, _, ok := parseImage(imageWithRepositoryLength(test.length))
			if ok != test.want {
				t.Fatalf("repository length %d accepted=%v, want %v", test.length, ok, test.want)
			}
			setValidProductionInputs(t)
			t.Setenv("LIVE_PRODUCTION_GATEWAY_IMAGE", imageWithRepositoryLength(test.length))
			if err := validateProductionInputs("deploy"); (err == nil) != test.want {
				t.Fatalf("input repository length %d error=%v", test.length, err)
			}
		})
	}
}

func TestValidateProductionInputsRejectsDuplicateImageDigest(t *testing.T) {
	setValidProductionInputs(t)
	t.Setenv("LIVE_PRODUCTION_WORKER_IMAGE", os.Getenv("LIVE_PRODUCTION_GATEWAY_IMAGE"))
	if err := validateProductionInputs("deploy"); err == nil {
		t.Fatal("duplicate production image digest was accepted")
	}
}
