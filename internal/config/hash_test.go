package config

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHashIsStableAndIgnoresSecretReferences(t *testing.T) {
	a, err := loadFixture(t, "mode: local\n", nil)
	if err != nil {
		t.Fatal(err)
	}
	a.Auth.OIDCRequiredScopes = []string{"z", "a"}
	b := a
	b.Auth.OIDCRequiredScopes = []string{"a", "z"}
	b.Gateway.Voice.APIKeyEnv = "OTHER_SECRET"
	ha, err := Hash(a)
	if err != nil {
		t.Fatal(err)
	}
	hb, err := Hash(b)
	if err != nil {
		t.Fatal(err)
	}
	if ha != hb {
		t.Fatalf("semantic hashes differ: %s %s", ha, hb)
	}
	b.Gateway.AdvertisedModel += "-changed"
	hb, err = Hash(b)
	if err != nil {
		t.Fatal(err)
	}
	if ha == hb {
		t.Fatal("meaningful change did not alter hash")
	}
}

func TestHashIgnoresReconnectSecretReferenceButBindsKeyIdentity(t *testing.T) {
	cfg, err := loadFixture(t, "mode: local\ngateway:\n  reconnect: {hmac_key_env: RECONNECT_A, hmac_key_id: reconnect-v1}\n", func(string) (string, bool) {
		return base64.StdEncoding.EncodeToString(make([]byte, 32)), true
	})
	if err != nil {
		t.Fatal(err)
	}
	a, err := Hash(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Gateway.Reconnect.HMACKeyEnv = "RECONNECT_B"
	b, err := Hash(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatal("secret reference name changed canonical hash")
	}
	cfg.Gateway.Reconnect.HMACKeyID = "reconnect-v2"
	c, err := Hash(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if a == c {
		t.Fatal("reconnect key identity did not change canonical hash")
	}
}

func TestToolCatalogChangesConfigHash(t *testing.T) {
	a, b := validConfig(t), validConfig(t)
	b.Tools.WebSearch.MaxResults = a.Tools.WebSearch.MaxResults + 1
	assert.NotEqual(t, mustHash(t, a), mustHash(t, b))
}

func TestValidateExpectedDoesNotDiscloseValue(t *testing.T) {
	if err := ValidateExpected("v1:"+string(make([]byte, 64)), "secret"); err == nil || err.Error() == "secret" {
		t.Fatalf("unexpected error %v", err)
	}
}

func TestHashFileIgnoresSemanticYAMLOrdering(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.yaml")
	b := filepath.Join(dir, "b.yaml")
	if err := os.WriteFile(a, []byte("mode: local\nauth:\n  static_scopes: [z, a]\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(b, []byte("auth:\n  static_scopes:\n    - a\n    - z\nmode: local\n"), 0600); err != nil {
		t.Fatal(err)
	}
	ha, err := HashFile(a)
	if err != nil {
		t.Fatal(err)
	}
	hb, err := HashFile(b)
	if err != nil {
		t.Fatal(err)
	}
	if ha != hb {
		t.Fatalf("%s != %s", ha, hb)
	}
}

func TestGatewayExampleConfigHashGolden(t *testing.T) {
	hash, err := HashFile(filepath.Join("..", "..", "configs", "gateway.example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	const want = "v1:94737ba565f3157a40f665bdaea0ea4feac95a9fa069cff68a7bf33c47be8ea3"
	if hash != want {
		t.Fatalf("hash=%s want=%s", hash, want)
	}
}

func mustHash(t *testing.T, cfg Config) string {
	t.Helper()
	hash, err := Hash(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return hash
}
