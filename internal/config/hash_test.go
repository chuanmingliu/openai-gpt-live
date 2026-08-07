package config

import (
	"os"
	"path/filepath"
	"testing"
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
	const want = "v1:52dc8f4c4840f900648b308e61c3d45ccd0c1d1ef283b46f670483adc19e5861"
	if hash != want {
		t.Fatalf("hash=%s want=%s", hash, want)
	}
}
