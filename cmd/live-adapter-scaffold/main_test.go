package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/chuanmingliu/openai-gpt-live/internal/adaptercatalog"
)

func TestScaffoldCreatesOneNativeAdapterWithoutExecutableConfig(t *testing.T) {
	root := t.TempDir()
	if err := run([]string{"-root", root, "-name", "policy_lookup"}); err != nil {
		t.Fatalf("run scaffold: %v", err)
	}
	directory := filepath.Join(root, "internal", "adapters", "policy_lookup")
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("got %d generated files, want 3", len(entries))
	}
	manifest, err := os.ReadFile(filepath.Join(directory, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	lower := strings.ToLower(string(manifest))
	for _, forbidden := range []string{"command", "executable", "url"} {
		if strings.Contains(lower, forbidden) {
			t.Fatalf("manifest contains forbidden field %q: %s", forbidden, manifest)
		}
	}
	if _, err := adaptercatalog.DecodeManifest(manifest); err != nil {
		t.Fatalf("generated manifest is not catalog-valid: %v", err)
	}
	if info, err := os.Stat(directory); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("generated directory mode: %v %v", info, err)
	}
}

func TestScaffoldRejectsHostileTargetsAndCollisions(t *testing.T) {
	for _, name := range []string{"../escape", "a/b", "Upper", "trailing-", ""} {
		t.Run(strings.ReplaceAll(name, "/", "_"), func(t *testing.T) {
			if err := run([]string{"-root", t.TempDir(), "-name", name}); err == nil {
				t.Fatalf("expected %q rejection", name)
			}
		})
	}
	root := t.TempDir()
	parent := filepath.Join(root, "internal", "adapters")
	if err := os.MkdirAll(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(parent, "policy_lookup")); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-root", root, "-name", "policy_lookup"}); err == nil {
		t.Fatal("expected symlink target rejection")
	}
}

func TestScaffoldCollisionLeavesExistingFilesUnchanged(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "internal", "adapters", "policy_lookup")
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatal(err)
	}
	existing := filepath.Join(target, "keep.txt")
	if err := os.WriteFile(existing, []byte("owned"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-root", root, "-name", "policy_lookup"}); err == nil {
		t.Fatal("expected collision rejection")
	}
	got, err := os.ReadFile(existing)
	if err != nil || string(got) != "owned" {
		t.Fatalf("existing file changed: %q %v", got, err)
	}
}
