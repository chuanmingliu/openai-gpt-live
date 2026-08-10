package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/chuanmingliu/openai-gpt-live/internal/loadgen"
)

func TestPreflightOutputIsFixedAndContentSafe(t *testing.T) {
	writeProfile := func(name string, profile loadgen.HostProfile) string {
		t.Helper()
		raw, err := json.Marshal(profile)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(t.TempDir(), name)
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	actual := loadgen.DetectHostProfile()
	matching := writeProfile("matching.json", actual)
	var stdout, stderr bytes.Buffer
	if code := execute([]string{"-profile", matching}, &stdout, &stderr); code != 0 {
		t.Fatalf("matching preflight failed: %d %q", code, stderr.String())
	}
	if stdout.Len() != 0 || stderr.String() != "pinned runner preflight passed\n" {
		t.Fatalf("unexpected output stdout=%q stderr=%q", stdout.String(), stderr.String())
	}

	actual.CPUModel = "sensitive-host-model-marker"
	mismatch := writeProfile("mismatch.json", actual)
	stdout.Reset()
	stderr.Reset()
	if code := execute([]string{"-profile", mismatch}, &stdout, &stderr); code == 0 {
		t.Fatal("mismatched profile passed")
	}
	combined := stdout.String() + stderr.String()
	if combined != "pinned runner preflight failed\n" || strings.Contains(combined, actual.CPUModel) {
		t.Fatalf("preflight disclosed profile content: %q", combined)
	}
}
