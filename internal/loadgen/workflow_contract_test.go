package loadgen

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type pinnedWorkflow struct {
	Jobs map[string]struct {
		RunsOn      any    `yaml:"runs-on"`
		Needs       any    `yaml:"needs"`
		Environment string `yaml:"environment"`
		Steps       []struct {
			Name string         `yaml:"name"`
			Uses string         `yaml:"uses"`
			With map[string]any `yaml:"with"`
			Run  string         `yaml:"run"`
		} `yaml:"steps"`
	} `yaml:"jobs"`
}

func TestReleaseWorkflowRequiresProtectedCandidateCanaryEdge(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "release-gate.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var workflow pinnedWorkflow
	if err := yaml.Unmarshal(raw, &workflow); err != nil {
		t.Fatal(err)
	}
	edge, ok := workflow.Jobs["deploy-observe-candidate-canary"]
	if !ok {
		t.Fatal("release workflow has no explicit candidate canary deployment edge")
	}
	if edge.Environment != "release-candidate-canary" {
		t.Fatalf("candidate canary environment=%q", edge.Environment)
	}
	if labels, ok := edge.RunsOn.([]any); !ok || !reflect.DeepEqual(labels, []any{"self-hosted", "macOS", "ARM64", "gpt-live-candidate-canary"}) {
		t.Fatalf("candidate canary authority runner labels=%v", edge.RunsOn)
	}
	collect := workflow.Jobs["collect-unsigned-evidence"]
	needs, ok := collect.Needs.([]any)
	if !ok || !reflect.DeepEqual(needs, []any{"candidate-manifest", "deploy-observe-candidate-canary"}) {
		t.Fatalf("unsigned collection needs=%v", collect.Needs)
	}
	var edgeRun string
	for _, step := range edge.Steps {
		if step.Name == "Deploy, observe for thirty minutes, and roll back candidate" {
			edgeRun = step.Run
		}
	}
	if !strings.Contains(edgeRun, "run-candidate-canary-edge.sh") || !strings.Contains(edgeRun, "--minimum-observation-seconds 1800") {
		t.Fatal("protected edge does not invoke the fixed >=30-minute platform contract")
	}
	wrapper, err := os.ReadFile(filepath.Join("..", "..", "deploy", "scripts", "run-candidate-canary-edge.sh"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(wrapper)
	if !strings.Contains(text, "authority=/usr/local/libexec/gpt-live/candidate-canary-edge") || strings.Contains(text, "${CANDIDATE_CANARY_AUTHORITY") {
		t.Fatal("candidate canary wrapper does not bind the fixed platform authority")
	}
	for _, required := range []string{
		"stat -f '%u'", "(( (8#$mode & 0022) == 0 ))", `"$authority" run`,
		"--halt-policy-sha256", "--minimum-observation-seconds",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("candidate canary wrapper lacks fail-closed contract %q", required)
		}
	}
	receiptContract, err := os.ReadFile(filepath.Join("..", "..", "internal", "releasegate", "candidate_canary.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(receiptContract), `receipt.CleanupStatus != "rolled_back"`) {
		t.Fatal("candidate canary receipt does not require completed rollback")
	}
}

func TestFormalWorkflowsExactlyMatchCheckedInPinnedRunner(t *testing.T) {
	profilePath := filepath.Join("..", "..", "test", "load", "pinned-profile.json")
	profile, err := LoadPinnedProfile(profilePath)
	if err != nil {
		t.Fatal(err)
	}
	if profile.OS != "darwin" || profile.Arch != "arm64" || profile.CPUModel != "Apple M3 Max" || profile.GoVersion != "go1.26.2" {
		t.Fatalf("checked-in formal profile changed without workflow contract review: %#v", profile)
	}
	wantLabels := []string{"self-hosted", "macOS", "ARM64", "gpt-live-apple-m3-max"}
	for _, test := range []struct{ file, job string }{
		{filepath.Join("..", "..", ".github", "workflows", "nightly.yml"), "protocol-chaos-load-soak"},
		{filepath.Join("..", "..", ".github", "workflows", "release-gate.yml"), "collect-unsigned-evidence"},
	} {
		raw, err := os.ReadFile(test.file)
		if err != nil {
			t.Fatal(err)
		}
		var workflow pinnedWorkflow
		if err := yaml.Unmarshal(raw, &workflow); err != nil {
			t.Fatal(err)
		}
		job, ok := workflow.Jobs[test.job]
		if !ok {
			t.Fatalf("%s has no %s job", test.file, test.job)
		}
		labels, ok := job.RunsOn.([]any)
		gotLabels := make([]string, 0, len(labels))
		for _, label := range labels {
			value, stringOK := label.(string)
			if !stringOK {
				ok = false
				break
			}
			gotLabels = append(gotLabels, value)
		}
		if !ok || !reflect.DeepEqual(gotLabels, wantLabels) {
			t.Fatalf("%s runner labels=%v want=%v", test.file, job.RunsOn, wantLabels)
		}
		var goVersion, preflight string
		for _, step := range job.Steps {
			if step.Uses == "actions/setup-go@0a12ed9d6a96ab950c8f026ed9f722fe0da7ef32" {
				goVersion, _ = step.With["go-version"].(string)
			}
			if step.Name == "Verify exact content-safe pinned runner" {
				preflight = step.Run
			}
		}
		if goVersion != "1.26.2" {
			t.Fatalf("%s setup-go version=%q", test.file, goVersion)
		}
		if preflight != "go run ./cmd/live-runner-preflight -profile test/load/pinned-profile.json\n" {
			t.Fatalf("%s does not use the reviewed content-safe preflight", test.file)
		}
	}
}

func TestCIUsesFullHistoryEventAwareWhitespaceGate(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var workflow pinnedWorkflow
	if err := yaml.Unmarshal(raw, &workflow); err != nil {
		t.Fatal(err)
	}
	job := workflow.Jobs["verify"]
	var fetchDepth int
	var diffRun string
	for _, step := range job.Steps {
		if step.Uses == "actions/checkout@11bd71901bbe5b1630ceea73d27597364c9af683" {
			fetchDepth, _ = step.With["fetch-depth"].(int)
		}
		if step.Name == "Event-aware whitespace gate" {
			diffRun = step.Run
		}
	}
	if fetchDepth != 0 {
		t.Fatalf("CI checkout fetch-depth=%d, want full history", fetchDepth)
	}
	if diffRun != "./deploy/scripts/check-event-diff.sh \"$EVENT_NAME\" \"$PR_BASE_SHA\" \"$PUSH_BEFORE_SHA\" \"$EVENT_HEAD_SHA\"\n" {
		t.Fatal("CI does not invoke the reviewed event-aware diff gate")
	}
}
