package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalOperatorRunnerUsesOnlyFixedEntryPoints(t *testing.T) {
	root := t.TempDir()
	stateDir := filepath.Join(t.TempDir(), "operator-state")
	recorder := filepath.Join(t.TempDir(), "recorded")
	script := "#!/bin/sh\nprintf 'argv:' >> \"$RECORDER\"\nfor value in \"$@\"; do printf '<%s>' \"$value\" >> \"$RECORDER\"; done\nprintf '\\n' >> \"$RECORDER\"\nenv | sed 's/=.*//' | sort -u >> \"$RECORDER\"\nprintf '\\n--\\n' >> \"$RECORDER\"\ncase \"${1:-}\" in\n  *-id) service=${1%-id}; cat \"$RECORDER.$service.id\" 2>/dev/null || true ;;\n  *-start) service=${1%-start}; printf '%s-new' \"$service\" > \"$RECORDER.$service.id\" ;;\n  *-stop) service=${1%-stop}; rm -f \"$RECORDER.$service.id\" ;;\nesac\n"
	speech := writeRunnerScript(t, root, "tools/speech-launcher.sh", script)
	compose := writeRunnerScript(t, root, "deploy/scripts/durable-compose.sh", script)
	profile := filepath.Join(root, "config", "speech-profile.yaml")
	require.NoError(t, os.MkdirAll(filepath.Dir(profile), 0o700))
	require.NoError(t, os.WriteFile(profile, []byte("profile"), 0o600))
	t.Setenv("RECORDER", recorder)
	t.Setenv("OPERATOR_SECRET", "secret-value-must-not-appear")
	setRequiredOperatorEnvironment(t)

	cfg := operatorConfig{
		Enabled:        true,
		StateDir:       stateDir,
		RepositoryRoot: root,
		SpeechCommand:  speech,
		SpeechProfile:  profile,
		Readiness: map[operatorComponent]string{
			componentSpeech:  "http://127.0.0.1:8765/v1/pool",
			componentGateway: "http://127.0.0.1:19090/health/ready",
			componentWorker:  "http://127.0.0.1:19091/health/ready",
			componentClient:  "http://127.0.0.1:3000/health/ready",
		},
		ClientStop: func(context.Context) error { return nil },
	}
	probed := ""
	runner, err := newLocalOperatorRunner(cfg, nil, func(_ context.Context, endpoint string) bool {
		probed = endpoint
		return true
	})
	require.NoError(t, err)
	cfg.Readiness[componentGateway] = "http://127.0.0.1:19999/browser-derived-url"
	assert.True(t, runner.Ready(context.Background(), componentGateway))
	assert.Equal(t, "http://127.0.0.1:19090/health/ready", probed, "startup configuration is copied before any browser request can run")

	for _, operation := range []fixedOperation{
		{Component: componentSpeech, Action: actionStart},
		{Component: componentPostgres, Action: actionStart},
		{Component: componentPostgres, Action: actionMigrate},
		{Component: componentGateway, Action: actionStart},
		{Component: componentWorker, Action: actionStart},
		{Component: componentWorker, Action: actionStop},
		{Component: componentGateway, Action: actionStop},
		{Component: componentPostgres, Action: actionStop},
	} {
		require.NoError(t, runner.Run(context.Background(), operation), operation)
	}
	beforeClient, err := os.ReadFile(recorder)
	require.NoError(t, err)
	require.NoError(t, runner.Run(context.Background(), fixedOperation{Component: componentClient, Action: actionStart}))
	time.Sleep(10 * time.Millisecond)
	afterClient, err := os.ReadFile(recorder)
	require.NoError(t, err)

	recorded := string(beforeClient)
	assert.Equal(t, string(beforeClient), string(afterClient), "the already-running BFF is never a child command")
	canonicalProfile, err := filepath.EvalSymlinks(profile)
	require.NoError(t, err)
	assert.Contains(t, recorded, "<"+canonicalProfile+">")
	for _, verb := range []string{"postgres-start", "postgres-migrate", "gateway-start", "gateway-stop", "worker-start", "worker-stop", "postgres-stop"} {
		assert.Contains(t, recorded, "<"+verb+">", "every component maps to its own reviewed Compose operation")
	}
	assert.NotContains(t, recorded, "<up>")
	assert.NotContains(t, recorded, "<down>")
	assert.NotContains(t, recorded, "secret-value-must-not-appear")
	assert.Contains(t, recorded, "OPERATOR_SECRET", "the prepared environment is inherited without recording values")
	assert.True(t, runner.Owned(componentClient))

	_ = compose
}

func TestLocalOperatorRunnerRejectsUnsafeStartupConfiguration(t *testing.T) {
	root := t.TempDir()
	speech := writeRunnerScript(t, root, "tools/speech-launcher.sh", "#!/bin/sh\n")
	profile := filepath.Join(root, "config", "speech-profile.yaml")
	require.NoError(t, os.MkdirAll(filepath.Dir(profile), 0o700))
	require.NoError(t, os.WriteFile(profile, []byte("profile"), 0o600))

	base := operatorConfig{
		Enabled:        true,
		StateDir:       filepath.Join(t.TempDir(), "state"),
		RepositoryRoot: root,
		SpeechCommand:  speech,
		SpeechProfile:  profile,
		Readiness: map[operatorComponent]string{
			componentSpeech: "http://127.0.0.1:8765/v1/pool", componentGateway: "http://127.0.0.1:19090/health/ready", componentWorker: "http://127.0.0.1:19091/health/ready", componentClient: "http://127.0.0.1:3000/health/ready",
		},
	}
	for name, mutate := range map[string]func(*operatorConfig){
		"remote readiness":             func(cfg *operatorConfig) { cfg.Readiness[componentSpeech] = "https://example.com/secret-url" },
		"relative executable":          func(cfg *operatorConfig) { cfg.SpeechCommand = "tools/speech-launcher.sh" },
		"out of root profile":          func(cfg *operatorConfig) { cfg.SpeechProfile = filepath.Join(t.TempDir(), "outside-profile") },
		"missing server configuration": func(cfg *operatorConfig) { cfg.SpeechProfile = "" },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := base
			cfg.Readiness = map[operatorComponent]string{}
			for component, endpoint := range base.Readiness {
				cfg.Readiness[component] = endpoint
			}
			mutate(&cfg)
			_, err := newLocalOperatorRunner(cfg, nil, nil)
			require.Error(t, err)
			assert.NotContains(t, err.Error(), "secret-url")
			assert.NotContains(t, err.Error(), cfg.SpeechCommand)
		})
	}
}

func TestLocalOperatorRunnerKeepsSpeechAliveAfterActionCancellation(t *testing.T) {
	root := t.TempDir()
	speech := writeRunnerScript(t, root, "tools/speech-launcher.sh", "#!/bin/sh\ntrap 'exit 0' TERM\nwhile :; do sleep 1; done\n")
	writeRunnerScript(t, root, "deploy/scripts/durable-compose.sh", "#!/bin/sh\n")
	profile := filepath.Join(root, "config", "speech.yaml")
	require.NoError(t, os.MkdirAll(filepath.Dir(profile), 0o700))
	require.NoError(t, os.WriteFile(profile, []byte("ok"), 0o600))
	runner, err := newLocalOperatorRunner(operatorConfig{Enabled: true, StateDir: filepath.Join(t.TempDir(), "state"), RepositoryRoot: root, SpeechCommand: speech, SpeechProfile: profile, Readiness: fixedReadiness()}, nil, func(context.Context, string) bool { return true })
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, runner.Run(ctx, fixedOperation{Component: componentSpeech, Action: actionStart}))
	cancel()
	time.Sleep(20 * time.Millisecond)
	assert.True(t, runner.Owned(componentSpeech), "action cancellation must not terminate a successfully started child")
	require.NoError(t, runner.Run(context.Background(), fixedOperation{Component: componentSpeech, Action: actionStop}))
}

func TestLocalOperatorRunnerReadinessRejectsRedirects(t *testing.T) {
	var redirected atomic.Bool
	remote := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { redirected.Store(true) }))
	defer remote.Close()
	local := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		http.Redirect(response, request, remote.URL, http.StatusFound)
	}))
	defer local.Close()
	assert.False(t, probeLocalReadiness(context.Background(), local.URL))
	assert.False(t, redirected.Load(), "fixed readiness probes never follow a redirect")
}

func writeRunnerScript(t *testing.T, root, relative, body string) string {
	t.Helper()
	path := filepath.Join(root, relative)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	require.NoError(t, os.WriteFile(path, []byte(body), 0o700))
	return path
}

func setRequiredOperatorEnvironment(t *testing.T) {
	t.Helper()
	for _, names := range operatorRequiredEnvironment {
		for _, name := range names {
			t.Setenv(name, "test-value")
		}
	}
}

func fixedReadiness() map[operatorComponent]string {
	return map[operatorComponent]string{componentSpeech: "http://127.0.0.1:8765/v1/pool", componentGateway: "http://127.0.0.1:19090/health/ready", componentWorker: "http://127.0.0.1:19091/health/ready", componentClient: "http://127.0.0.1:3000/health/ready"}
}
