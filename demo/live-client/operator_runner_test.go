package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
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
	setRequiredOperatorEnvironment(t)
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

func TestLocalOperatorRunnerSupervisorCleansOnlyNewComposeAfterIdentityFailures(t *testing.T) {
	tests := []struct {
		name           string
		startFails     bool
		before         string
		lookup         func(call int, shortDeadline bool) (string, bool, error)
		wantStops      int
		wantOwnedAfter bool
		wantReady      bool
	}{
		{
			name: "successful start retries an initial post-start query failure",
			lookup: func(call int, _ bool) (string, bool, error) {
				switch call {
				case 1:
					return "", false, nil
				case 2:
					return "", false, errors.New("sentinel post-query failure")
				default:
					return "postgres-new", true, nil
				}
			},
			wantOwnedAfter: true,
			wantReady:      true,
		},
		{
			name:       "failed start claims a late identity for supervisor cleanup",
			startFails: true,
			lookup: func(call int, _ bool) (string, bool, error) {
				switch call {
				case 1:
					return "", false, nil
				case 2:
					return "", false, errors.New("sentinel initial late-id failure")
				default:
					return "postgres-late", true, nil
				}
			},
			wantStops: 1,
		},
		{
			name:   "unknown post-state never grants ownership of a pre-existing service",
			before: "postgres-existing",
			lookup: func(call int, shortDeadline bool) (string, bool, error) {
				if call == 1 {
					return "postgres-existing", true, nil
				}
				if shortDeadline {
					return "", false, errors.New("sentinel post-query failure")
				}
				return "postgres-existing", true, nil
			},
		},
		{
			name: "cleanup deadline retains pending ownership without stopping an unknown identity",
			lookup: func(call int, _ bool) (string, bool, error) {
				if call == 1 {
					return "", false, nil
				}
				return "", false, errors.New("sentinel identity unavailable")
			},
			wantOwnedAfter: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setRequiredOperatorEnvironment(t)
			runner := newTestLocalOperatorRunner(t, func(ctx context.Context, _ string, args ...string) *exec.Cmd {
				if len(args) == 1 && args[0] == "postgres-start" && test.startFails {
					return exec.CommandContext(ctx, "/bin/sh", "-c", "printf sentinel-start-error >&2; exit 23")
				}
				return exec.CommandContext(ctx, "/usr/bin/true")
			})
			runner.identityTimeout = 8 * time.Millisecond
			runner.identityRetryWait = time.Millisecond
			var lookupCalls atomic.Int32
			runner.composeIdentity = func(ctx context.Context, _ operatorComponent) (string, bool, error) {
				deadline, _ := ctx.Deadline()
				return test.lookup(int(lookupCalls.Add(1)), time.Until(deadline) < 20*time.Millisecond)
			}
			var stopCalls atomic.Int32
			runner.newCommand = func(ctx context.Context, _ string, args ...string) *exec.Cmd {
				if len(args) == 1 && args[0] == "postgres-stop" {
					stopCalls.Add(1)
				}
				if len(args) == 1 && args[0] == "postgres-start" && test.startFails {
					return exec.CommandContext(ctx, "/bin/sh", "-c", "printf sentinel-start-error >&2; exit 23")
				}
				return exec.CommandContext(ctx, "/usr/bin/true")
			}

			supervisor := newOperatorSupervisor(runner, 100*time.Millisecond)
			status := supervisor.Apply(t.Context(), componentPostgres, actionStart)
			assert.Equal(t, test.wantStops, int(stopCalls.Load()))
			assert.Equal(t, test.wantOwnedAfter, runner.Owned(componentPostgres))
			if !test.wantReady {
				assert.Equal(t, "failed", status.State)
				assert.Equal(t, "process_exited", status.Code)
			} else {
				assert.Equal(t, "ready", status.State)
			}
			assert.NotContains(t, fmt.Sprint(status), "sentinel")
		})
	}
}

func TestLocalOperatorRunnerSupervisorCleansFailedStartAfterAbsentThenLateIdentity(t *testing.T) {
	setRequiredOperatorEnvironment(t)
	var identityCalls atomic.Int32
	var stopCalls atomic.Int32
	runner := newTestLocalOperatorRunner(t, func(ctx context.Context, _ string, args ...string) *exec.Cmd {
		if len(args) == 1 && args[0] == "postgres-start" {
			return exec.CommandContext(ctx, "/usr/bin/false")
		}
		if len(args) == 1 && args[0] == "postgres-stop" {
			stopCalls.Add(1)
		}
		return exec.CommandContext(ctx, "/usr/bin/true")
	})
	runner.identityTimeout = 15 * time.Millisecond
	runner.identityRetryWait = time.Millisecond
	runner.composeIdentity = func(context.Context, operatorComponent) (string, bool, error) {
		switch identityCalls.Add(1) {
		case 1, 2:
			return "", false, nil
		default:
			return "postgres-late-after-absence", true, nil
		}
	}

	status := newOperatorSupervisor(runner, 100*time.Millisecond).Apply(t.Context(), componentPostgres, actionStart)
	assert.Equal(t, "failed", status.State)
	assert.Equal(t, "process_exited", status.Code)
	assert.EqualValues(t, 1, stopCalls.Load())
	assert.False(t, runner.Owned(componentPostgres))
}

func TestLocalOperatorRunnerSupervisorPendingCleanupWaitsPastOneAbsenceForLateIdentity(t *testing.T) {
	setRequiredOperatorEnvironment(t)
	var identityMu sync.Mutex
	var initialPostContext context.Context
	var totalCalls int
	var cleanupCalls int
	var stopCalls atomic.Int32
	runner := newTestLocalOperatorRunner(t, func(ctx context.Context, _ string, args ...string) *exec.Cmd {
		if len(args) == 1 && args[0] == "postgres-start" {
			return exec.CommandContext(ctx, "/usr/bin/false")
		}
		if len(args) == 1 && args[0] == "postgres-stop" {
			stopCalls.Add(1)
		}
		return exec.CommandContext(ctx, "/usr/bin/true")
	})
	runner.identityTimeout = 8 * time.Millisecond
	runner.identityRetryWait = time.Millisecond
	runner.composeIdentity = func(ctx context.Context, _ operatorComponent) (string, bool, error) {
		identityMu.Lock()
		defer identityMu.Unlock()
		totalCalls++
		if totalCalls == 1 {
			return "", false, nil
		}
		if initialPostContext == nil {
			initialPostContext = ctx
		}
		if ctx == initialPostContext {
			return "", false, errors.New("sentinel initial discovery unavailable")
		}
		cleanupCalls++
		if cleanupCalls == 1 {
			return "", false, nil
		}
		return "postgres-late-after-pending-absence", true, nil
	}

	status := newOperatorSupervisor(runner, 100*time.Millisecond).Apply(t.Context(), componentPostgres, actionStart)
	assert.Equal(t, "failed", status.State)
	assert.Equal(t, "process_exited", status.Code)
	assert.GreaterOrEqual(t, cleanupCalls, 2)
	assert.EqualValues(t, 1, stopCalls.Load())
	assert.False(t, runner.Owned(componentPostgres))
	assert.NotContains(t, fmt.Sprint(status), "sentinel")
}

func TestLocalOperatorRunnerRejectsKnownPreExistingBeforeMutationOrPostDiscovery(t *testing.T) {
	setRequiredOperatorEnvironment(t)
	var identityCalls atomic.Int32
	var startCalls atomic.Int32
	var stopCalls atomic.Int32
	runner := newTestLocalOperatorRunner(t, func(ctx context.Context, _ string, args ...string) *exec.Cmd {
		if len(args) == 1 && args[0] == "postgres-start" {
			startCalls.Add(1)
			return exec.CommandContext(ctx, "/usr/bin/false")
		}
		if len(args) == 1 && args[0] == "postgres-stop" {
			stopCalls.Add(1)
		}
		return exec.CommandContext(ctx, "/usr/bin/true")
	})
	runner.identityTimeout = 15 * time.Millisecond
	runner.identityRetryWait = time.Millisecond
	runner.composeIdentity = func(context.Context, operatorComponent) (string, bool, error) {
		identityCalls.Add(1)
		return "postgres-pre-existing", true, nil
	}

	status := newOperatorSupervisor(runner, 100*time.Millisecond).Apply(t.Context(), componentPostgres, actionStart)
	assert.Equal(t, "failed", status.State)
	assert.Equal(t, "process_exited", status.Code)
	assert.EqualValues(t, 1, identityCalls.Load())
	assert.Zero(t, startCalls.Load())
	assert.Zero(t, stopCalls.Load())
	assert.False(t, runner.Owned(componentPostgres))
}

func TestLocalOperatorRunnerSupervisorPreventsSuccessfulUnownedOldIDToNewIDMutation(t *testing.T) {
	setRequiredOperatorEnvironment(t)
	var identityMu sync.Mutex
	identity := "postgres-unowned-old"
	var startCalls atomic.Int32
	runner := newTestLocalOperatorRunner(t, func(ctx context.Context, _ string, args ...string) *exec.Cmd {
		if len(args) == 1 && args[0] == "postgres-start" {
			startCalls.Add(1)
			identityMu.Lock()
			identity = "postgres-new-from-forbidden-mutation"
			identityMu.Unlock()
		}
		return exec.CommandContext(ctx, "/usr/bin/true")
	})
	runner.composeIdentity = func(context.Context, operatorComponent) (string, bool, error) {
		identityMu.Lock()
		defer identityMu.Unlock()
		return identity, true, nil
	}

	status := newOperatorSupervisor(runner, 100*time.Millisecond).Apply(t.Context(), componentPostgres, actionStart)
	assert.Equal(t, "failed", status.State)
	assert.Equal(t, "process_exited", status.Code)
	assert.Zero(t, startCalls.Load(), "an unowned existing service must be rejected before the fixed Start command")
	identityMu.Lock()
	assert.Equal(t, "postgres-unowned-old", identity)
	identityMu.Unlock()
	assert.False(t, runner.Owned(componentPostgres))
}

func TestLocalOperatorRunnerAllowsSafeFixedStartForOwnedExistingComposeService(t *testing.T) {
	setRequiredOperatorEnvironment(t)
	runner := newTestLocalOperatorRunner(t, nil)
	const ownedID = "postgres-owned-existing"
	require.NoError(t, runner.writeOwner(componentPostgres, ownedID))
	runner.mu.Lock()
	runner.composeOwned[componentPostgres] = ownedID
	runner.mu.Unlock()
	runner.composeIdentity = func(context.Context, operatorComponent) (string, bool, error) {
		return ownedID, true, nil
	}
	var startCalls atomic.Int32
	runner.newCommand = func(ctx context.Context, _ string, args ...string) *exec.Cmd {
		if len(args) == 1 && args[0] == "postgres-start" {
			startCalls.Add(1)
		}
		return exec.CommandContext(ctx, "/usr/bin/true")
	}

	status := newOperatorSupervisor(runner, 100*time.Millisecond).Apply(t.Context(), componentPostgres, actionStart)
	assert.Equal(t, "ready", status.State)
	assert.EqualValues(t, 1, startCalls.Load())
	assert.True(t, runner.Owned(componentPostgres))
}

func TestLocalOperatorRunnerSupervisorTransfersOwnedRecreationAndStopsOnlyNewIdentity(t *testing.T) {
	setRequiredOperatorEnvironment(t)
	const oldID = "worker-owned-old"
	const newID = "worker-owned-new"
	var identityMu sync.Mutex
	identity := oldID
	var stopCalls atomic.Int32
	var stoppedID string
	runner := newTestLocalOperatorRunner(t, func(ctx context.Context, _ string, args ...string) *exec.Cmd {
		if len(args) == 1 && args[0] == "worker-start" {
			identityMu.Lock()
			identity = newID
			identityMu.Unlock()
		}
		if len(args) == 1 && args[0] == "worker-stop" {
			stopCalls.Add(1)
			identityMu.Lock()
			stoppedID = identity
			identity = ""
			identityMu.Unlock()
		}
		return exec.CommandContext(ctx, "/usr/bin/true")
	})
	require.NoError(t, runner.writeOwner(componentWorker, oldID))
	runner.mu.Lock()
	runner.composeOwned[componentWorker] = oldID
	runner.mu.Unlock()
	runner.composeIdentity = func(context.Context, operatorComponent) (string, bool, error) {
		identityMu.Lock()
		defer identityMu.Unlock()
		return identity, identity != "", nil
	}
	runner.probe = func(context.Context, string) bool {
		identityMu.Lock()
		defer identityMu.Unlock()
		return identity != ""
	}
	supervisor := newOperatorSupervisor(runner, 100*time.Millisecond)
	supervisor.migrated = true

	startStatus := supervisor.Apply(t.Context(), componentWorker, actionStart)
	assert.Equal(t, "ready", startStatus.State, startStatus)
	assert.True(t, runner.ownerMatches(componentWorker, newID))
	assert.False(t, runner.ownerMatches(componentWorker, oldID))

	identityMu.Lock()
	identity = oldID
	identityMu.Unlock()
	staleStop := supervisor.Apply(t.Context(), componentWorker, actionStop)
	assert.Equal(t, "failed", staleStop.State, staleStop)
	assert.Equal(t, "process_exited", staleStop.Code, staleStop)
	assert.Zero(t, stopCalls.Load(), "stale ownership must never invoke the fixed Stop command")

	identityMu.Lock()
	identity = newID
	identityMu.Unlock()
	newStop := supervisor.Apply(t.Context(), componentWorker, actionStop)
	assert.Equal(t, "stopped", newStop.State, newStop)
	assert.EqualValues(t, 1, stopCalls.Load())
	assert.Equal(t, newID, stoppedID)
	assert.False(t, runner.Owned(componentWorker))
	assert.NoFileExists(t, runner.ownerPath(componentWorker))
}

func TestLocalOperatorRunnerSupervisorTransfersOwnedRecreationAfterFailureOrTimeout(t *testing.T) {
	for _, test := range []struct {
		name       string
		command    string
		arguments  []string
		timeout    time.Duration
		statusCode string
	}{
		{name: "failed start", command: "/usr/bin/false", timeout: 100 * time.Millisecond, statusCode: "process_exited"},
		{name: "timed out start", command: "/bin/sleep", arguments: []string{"5"}, timeout: 15 * time.Millisecond, statusCode: "startup_timeout"},
	} {
		t.Run(test.name, func(t *testing.T) {
			setRequiredOperatorEnvironment(t)
			const oldID = "worker-owned-old"
			const newID = "worker-owned-new-after-partial-start"
			var identityMu sync.Mutex
			identity := oldID
			var stopCalls atomic.Int32
			runner := newTestLocalOperatorRunner(t, func(ctx context.Context, _ string, args ...string) *exec.Cmd {
				if len(args) == 1 && args[0] == "worker-start" {
					identityMu.Lock()
					identity = newID
					identityMu.Unlock()
					return exec.CommandContext(ctx, test.command, test.arguments...)
				}
				if len(args) == 1 && args[0] == "worker-stop" {
					stopCalls.Add(1)
					identityMu.Lock()
					identity = ""
					identityMu.Unlock()
				}
				return exec.CommandContext(ctx, "/usr/bin/true")
			})
			require.NoError(t, runner.writeOwner(componentWorker, oldID))
			runner.mu.Lock()
			runner.composeOwned[componentWorker] = oldID
			runner.mu.Unlock()
			runner.composeIdentity = func(context.Context, operatorComponent) (string, bool, error) {
				identityMu.Lock()
				defer identityMu.Unlock()
				return identity, identity != "", nil
			}
			runner.probe = func(context.Context, string) bool {
				identityMu.Lock()
				defer identityMu.Unlock()
				return identity != ""
			}
			supervisor := newOperatorSupervisor(runner, test.timeout)
			supervisor.migrated = true

			status := supervisor.Apply(t.Context(), componentWorker, actionStart)
			assert.Equal(t, "failed", status.State, status)
			assert.Equal(t, test.statusCode, status.Code, status)
			assert.Zero(t, stopCalls.Load(), "an already-owned partial recreation remains owned for an explicit Stop")
			assert.True(t, runner.ownerMatches(componentWorker, newID))
			assert.False(t, runner.ownerMatches(componentWorker, oldID))

			stopStatus := supervisor.Apply(t.Context(), componentWorker, actionStop)
			assert.Equal(t, "stopped", stopStatus.State, stopStatus)
			assert.EqualValues(t, 1, stopCalls.Load())
			assert.False(t, runner.Owned(componentWorker))
			assert.NoFileExists(t, runner.ownerPath(componentWorker))
		})
	}
}

func TestLocalOperatorRunnerSupervisorFencesUnresolvedOwnedRecreation(t *testing.T) {
	setRequiredOperatorEnvironment(t)
	const oldID = "worker-owned-old"
	const newID = "worker-owned-new-after-unresolved-start"
	var identityCalls atomic.Int32
	var startCalls atomic.Int32
	var allowResolution atomic.Bool
	var stopCalls atomic.Int32
	runner := newTestLocalOperatorRunner(t, func(ctx context.Context, _ string, args ...string) *exec.Cmd {
		if len(args) == 1 && args[0] == "worker-start" {
			startCalls.Add(1)
		}
		if len(args) == 1 && args[0] == "worker-stop" {
			stopCalls.Add(1)
		}
		return exec.CommandContext(ctx, "/usr/bin/true")
	})
	require.NoError(t, runner.writeOwner(componentWorker, oldID))
	runner.mu.Lock()
	runner.composeOwned[componentWorker] = oldID
	runner.mu.Unlock()
	runner.composeIdentity = func(context.Context, operatorComponent) (string, bool, error) {
		if identityCalls.Add(1) == 1 {
			return oldID, true, nil
		}
		if allowResolution.Load() {
			return newID, true, nil
		}
		return "", false, errors.New("sentinel unresolved owned recreation")
	}
	runner.probe = func(context.Context, string) bool { return true }
	supervisor := newOperatorSupervisor(runner, 100*time.Millisecond)
	supervisor.migrated = true

	status := supervisor.Apply(t.Context(), componentWorker, actionStart)
	assert.Equal(t, "failed", status.State, status)
	assert.Equal(t, "process_exited", status.Code, status)
	runner.mu.Lock()
	pending := runner.composePending[componentWorker]
	runner.mu.Unlock()
	assert.True(t, pending)
	assert.EqualValues(t, 1, startCalls.Load())

	retryStatus := supervisor.Apply(t.Context(), componentWorker, actionStart)
	assert.Equal(t, "failed", retryStatus.State, retryStatus)
	assert.Equal(t, "process_exited", retryStatus.Code, retryStatus)
	assert.EqualValues(t, 1, startCalls.Load(), "pending ownership must fence a second Start before mutation")

	allowResolution.Store(true)
	stopStatus := supervisor.Apply(t.Context(), componentWorker, actionStop)
	assert.EqualValues(t, 1, stopCalls.Load())
	assert.False(t, runner.Owned(componentWorker))
	assert.NoFileExists(t, runner.ownerPath(componentWorker))
	assert.NotContains(t, fmt.Sprint(status, retryStatus, stopStatus), "sentinel")
}

func TestLocalOperatorRunnerPreIdentityFailureNeverStartsService(t *testing.T) {
	setRequiredOperatorEnvironment(t)
	var commandCalls atomic.Int32
	runner := newTestLocalOperatorRunner(t, func(ctx context.Context, _ string, _ ...string) *exec.Cmd {
		commandCalls.Add(1)
		return exec.CommandContext(ctx, "/usr/bin/true")
	})
	runner.composeIdentity = func(context.Context, operatorComponent) (string, bool, error) {
		return "", false, errors.New("sentinel pre-query failure")
	}

	err := runner.Run(t.Context(), fixedOperation{Component: componentPostgres, Action: actionStart})
	assert.ErrorIs(t, err, errOperatorProcessExited)
	assert.Zero(t, commandCalls.Load())
	assert.False(t, runner.Owned(componentPostgres))
	assert.NotContains(t, err.Error(), "sentinel")
}

func TestLocalOperatorRunnerAtomicRecordsFailClosedAtEveryInstallPhase(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*localOperatorRunner)
	}{
		{
			name: "write failure",
			mutate: func(runner *localOperatorRunner) {
				runner.records.write = func(*os.File, []byte) error { return errors.New("sentinel write") }
			},
		},
		{
			name: "file fsync failure",
			mutate: func(runner *localOperatorRunner) {
				runner.records.syncFile = func(*os.File) error { return errors.New("sentinel file sync") }
			},
		},
		{
			name: "atomic install failure",
			mutate: func(runner *localOperatorRunner) {
				runner.records.install = func(*os.Root, string, string) error { return errors.New("sentinel install") }
			},
		},
		{
			name: "ambiguous atomic install failure removes only its own link",
			mutate: func(runner *localOperatorRunner) {
				runner.records.install = func(root *os.Root, temporary, final string) error {
					require.NoError(t, root.Link(temporary, final))
					return errors.New("sentinel ambiguous install")
				}
			},
		},
		{
			name: "directory fsync failure rolls back installed record",
			mutate: func(runner *localOperatorRunner) {
				runner.records.syncDirectory = func(*os.Root) error { return errors.New("sentinel directory sync") }
			},
		},
		{
			name: "temporary-removal fsync failure rolls back installed record",
			mutate: func(runner *localOperatorRunner) {
				var calls atomic.Int32
				runner.records.syncDirectory = func(*os.Root) error {
					if calls.Add(1) == 2 {
						return errors.New("sentinel removal sync")
					}
					return nil
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := newTestLocalOperatorRunner(t, nil)
			test.mutate(runner)
			err := runner.writeOwner(componentPostgres, "container-secret-sentinel")
			require.Error(t, err)
			assert.NoFileExists(t, runner.ownerPath(componentPostgres))
			assertNoTemporaryRecords(t, runner.cfg.StateDir)
			assert.NotContains(t, errOperatorProcessExited.Error(), "container-secret-sentinel")
		})
	}
}

func TestLocalOperatorRunnerAtomicRecordsPreserveExistingAndRetargetedFiles(t *testing.T) {
	t.Run("exclusive install preserves an existing record", func(t *testing.T) {
		runner := newTestLocalOperatorRunner(t, nil)
		path := runner.ownerPath(componentPostgres)
		require.NoError(t, os.WriteFile(path, []byte("stale-owner\n"), 0o600))
		require.Error(t, runner.writeOwner(componentPostgres, "new-owner"))
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, "stale-owner\n", string(data))
		assertNoTemporaryRecords(t, runner.cfg.StateDir)
	})

	t.Run("exclusive install never follows a final symlink", func(t *testing.T) {
		runner := newTestLocalOperatorRunner(t, nil)
		target := filepath.Join(t.TempDir(), "target")
		require.NoError(t, os.WriteFile(target, []byte("outside\n"), 0o600))
		require.NoError(t, os.Symlink(target, runner.ownerPath(componentPostgres)))
		require.Error(t, runner.writeOwner(componentPostgres, "new-owner"))
		data, err := os.ReadFile(target)
		require.NoError(t, err)
		assert.Equal(t, "outside\n", string(data))
		assertNoTemporaryRecords(t, runner.cfg.StateDir)
	})

	t.Run("changed identity after install is rejected without deleting the replacement", func(t *testing.T) {
		runner := newTestLocalOperatorRunner(t, nil)
		runner.records.install = func(root *os.Root, temporary, final string) error {
			require.NoError(t, root.Link(temporary, final))
			require.NoError(t, root.Remove(final))
			return root.WriteFile(final, []byte("hostile-replacement\n"), 0o600)
		}
		require.Error(t, runner.writeOwner(componentPostgres, "new-owner"))
		data, err := os.ReadFile(runner.ownerPath(componentPostgres))
		require.NoError(t, err)
		assert.Equal(t, "hostile-replacement\n", string(data))
		assertNoTemporaryRecords(t, runner.cfg.StateDir)
	})
}

func TestLocalOperatorRunnerRejectsHostileStateAndOwnershipRecords(t *testing.T) {
	t.Run("state directory swap cannot redirect a record", func(t *testing.T) {
		runner := newTestLocalOperatorRunner(t, nil)
		original := runner.cfg.StateDir + ".original"
		target := t.TempDir()
		require.NoError(t, os.Rename(runner.cfg.StateDir, original))
		require.NoError(t, os.Symlink(target, runner.cfg.StateDir))
		require.Error(t, runner.writeOwner(componentPostgres, "new-owner"))
		assert.NoFileExists(t, filepath.Join(target, "postgres.owner"))
		assert.NoFileExists(t, filepath.Join(original, "postgres.owner"))
	})

	for _, test := range []struct {
		name    string
		prepare func(t *testing.T, path string)
	}{
		{name: "partial owner", prepare: func(t *testing.T, path string) { require.NoError(t, os.WriteFile(path, []byte("expected"), 0o600)) }},
		{name: "public owner mode", prepare: func(t *testing.T, path string) { require.NoError(t, os.WriteFile(path, []byte("expected\n"), 0o644)) }},
		{name: "owner symlink", prepare: func(t *testing.T, path string) {
			target := filepath.Join(t.TempDir(), "owner")
			require.NoError(t, os.WriteFile(target, []byte("expected\n"), 0o600))
			require.NoError(t, os.Symlink(target, path))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := newTestLocalOperatorRunner(t, nil)
			test.prepare(t, runner.ownerPath(componentPostgres))
			assert.False(t, runner.ownerMatches(componentPostgres, "expected"))
		})
	}
}

func TestLocalOperatorRunnerRejectsUnsafeStateDirectoryTable(t *testing.T) {
	root, speech, profile := newRunnerRepository(t)
	for _, test := range []struct {
		name    string
		prepare func(t *testing.T) string
	}{
		{name: "regular file", prepare: func(t *testing.T) string {
			path := filepath.Join(t.TempDir(), "state")
			require.NoError(t, os.WriteFile(path, nil, 0o600))
			return path
		}},
		{name: "public mode", prepare: func(t *testing.T) string {
			path := filepath.Join(t.TempDir(), "state")
			require.NoError(t, os.Mkdir(path, 0o755))
			return path
		}},
		{name: "symlink", prepare: func(t *testing.T) string {
			parent := t.TempDir()
			path := filepath.Join(parent, "state")
			require.NoError(t, os.Symlink(t.TempDir(), path))
			return path
		}},
		{name: "writable parent", prepare: func(t *testing.T) string {
			parent := filepath.Join(t.TempDir(), "public")
			require.NoError(t, os.Mkdir(parent, 0o777))
			require.NoError(t, os.Chmod(parent, 0o777))
			return filepath.Join(parent, "state")
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := newLocalOperatorRunner(operatorConfig{Enabled: true, StateDir: test.prepare(t), RepositoryRoot: root, SpeechCommand: speech, SpeechProfile: profile, Readiness: fixedReadiness()}, nil, nil)
			require.Error(t, err)
			assert.ErrorIs(t, err, errOperatorMissingConfiguration)
		})
	}
}

func TestLocalOperatorRunnerClassifiesEveryMissingStartupRequirementWithoutSecrets(t *testing.T) {
	tests := []struct {
		component operatorComponent
		action    operatorAction
		missing   string
	}{
		{componentSpeech, actionStart, "S2S_API_KEY"},
		{componentPostgres, actionStart, "POSTGRES_PASSWORD"},
		{componentPostgres, actionMigrate, "POSTGRES_PASSWORD"},
		{componentGateway, actionStart, "S2S_API_KEY"},
		{componentWorker, actionStart, "DEEPSEEK_API_KEY"},
	}

	for _, test := range tests {
		t.Run(string(test.component)+"-"+string(test.action)+"-"+test.missing, func(t *testing.T) {
			setRequiredOperatorEnvironment(t)
			t.Setenv(test.missing, "")
			var commandCalls atomic.Int32
			runner := newTestLocalOperatorRunner(t, func(ctx context.Context, _ string, _ ...string) *exec.Cmd {
				commandCalls.Add(1)
				return exec.CommandContext(ctx, "/bin/sh", "-c", "printf provider-json-secret-sentinel >&2; exit 91")
			})
			err := runner.Run(t.Context(), fixedOperation{Component: test.component, Action: test.action})
			status := componentStatus{Component: test.component, State: "failed", Code: operationCode(err)}
			assert.Equal(t, "failed", status.State)
			assert.Equal(t, "missing_configuration", status.Code)
			assert.Zero(t, commandCalls.Load())
			assert.NotContains(t, fmt.Sprint(status), "sentinel")
			entries, err := os.ReadDir(runner.cfg.StateDir)
			require.NoError(t, err)
			assert.Empty(t, entries)
		})
	}
}

func TestLocalOperatorRunnerStopPathsNeedNoStartupCredentials(t *testing.T) {
	setRequiredOperatorEnvironment(t)
	runner := newTestLocalOperatorRunner(t, nil)
	var lookupMu sync.Mutex
	identities := map[operatorComponent]string{}
	runner.composeIdentity = func(_ context.Context, component operatorComponent) (string, bool, error) {
		lookupMu.Lock()
		defer lookupMu.Unlock()
		id := identities[component]
		return id, id != "", nil
	}
	runner.newCommand = func(ctx context.Context, _ string, args ...string) *exec.Cmd {
		if len(args) == 1 && strings.HasSuffix(args[0], "-start") {
			component := operatorComponent(strings.TrimSuffix(args[0], "-start"))
			lookupMu.Lock()
			identities[component] = string(component) + "-new"
			lookupMu.Unlock()
		}
		if len(args) == 1 && strings.HasSuffix(args[0], "-stop") {
			component := operatorComponent(strings.TrimSuffix(args[0], "-stop"))
			lookupMu.Lock()
			delete(identities, component)
			lookupMu.Unlock()
		}
		return exec.CommandContext(ctx, "/usr/bin/true")
	}

	for _, component := range []operatorComponent{componentPostgres, componentGateway, componentWorker} {
		require.NoError(t, runner.Run(t.Context(), fixedOperation{Component: component, Action: actionStart}))
	}
	for _, names := range operatorRequiredEnvironment {
		for _, name := range names {
			t.Setenv(name, "")
		}
	}
	for _, component := range []operatorComponent{componentWorker, componentGateway, componentPostgres} {
		require.NoError(t, runner.Run(t.Context(), fixedOperation{Component: component, Action: actionStop}))
	}
}

func TestLocalOperatorRunnerOwnerWriteFailureStillCleansPartialStart(t *testing.T) {
	setRequiredOperatorEnvironment(t)
	runner := newTestLocalOperatorRunner(t, nil)
	var identity atomic.Bool
	runner.composeIdentity = func(context.Context, operatorComponent) (string, bool, error) {
		if identity.Load() {
			return "postgres-partial", true, nil
		}
		return "", false, nil
	}
	var stopCalls atomic.Int32
	runner.newCommand = func(ctx context.Context, _ string, args ...string) *exec.Cmd {
		if len(args) == 1 && args[0] == "postgres-start" {
			identity.Store(true)
		}
		if len(args) == 1 && args[0] == "postgres-stop" {
			stopCalls.Add(1)
			identity.Store(false)
		}
		return exec.CommandContext(ctx, "/usr/bin/true")
	}
	runner.records.install = func(*os.Root, string, string) error { return errors.New("sentinel owner install failure") }

	status := newOperatorSupervisor(runner, 100*time.Millisecond).Apply(t.Context(), componentPostgres, actionStart)
	assert.Equal(t, "failed", status.State)
	assert.Equal(t, "process_exited", status.Code)
	assert.EqualValues(t, 1, stopCalls.Load())
	assert.False(t, identity.Load())
	assert.False(t, runner.Owned(componentPostgres))
	assertNoTemporaryRecords(t, runner.cfg.StateDir)
}

func TestLocalOperatorRunnerRejectsStalePIDAndOwnerRecords(t *testing.T) {
	t.Run("pid marker mismatch and symlink never receive a signal", func(t *testing.T) {
		setRequiredOperatorEnvironment(t)
		runner := newTestLocalOperatorRunner(t, nil)
		runner.terminateWait = 20 * time.Millisecond
		require.NoError(t, runner.Run(t.Context(), fixedOperation{Component: componentSpeech, Action: actionStart}))
		path := runner.pidPath(componentSpeech)
		original, err := os.ReadFile(path)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, []byte("1 stale-marker\n"), 0o600))
		assert.ErrorIs(t, runner.Run(t.Context(), fixedOperation{Component: componentSpeech, Action: actionStop}), errOperatorProcessExited)
		assert.True(t, runner.Owned(componentSpeech))
		require.NoError(t, os.WriteFile(path, original, 0o600))
		require.NoError(t, os.Chmod(path, 0o644))
		assert.ErrorIs(t, runner.Run(t.Context(), fixedOperation{Component: componentSpeech, Action: actionStop}), errOperatorProcessExited)
		assert.True(t, runner.Owned(componentSpeech))
		require.NoError(t, os.Remove(path))
		target := filepath.Join(t.TempDir(), "pid")
		require.NoError(t, os.WriteFile(target, original, 0o600))
		require.NoError(t, os.Symlink(target, path))
		assert.ErrorIs(t, runner.Run(t.Context(), fixedOperation{Component: componentSpeech, Action: actionStop}), errOperatorProcessExited)
		assert.True(t, runner.Owned(componentSpeech))
		require.NoError(t, os.Remove(path))
		require.NoError(t, os.WriteFile(path, original, 0o600))
		require.NoError(t, runner.Run(t.Context(), fixedOperation{Component: componentSpeech, Action: actionStop}))
	})

	t.Run("owner identity and mode mismatch never stop a container", func(t *testing.T) {
		setRequiredOperatorEnvironment(t)
		runner := newTestLocalOperatorRunner(t, nil)
		var identity atomic.Bool
		runner.composeIdentity = func(context.Context, operatorComponent) (string, bool, error) {
			return "gateway-owned", identity.Load(), nil
		}
		var stops atomic.Int32
		runner.newCommand = func(ctx context.Context, _ string, args ...string) *exec.Cmd {
			if len(args) == 1 && args[0] == "gateway-start" {
				identity.Store(true)
			}
			if len(args) == 1 && args[0] == "gateway-stop" {
				stops.Add(1)
			}
			return exec.CommandContext(ctx, "/usr/bin/true")
		}
		require.NoError(t, runner.Run(t.Context(), fixedOperation{Component: componentGateway, Action: actionStart}))
		path := runner.ownerPath(componentGateway)
		require.NoError(t, os.WriteFile(path, []byte("gateway-stale\n"), 0o600))
		assert.ErrorIs(t, runner.Run(t.Context(), fixedOperation{Component: componentGateway, Action: actionStop}), errOperatorProcessExited)
		require.NoError(t, os.WriteFile(path, []byte("gateway-owned\n"), 0o644))
		require.NoError(t, os.Chmod(path, 0o644))
		assert.ErrorIs(t, runner.Run(t.Context(), fixedOperation{Component: componentGateway, Action: actionStop}), errOperatorProcessExited)
		assert.Zero(t, stops.Load())
		require.NoError(t, os.Chmod(path, 0o600))
		require.NoError(t, runner.Run(t.Context(), fixedOperation{Component: componentGateway, Action: actionStop}))
		assert.EqualValues(t, 1, stops.Load())
	})
}

func TestLocalOperatorRunnerEscalatesSpeechTERMToBoundedKILL(t *testing.T) {
	setRequiredOperatorEnvironment(t)
	ready := filepath.Join(t.TempDir(), "ready")
	runner := newTestLocalOperatorRunner(t, func(ctx context.Context, program string, args ...string) *exec.Cmd {
		if strings.HasSuffix(program, "speech-launcher.sh") {
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLocalOperatorRunnerIgnoreTERMHelper$")
			cmd.Env = append(os.Environ(), "OPERATOR_TERM_HELPER=1", "OPERATOR_TERM_READY="+ready)
			return cmd
		}
		return exec.CommandContext(ctx, "/usr/bin/true")
	})
	runner.terminateWait = 20 * time.Millisecond
	require.NoError(t, runner.Run(t.Context(), fixedOperation{Component: componentSpeech, Action: actionStart}))
	require.Eventually(t, func() bool { _, err := os.Stat(ready); return err == nil }, time.Second, time.Millisecond)
	started := time.Now()
	require.NoError(t, runner.Run(t.Context(), fixedOperation{Component: componentSpeech, Action: actionStop}))
	elapsed := time.Since(started)
	assert.GreaterOrEqual(t, elapsed, 20*time.Millisecond)
	assert.Less(t, elapsed, 500*time.Millisecond)
	require.Eventually(t, func() bool { return !runner.Owned(componentSpeech) }, time.Second, time.Millisecond)
}

func TestLocalOperatorRunnerIgnoreTERMHelper(t *testing.T) {
	if os.Getenv("OPERATOR_TERM_HELPER") != "1" {
		return
	}
	signal.Ignore(syscall.SIGTERM)
	if err := os.WriteFile(os.Getenv("OPERATOR_TERM_READY"), nil, 0o600); err != nil {
		os.Exit(94)
	}
	for {
		time.Sleep(time.Second)
	}
}

func TestLocalOperatorRunnerClientStopUsesOnlyServerCallback(t *testing.T) {
	root, speech, profile := newRunnerRepository(t)
	var callbackCalls atomic.Int32
	var commandCalls atomic.Int32
	runnerInterface, err := newLocalOperatorRunner(operatorConfig{
		Enabled: true, StateDir: filepath.Join(t.TempDir(), "state"), RepositoryRoot: root, SpeechCommand: speech, SpeechProfile: profile, Readiness: fixedReadiness(),
		ClientStop: func(context.Context) error { callbackCalls.Add(1); return nil },
	}, func(ctx context.Context, _ string, _ ...string) *exec.Cmd {
		commandCalls.Add(1)
		return exec.CommandContext(ctx, "/usr/bin/false")
	}, func(context.Context, string) bool { return true })
	require.NoError(t, err)
	runner := runnerInterface.(*localOperatorRunner)
	t.Cleanup(func() { _ = runner.stateRoot.Close() })
	require.NoError(t, runner.Run(t.Context(), fixedOperation{Component: componentClient, Action: actionStart}))
	require.NoError(t, runner.Run(t.Context(), fixedOperation{Component: componentClient, Action: actionStop}))
	assert.EqualValues(t, 1, callbackCalls.Load())
	assert.Zero(t, commandCalls.Load())
	assert.False(t, runner.Owned(componentClient))
}

func TestLocalOperatorRunnerFreshPostgresStartReadyMigrateWithoutDocker(t *testing.T) {
	setRequiredOperatorEnvironment(t)
	root, speech, profile := newRunnerRepository(t)
	marker := filepath.Join(t.TempDir(), "migrated")
	wrapper := "#!/bin/sh\nset -eu\nstate=${RUNNER_FAKE_STATE:?}\ncase \"$1\" in\npostgres-id) cat \"$state\" 2>/dev/null || true ;;\npostgres-start) printf postgres-fresh >\"$state\" ;;\npostgres-ready) test \"$(cat \"$state\" 2>/dev/null)\" = postgres-fresh ;;\npostgres-migrate) : >\"${RUNNER_FAKE_MIGRATED:?}\" ;;\npostgres-stop) rm -f \"$state\" ;;\n*) exit 71 ;;\nesac\n"
	writeRunnerScript(t, root, "deploy/scripts/durable-compose.sh", wrapper)
	t.Setenv("RUNNER_FAKE_STATE", filepath.Join(t.TempDir(), "identity"))
	t.Setenv("RUNNER_FAKE_MIGRATED", marker)
	runnerInterface, err := newLocalOperatorRunner(operatorConfig{Enabled: true, StateDir: filepath.Join(t.TempDir(), "state"), RepositoryRoot: root, SpeechCommand: speech, SpeechProfile: profile, Readiness: fixedReadiness()}, nil, func(context.Context, string) bool { return true })
	require.NoError(t, err)
	runner := runnerInterface.(*localOperatorRunner)
	t.Cleanup(func() { _ = runner.stateRoot.Close() })
	supervisor := newOperatorSupervisor(runner, 5*time.Second)
	startStatus := supervisor.Apply(t.Context(), componentPostgres, actionStart)
	assert.Equal(t, "ready", startStatus.State, startStatus)
	migrateStatus := supervisor.Apply(t.Context(), componentPostgres, actionMigrate)
	assert.Equal(t, "ready", migrateStatus.State, migrateStatus)
	assert.FileExists(t, marker)
	require.NoError(t, runner.Run(t.Context(), fixedOperation{Component: componentPostgres, Action: actionStop}))
}

func newTestLocalOperatorRunner(t *testing.T, factory commandFactory) *localOperatorRunner {
	t.Helper()
	root, speech, profile := newRunnerRepository(t)
	if factory == nil {
		factory = func(ctx context.Context, program string, args ...string) *exec.Cmd {
			if strings.HasSuffix(program, "speech-launcher.sh") {
				return exec.CommandContext(ctx, program, args...)
			}
			if len(args) == 1 && strings.HasSuffix(args[0], "-id") {
				return exec.CommandContext(ctx, "/usr/bin/true")
			}
			return exec.CommandContext(ctx, "/usr/bin/true")
		}
	}
	runnerInterface, err := newLocalOperatorRunner(operatorConfig{Enabled: true, StateDir: filepath.Join(t.TempDir(), "state"), RepositoryRoot: root, SpeechCommand: speech, SpeechProfile: profile, Readiness: fixedReadiness(), ClientStop: func(context.Context) error { return nil }}, factory, func(context.Context, string) bool { return true })
	require.NoError(t, err)
	runner := runnerInterface.(*localOperatorRunner)
	runner.identityTimeout = 20 * time.Millisecond
	runner.identityRetryWait = time.Millisecond
	runner.terminateWait = 50 * time.Millisecond
	t.Cleanup(func() {
		if runner.Owned(componentSpeech) {
			_ = runner.Run(context.Background(), fixedOperation{Component: componentSpeech, Action: actionStop})
		}
		_ = runner.stateRoot.Close()
	})
	return runner
}

func newRunnerRepository(t *testing.T) (root, speech, profile string) {
	t.Helper()
	root = t.TempDir()
	speech = writeRunnerScript(t, root, "tools/speech-launcher.sh", "#!/bin/sh\ntrap '' TERM\nwhile :; do :; done\n")
	writeRunnerScript(t, root, "deploy/scripts/durable-compose.sh", "#!/bin/sh\nexit 0\n")
	profile = filepath.Join(root, "config", "speech-profile.yaml")
	require.NoError(t, os.MkdirAll(filepath.Dir(profile), 0o700))
	require.NoError(t, os.WriteFile(profile, []byte("profile"), 0o600))
	return root, speech, profile
}

func assertNoTemporaryRecords(t *testing.T, stateDir string) {
	t.Helper()
	entries, err := os.ReadDir(stateDir)
	require.NoError(t, err)
	for _, entry := range entries {
		assert.False(t, strings.HasSuffix(entry.Name(), ".tmp"), entry.Name())
	}
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
