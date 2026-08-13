package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOperatorSupervisorEnforcesDependenciesWithoutRequiringWorkerForVoice(t *testing.T) {
	runner := newFakeOperatorRunner()
	supervisor := newOperatorSupervisor(runner, 100*time.Millisecond)

	assert.Equal(t, "dependency_not_ready", supervisor.Apply(t.Context(), componentGateway, actionStart).Code)
	runner.setReady(componentSpeech, true)
	runner.setReady(componentPostgres, true)
	assert.Equal(t, "ready", supervisor.Apply(t.Context(), componentPostgres, actionMigrate).State)
	assert.Equal(t, "ready", supervisor.Apply(t.Context(), componentGateway, actionStart).State)
	assert.Equal(t, "ready", supervisor.Apply(t.Context(), componentClient, actionStart).State)
}

func TestOperatorSupervisorRejectsExactEnumsAndInvalidActionPairs(t *testing.T) {
	supervisor := newOperatorSupervisor(newFakeOperatorRunner(), 100*time.Millisecond)

	for _, request := range []struct {
		component operatorComponent
		action    operatorAction
	}{
		{"Speech", actionStart},
		{componentSpeech, "START"},
		{componentGateway, actionMigrate},
	} {
		status := supervisor.Apply(t.Context(), request.component, request.action)
		assert.Equal(t, "failed", status.State)
		assert.Equal(t, "operation_failed", status.Code)
		assert.False(t, status.Busy)
	}
}

func TestOperatorSupervisorRequiresPostgresMigrationBeforeDurableServices(t *testing.T) {
	runner := newFakeOperatorRunner()
	supervisor := newOperatorSupervisor(runner, 100*time.Millisecond)
	runner.setReady(componentPostgres, true)

	assert.Equal(t, "dependency_not_ready", supervisor.Apply(t.Context(), componentWorker, actionStart).Code)
	assert.Equal(t, "dependency_not_ready", supervisor.Apply(t.Context(), componentGateway, actionStart).Code)
	assert.Equal(t, "ready", supervisor.Apply(t.Context(), componentPostgres, actionMigrate).State)
	assert.Equal(t, "dependency_not_ready", supervisor.Apply(t.Context(), componentGateway, actionStart).Code)

	runner.setReady(componentSpeech, true)
	assert.Equal(t, "ready", supervisor.Apply(t.Context(), componentWorker, actionStart).State)
	assert.Equal(t, "ready", supervisor.Apply(t.Context(), componentGateway, actionStart).State)
}

func TestOperatorSupervisorRejectsUnsafeStops(t *testing.T) {
	runner := newFakeOperatorRunner()
	supervisor := newOperatorSupervisor(runner, 100*time.Millisecond)
	for _, component := range []operatorComponent{componentSpeech, componentPostgres, componentGateway, componentWorker, componentClient} {
		runner.setReady(component, true)
		runner.setOwned(component, true)
	}
	runner.setMigrated(true)

	assert.Equal(t, "dependency_running", supervisor.Apply(t.Context(), componentSpeech, actionStop).Code)
	assert.Equal(t, "dependency_running", supervisor.Apply(t.Context(), componentPostgres, actionStop).Code)
	assert.Equal(t, "dependency_running", supervisor.Apply(t.Context(), componentGateway, actionStop).Code)

	runner.setReady(componentClient, false)
	runner.setOwned(componentClient, false)
	assert.Equal(t, "stopped", supervisor.Apply(t.Context(), componentGateway, actionStop).Code)
}

func TestOperatorSupervisorSuppressesDuplicateOperations(t *testing.T) {
	runner := newFakeOperatorRunner()
	runner.block = make(chan struct{})
	supervisor := newOperatorSupervisor(runner, time.Second)

	firstDone := make(chan componentStatus, 1)
	go func() { firstDone <- supervisor.Apply(context.Background(), componentSpeech, actionStart) }()
	require.Eventually(t, func() bool { return runner.callsFor(componentSpeech, actionStart) == 1 }, time.Second, time.Millisecond)

	duplicate := supervisor.Apply(t.Context(), componentSpeech, actionStart)
	assert.Equal(t, "operation_in_progress", duplicate.Code)
	assert.True(t, duplicate.Busy)
	assert.Equal(t, 1, runner.callsFor(componentSpeech, actionStart))

	close(runner.block)
	assert.Equal(t, "ready", (<-firstDone).State)
}

func TestOperatorSupervisorTimeoutCleansOnlyNewlyOwnedProcess(t *testing.T) {
	runner := newFakeOperatorRunner()
	runner.startOwnsThenWaits = true
	supervisor := newOperatorSupervisor(runner, 20*time.Millisecond)

	status := supervisor.Apply(t.Context(), componentSpeech, actionStart)
	assert.Equal(t, "failed", status.State)
	assert.Equal(t, "startup_timeout", status.Code)
	assert.Equal(t, 1, runner.callsFor(componentSpeech, actionStop))

	runner = newFakeOperatorRunner()
	runner.setOwned(componentSpeech, true)
	runner.startOwnsThenWaits = true
	supervisor = newOperatorSupervisor(runner, 20*time.Millisecond)
	status = supervisor.Apply(t.Context(), componentSpeech, actionStart)
	assert.Equal(t, "startup_timeout", status.Code)
	assert.Equal(t, 0, runner.callsFor(componentSpeech, actionStop))
}

func TestOperatorSupervisorKeepsTimedOutStartBusyUntilCleanupCompletes(t *testing.T) {
	runner := newFakeOperatorRunner()
	runner.startOwnsThenWaits = true
	runner.stopBlock = make(chan struct{})
	supervisor := newOperatorSupervisor(runner, 100*time.Millisecond)

	firstDone := make(chan componentStatus, 1)
	go func() { firstDone <- supervisor.Apply(context.Background(), componentSpeech, actionStart) }()
	require.Eventually(t, func() bool { return runner.callsFor(componentSpeech, actionStop) == 1 }, time.Second, time.Millisecond)

	second := supervisor.Apply(t.Context(), componentSpeech, actionStart)
	assert.Equal(t, "operation_in_progress", second.Code)
	assert.True(t, second.Busy)
	assert.Equal(t, 1, runner.callsFor(componentSpeech, actionStart))
	assert.Equal(t, 1, runner.callsFor(componentSpeech, actionStop))

	close(runner.stopBlock)
	assert.Equal(t, "startup_timeout", (<-firstDone).Code)
}

func TestOperatorSupervisorRejectsStopWhileDependantIsStarting(t *testing.T) {
	runner := newFakeOperatorRunner()
	supervisor := newOperatorSupervisor(runner, time.Second)
	runner.setReady(componentSpeech, true)
	runner.setOwned(componentSpeech, true)
	runner.setReady(componentPostgres, true)
	runner.setOwned(componentPostgres, true)
	assert.Equal(t, "ready", supervisor.Apply(t.Context(), componentPostgres, actionMigrate).State)
	runner.block = make(chan struct{})

	gatewayDone := make(chan componentStatus, 1)
	go func() { gatewayDone <- supervisor.Apply(context.Background(), componentGateway, actionStart) }()
	require.Eventually(t, func() bool { return runner.callsFor(componentGateway, actionStart) == 1 }, time.Second, time.Millisecond)

	assert.Equal(t, "dependency_running", supervisor.Apply(t.Context(), componentSpeech, actionStop).Code)
	assert.Equal(t, "dependency_running", supervisor.Apply(t.Context(), componentPostgres, actionStop).Code)
	assert.Equal(t, 0, runner.callsFor(componentSpeech, actionStop))
	assert.Equal(t, 0, runner.callsFor(componentPostgres, actionStop))

	close(runner.block)
	assert.Equal(t, "ready", (<-gatewayDone).State)
}

func TestOperatorSupervisorRejectsStopOfUnownedComponent(t *testing.T) {
	supervisor := newOperatorSupervisor(newFakeOperatorRunner(), 100*time.Millisecond)

	status := supervisor.Apply(t.Context(), componentSpeech, actionStop)
	assert.Equal(t, "failed", status.State)
	assert.Equal(t, "process_exited", status.Code)
}

func TestOperatorSupervisorSnapshotNeverExposesRunnerErrors(t *testing.T) {
	runner := newFakeOperatorRunner()
	runner.err = errors.New("token=secret command=/private/tool path=/private transcript=words audio=bytes")
	supervisor := newOperatorSupervisor(runner, 100*time.Millisecond)

	status := supervisor.Apply(t.Context(), componentSpeech, actionStart)
	assert.Equal(t, "operation_failed", status.Code)
	for _, snapshot := range supervisor.Snapshot(t.Context()) {
		assert.LessOrEqual(t, len(snapshot.State), 32)
		assert.LessOrEqual(t, len(snapshot.Code), 32)
		for _, sentinel := range []string{"secret", "command", "private", "transcript", "audio"} {
			assert.NotContains(t, snapshot.State, sentinel)
			assert.NotContains(t, snapshot.Code, sentinel)
		}
	}
}

type fakeOperatorRunner struct {
	mu                 sync.Mutex
	ready              map[operatorComponent]bool
	owned              map[operatorComponent]bool
	migrated           bool
	calls              map[fixedOperation]int
	err                error
	block              chan struct{}
	stopBlock          chan struct{}
	startOwnsThenWaits bool
}

func newFakeOperatorRunner() *fakeOperatorRunner {
	return &fakeOperatorRunner{
		ready: map[operatorComponent]bool{}, owned: map[operatorComponent]bool{}, calls: map[fixedOperation]int{},
	}
}

func (runner *fakeOperatorRunner) Run(ctx context.Context, operation fixedOperation) error {
	runner.mu.Lock()
	runner.calls[operation]++
	if operation.Action == actionStart {
		runner.owned[operation.Component] = true
	}
	if operation.Action == actionStop {
		runner.owned[operation.Component] = false
		runner.ready[operation.Component] = false
	}
	err, block, stopBlock, wait := runner.err, runner.block, runner.stopBlock, runner.startOwnsThenWaits
	runner.mu.Unlock()
	if err != nil {
		return err
	}
	if block != nil && operation.Action != actionStop {
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if operation.Action == actionStop && stopBlock != nil {
		select {
		case <-stopBlock:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if wait && operation.Action == actionStart {
		<-ctx.Done()
		return ctx.Err()
	}
	runner.mu.Lock()
	defer runner.mu.Unlock()
	if operation.Action == actionStart {
		runner.ready[operation.Component] = true
	}
	if operation.Action == actionMigrate {
		runner.migrated = true
	}
	return nil
}

func (runner *fakeOperatorRunner) Ready(_ context.Context, component operatorComponent) bool {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return runner.ready[component]
}

func (runner *fakeOperatorRunner) Owned(component operatorComponent) bool {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return runner.owned[component]
}

func (runner *fakeOperatorRunner) setReady(component operatorComponent, ready bool) {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	runner.ready[component] = ready
}
func (runner *fakeOperatorRunner) setOwned(component operatorComponent, owned bool) {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	runner.owned[component] = owned
}
func (runner *fakeOperatorRunner) setMigrated(migrated bool) {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	runner.migrated = migrated
}
func (runner *fakeOperatorRunner) callsFor(component operatorComponent, action operatorAction) int {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return runner.calls[fixedOperation{Component: component, Action: action}]
}
