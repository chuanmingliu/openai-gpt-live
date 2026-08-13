package main

import (
	"context"
	"errors"
	"sync"
	"time"
)

type operatorComponent string

const (
	componentSpeech   operatorComponent = "speech"
	componentPostgres operatorComponent = "postgres"
	componentGateway  operatorComponent = "gateway"
	componentWorker   operatorComponent = "worker"
	componentClient   operatorComponent = "client"
)

type operatorAction string

const (
	actionStart   operatorAction = "start"
	actionStop    operatorAction = "stop"
	actionMigrate operatorAction = "migrate"
)

// fixedOperation is deliberately limited to the operator's fixed allowlist.
// The local runner maps it to its reviewed server-side command.
type fixedOperation struct {
	Component operatorComponent
	Action    operatorAction
}

type componentStatus struct {
	Component operatorComponent
	State     string
	Code      string
	Ready     bool
	Busy      bool
}

type operatorRunner interface {
	Run(context.Context, fixedOperation) error
	Ready(context.Context, operatorComponent) bool
	Owned(operatorComponent) bool
}

var (
	errOperatorMissingConfiguration = errors.New("operator configuration unavailable")
	errOperatorProcessExited        = errors.New("operator process exited")
)

type operatorSupervisor struct {
	runner  operatorRunner
	timeout time.Duration

	mu       sync.Mutex
	inFlight map[operatorComponent]operatorAction
	failed   map[operatorComponent]string
	migrated bool
}

func newOperatorSupervisor(runner operatorRunner, timeout time.Duration) *operatorSupervisor {
	if timeout <= 0 {
		timeout = time.Second
	}
	return &operatorSupervisor{
		runner: runner, timeout: timeout, inFlight: make(map[operatorComponent]operatorAction), failed: make(map[operatorComponent]string),
	}
}

func (supervisor *operatorSupervisor) Snapshot(ctx context.Context) []componentStatus {
	components := []operatorComponent{componentSpeech, componentPostgres, componentGateway, componentWorker, componentClient}
	statuses := make([]componentStatus, 0, len(components))
	for _, component := range components {
		statuses = append(statuses, supervisor.status(ctx, component))
	}
	return statuses
}

func (supervisor *operatorSupervisor) Apply(ctx context.Context, component operatorComponent, action operatorAction) componentStatus {
	if !validOperation(component, action) || supervisor.runner == nil {
		return componentStatus{Component: component, State: "failed", Code: "operation_failed"}
	}
	if ctx == nil {
		ctx = context.Background()
	}

	supervisor.mu.Lock()
	if _, busy := supervisor.inFlight[component]; busy {
		supervisor.mu.Unlock()
		return supervisor.status(ctx, component)
	}
	if code := supervisor.dependencyCodeLocked(ctx, component, action); code != "" {
		supervisor.mu.Unlock()
		return componentStatus{Component: component, State: "failed", Code: code, Ready: supervisor.runner.Ready(ctx, component)}
	}
	wasOwned := supervisor.runner.Owned(component)
	supervisor.inFlight[component] = action
	delete(supervisor.failed, component)
	supervisor.mu.Unlock()

	actionCtx, cancel := context.WithTimeout(ctx, supervisor.timeout)
	err := supervisor.runner.Run(actionCtx, fixedOperation{Component: component, Action: action})
	cancel()

	supervisor.mu.Lock()
	delete(supervisor.inFlight, component)
	if err == nil && action == actionMigrate {
		supervisor.migrated = true
	}
	if err != nil {
		supervisor.failed[component] = operationCode(err)
	}
	supervisor.mu.Unlock()

	if err != nil && action == actionStart && !wasOwned && supervisor.runner.Owned(component) {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), supervisor.timeout)
		_ = supervisor.runner.Run(cleanupCtx, fixedOperation{Component: component, Action: actionStop})
		cleanupCancel()
	}
	return supervisor.status(ctx, component)
}

func (supervisor *operatorSupervisor) status(ctx context.Context, component operatorComponent) componentStatus {
	supervisor.mu.Lock()
	action, busy := supervisor.inFlight[component]
	failedCode := supervisor.failed[component]
	supervisor.mu.Unlock()
	ready := supervisor.runner != nil && supervisor.runner.Ready(ctx, component)
	if busy {
		state := "starting"
		if action == actionStop {
			state = "stopping"
		}
		return componentStatus{Component: component, State: state, Code: "operation_in_progress", Ready: ready, Busy: true}
	}
	if failedCode != "" {
		return componentStatus{Component: component, State: "failed", Code: failedCode, Ready: ready}
	}
	if ready {
		return componentStatus{Component: component, State: "ready", Code: "ready", Ready: true}
	}
	return componentStatus{Component: component, State: "stopped", Code: "stopped"}
}

func (supervisor *operatorSupervisor) dependencyCodeLocked(ctx context.Context, component operatorComponent, action operatorAction) string {
	if action == actionStop {
		if !supervisor.runner.Owned(component) {
			return "process_exited"
		}
		for _, dependant := range dependants[component] {
			if supervisor.runner.Ready(ctx, dependant) {
				return "dependency_running"
			}
		}
		return ""
	}
	if action == actionMigrate {
		if !supervisor.runner.Ready(ctx, componentPostgres) {
			return "dependency_not_ready"
		}
		return ""
	}
	for _, dependency := range dependencies[component] {
		if !supervisor.runner.Ready(ctx, dependency) {
			return "dependency_not_ready"
		}
	}
	if (component == componentGateway || component == componentWorker) && !supervisor.migrated {
		return "dependency_not_ready"
	}
	return ""
}

var dependencies = map[operatorComponent][]operatorComponent{
	componentGateway: {componentSpeech, componentPostgres},
	componentWorker:  {componentPostgres},
	componentClient:  {componentGateway},
}

var dependants = map[operatorComponent][]operatorComponent{
	componentSpeech:   {componentGateway},
	componentPostgres: {componentGateway, componentWorker},
	componentGateway:  {componentClient},
}

func validOperation(component operatorComponent, action operatorAction) bool {
	switch component {
	case componentSpeech, componentPostgres, componentGateway, componentWorker, componentClient:
	default:
		return false
	}
	switch action {
	case actionStart, actionStop:
		return true
	case actionMigrate:
		return component == componentPostgres
	default:
		return false
	}
}

func operationCode(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "startup_timeout"
	case errors.Is(err, errOperatorMissingConfiguration):
		return "missing_configuration"
	case errors.Is(err, errOperatorProcessExited):
		return "process_exited"
	default:
		return "operation_failed"
	}
}
