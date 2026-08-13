package main

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	operatorReadinessTimeout = 100 * time.Millisecond
	operatorConnectTimeout   = 50 * time.Millisecond
	operatorTerminateWait    = time.Second
)

type operatorConfig struct {
	Enabled        bool
	StateDir       string
	RepositoryRoot string
	SpeechCommand  string
	SpeechProfile  string
	Readiness      map[operatorComponent]string
}

type commandFactory func(context.Context, string, ...string) *exec.Cmd
type readinessProbe func(context.Context, string) bool

type localOperatorRunner struct {
	cfg        operatorConfig
	newCommand commandFactory
	probe      readinessProbe

	mu           sync.Mutex
	children     map[operatorComponent]*ownedChild
	composeOwned map[operatorComponent]bool
}

type ownedChild struct {
	cmd    *exec.Cmd
	pid    int
	marker string
	done   chan struct{}
}

func newLocalOperatorRunner(cfg operatorConfig, factory commandFactory, probe readinessProbe) (operatorRunner, error) {
	if err := cfg.validate(); err != nil {
		return nil, errOperatorMissingConfiguration
	}
	if err := prepareOperatorStateDir(cfg.StateDir); err != nil {
		return nil, errOperatorMissingConfiguration
	}
	if factory == nil {
		factory = exec.CommandContext
	}
	if probe == nil {
		probe = probeLocalReadiness
	}
	readiness := make(map[operatorComponent]string, len(cfg.Readiness))
	for component, endpoint := range cfg.Readiness {
		readiness[component] = endpoint
	}
	cfg.Readiness = readiness
	return &localOperatorRunner{
		cfg: cfg, newCommand: factory, probe: probe,
		children: make(map[operatorComponent]*ownedChild), composeOwned: make(map[operatorComponent]bool),
	}, nil
}

func (cfg operatorConfig) validate() error {
	if !cfg.Enabled || !filepath.IsAbs(cfg.StateDir) || !filepath.IsAbs(cfg.RepositoryRoot) || cfg.SpeechCommand == "" || cfg.SpeechProfile == "" {
		return errors.New("operator configuration unavailable")
	}
	root, err := filepath.EvalSymlinks(cfg.RepositoryRoot)
	if err != nil {
		return errors.New("operator configuration unavailable")
	}
	rootInfo, err := os.Stat(root)
	if err != nil || !rootInfo.IsDir() {
		return errors.New("operator configuration unavailable")
	}
	if err := validateOperatorPath(root, cfg.SpeechCommand, true); err != nil {
		return err
	}
	if err := validateOperatorPath(root, cfg.SpeechProfile, false); err != nil {
		return err
	}
	if err := validateOperatorPath(root, filepath.Join(root, "deploy", "scripts", "durable-compose.sh"), true); err != nil {
		return err
	}
	for _, component := range []operatorComponent{componentSpeech, componentGateway, componentWorker, componentClient} {
		if err := validateOperatorEndpoint(cfg.Readiness[component]); err != nil {
			return err
		}
	}
	if info, err := os.Stat(cfg.StateDir); err == nil && (!info.IsDir() || info.Mode().Perm()&0o007 != 0) {
		return errors.New("operator configuration unavailable")
	} else if err != nil && !os.IsNotExist(err) {
		return errors.New("operator configuration unavailable")
	}
	return nil
}

func validateOperatorPath(root, path string, executable bool) error {
	if !filepath.IsAbs(path) {
		return errors.New("operator configuration unavailable")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || !pathInside(root, resolved) {
		return errors.New("operator configuration unavailable")
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.Mode().IsRegular() || (executable && info.Mode().Perm()&0o111 == 0) {
		return errors.New("operator configuration unavailable")
	}
	return nil
}

func pathInside(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func validateOperatorEndpoint(raw string) error {
	endpoint, err := url.Parse(raw)
	if err != nil || endpoint.Scheme != "http" || endpoint.User != nil || endpoint.Host == "" || endpoint.Opaque != "" || endpoint.RawQuery != "" || endpoint.Fragment != "" || endpoint.Port() == "" {
		return errors.New("operator configuration unavailable")
	}
	ip := net.ParseIP(endpoint.Hostname())
	if ip == nil || !ip.IsLoopback() {
		return errors.New("operator configuration unavailable")
	}
	return nil
}

func prepareOperatorStateDir(stateDir string) error {
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return err
	}
	info, err := os.Stat(stateDir)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0o007 != 0 {
		return errors.New("unsafe operator state directory")
	}
	return os.Chmod(stateDir, 0o700)
}

func (runner *localOperatorRunner) Run(ctx context.Context, operation fixedOperation) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if !runner.cfg.Enabled || !validOperation(operation.Component, operation.Action) {
		return errOperatorMissingConfiguration
	}
	switch operation.Component {
	case componentClient:
		if operation.Action == actionStart {
			return nil
		}
		return errOperatorProcessExited
	case componentSpeech:
		if operation.Action == actionStart {
			return runner.startSpeech(ctx)
		}
		return runner.stopSpeech(ctx)
	case componentPostgres:
		if operation.Action == actionMigrate {
			return runner.runCompose(ctx, "migrate-check", "")
		}
		if operation.Action == actionStart {
			return runner.runCompose(ctx, "migrate-up", componentPostgres)
		}
	case componentGateway, componentWorker:
		if operation.Action == actionStart {
			return runner.runCompose(ctx, "up", operation.Component)
		}
	}
	return runner.stopCompose(ctx, operation.Component)
}

func (runner *localOperatorRunner) Ready(ctx context.Context, component operatorComponent) bool {
	if !runner.cfg.Enabled {
		return false
	}
	readyCtx, cancel := context.WithTimeout(nonNilContext(ctx), operatorReadinessTimeout)
	defer cancel()
	if component == componentPostgres {
		return runner.runFixed(readyCtx, runner.composeCommand(), "migrate-check") == nil
	}
	endpoint, ok := runner.cfg.Readiness[component]
	return ok && runner.probe(readyCtx, endpoint)
}

func (runner *localOperatorRunner) Owned(component operatorComponent) bool {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	if component == componentSpeech {
		_, owned := runner.children[component]
		return owned
	}
	return runner.composeOwned[component]
}

func (runner *localOperatorRunner) startSpeech(ctx context.Context) error {
	runner.mu.Lock()
	if _, started := runner.children[componentSpeech]; started {
		runner.mu.Unlock()
		return nil
	}
	runner.mu.Unlock()

	cmd := runner.newCommand(ctx, runner.cfg.SpeechCommand, runner.cfg.SpeechProfile)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		return commandFailure(ctx)
	}
	marker, err := newOperatorMarker()
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return errOperatorProcessExited
	}
	child := &ownedChild{cmd: cmd, pid: cmd.Process.Pid, marker: marker, done: make(chan struct{})}
	if err := runner.writePID(componentSpeech, child); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return errOperatorProcessExited
	}
	runner.mu.Lock()
	runner.children[componentSpeech] = child
	runner.mu.Unlock()
	go runner.waitForSpeech(child)
	return nil
}

func (runner *localOperatorRunner) waitForSpeech(child *ownedChild) {
	_ = child.cmd.Wait()
	close(child.done)
	runner.mu.Lock()
	if runner.children[componentSpeech] == child {
		delete(runner.children, componentSpeech)
		_ = os.Remove(runner.pidPath(componentSpeech))
	}
	runner.mu.Unlock()
}

func (runner *localOperatorRunner) stopSpeech(ctx context.Context) error {
	runner.mu.Lock()
	child := runner.children[componentSpeech]
	runner.mu.Unlock()
	if child == nil || !runner.pidMatches(componentSpeech, child) {
		return errOperatorProcessExited
	}
	if err := child.cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return errOperatorProcessExited
	}
	waitCtx, cancel := context.WithTimeout(nonNilContext(ctx), operatorTerminateWait)
	defer cancel()
	select {
	case <-child.done:
	case <-waitCtx.Done():
		_ = child.cmd.Process.Kill()
		select {
		case <-child.done:
		case <-time.After(operatorTerminateWait):
			return errOperatorProcessExited
		}
	}
	return nil
}

func (runner *localOperatorRunner) runCompose(ctx context.Context, verb string, component operatorComponent) error {
	if err := runner.runFixed(ctx, runner.composeCommand(), verb); err != nil {
		return err
	}
	if component != "" {
		runner.mu.Lock()
		runner.composeOwned[component] = true
		runner.mu.Unlock()
	}
	return nil
}

func (runner *localOperatorRunner) stopCompose(ctx context.Context, component operatorComponent) error {
	runner.mu.Lock()
	owned := runner.composeOwned[component]
	runner.mu.Unlock()
	if !owned {
		return errOperatorProcessExited
	}
	if err := runner.runFixed(ctx, runner.composeCommand(), "down"); err != nil {
		return err
	}
	runner.mu.Lock()
	for _, composeComponent := range []operatorComponent{componentPostgres, componentGateway, componentWorker} {
		delete(runner.composeOwned, composeComponent)
	}
	runner.mu.Unlock()
	return nil
}

func (runner *localOperatorRunner) composeCommand() string {
	return filepath.Join(runner.cfg.RepositoryRoot, "deploy", "scripts", "durable-compose.sh")
}

func (runner *localOperatorRunner) runFixed(ctx context.Context, program string, args ...string) error {
	cmd := runner.newCommand(ctx, program, args...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		return commandFailure(ctx)
	}
	return nil
}

func commandFailure(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return errOperatorProcessExited
}

func (runner *localOperatorRunner) pidPath(component operatorComponent) string {
	return filepath.Join(runner.cfg.StateDir, string(component)+".pid")
}

func (runner *localOperatorRunner) writePID(component operatorComponent, child *ownedChild) error {
	return os.WriteFile(runner.pidPath(component), []byte(strconv.Itoa(child.pid)+" "+child.marker+"\n"), 0o600)
}

func (runner *localOperatorRunner) pidMatches(component operatorComponent, child *ownedChild) bool {
	path := runner.pidPath(component)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	return string(data) == strconv.Itoa(child.pid)+" "+child.marker+"\n"
}

func newOperatorMarker() (string, error) {
	var bytes [16]byte
	if _, err := io.ReadFull(rand.Reader, bytes[:]); err != nil {
		return "", err
	}
	return strconv.FormatUint(uint64(bytes[0])<<56|uint64(bytes[1])<<48|uint64(bytes[2])<<40|uint64(bytes[3])<<32|uint64(bytes[4])<<24|uint64(bytes[5])<<16|uint64(bytes[6])<<8|uint64(bytes[7]), 16) + strconv.FormatUint(uint64(bytes[8])<<56|uint64(bytes[9])<<48|uint64(bytes[10])<<40|uint64(bytes[11])<<32|uint64(bytes[12])<<24|uint64(bytes[13])<<16|uint64(bytes[14])<<8|uint64(bytes[15]), 16), nil
}

func probeLocalReadiness(ctx context.Context, endpoint string) bool {
	dialer := net.Dialer{Timeout: operatorConnectTimeout}
	transport := &http.Transport{DialContext: dialer.DialContext}
	client := &http.Client{Transport: transport, Timeout: operatorReadinessTimeout}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return false
	}
	response, err := client.Do(request)
	if err != nil {
		return false
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1))
	return response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices
}

func nonNilContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
