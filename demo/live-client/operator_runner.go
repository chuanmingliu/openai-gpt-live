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
	ClientStop     func(context.Context) error
}

type commandFactory func(context.Context, string, ...string) *exec.Cmd
type readinessProbe func(context.Context, string) bool

type localOperatorRunner struct {
	cfg        operatorConfig
	newCommand commandFactory
	probe      readinessProbe

	mu           sync.Mutex
	children     map[operatorComponent]*ownedChild
	composeOwned map[operatorComponent]string
	clientOwned  bool
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
	root, _ := filepath.EvalSymlinks(cfg.RepositoryRoot)
	speechCommand, _ := filepath.EvalSymlinks(cfg.SpeechCommand)
	speechProfile, _ := filepath.EvalSymlinks(cfg.SpeechProfile)
	stateParent, _ := filepath.EvalSymlinks(filepath.Dir(cfg.StateDir))
	cfg.RepositoryRoot, cfg.SpeechCommand, cfg.SpeechProfile = root, speechCommand, speechProfile
	cfg.StateDir = filepath.Join(stateParent, filepath.Base(cfg.StateDir))
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
		children: make(map[operatorComponent]*ownedChild), composeOwned: make(map[operatorComponent]string), clientOwned: cfg.ClientStop != nil,
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
	if info, err := os.Lstat(cfg.StateDir); err == nil && (!info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0) {
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
	parent, err := filepath.EvalSymlinks(filepath.Dir(stateDir))
	if err != nil {
		return errors.New("unsafe operator state directory")
	}
	for parent != "." && parent != string(filepath.Separator) {
		info, err := os.Lstat(parent)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 {
			return errors.New("unsafe operator state directory")
		}
		next := filepath.Dir(parent)
		if next == parent {
			break
		}
		parent = next
	}
	if err := os.Mkdir(stateDir, 0o700); err != nil && !os.IsExist(err) {
		return err
	}
	info, err := os.Lstat(stateDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 {
		return errors.New("unsafe operator state directory")
	}
	return nil
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
			if runner.cfg.ClientStop == nil || !runner.Ready(ctx, componentClient) {
				return errOperatorMissingConfiguration
			}
			runner.mu.Lock()
			runner.clientOwned = true
			runner.mu.Unlock()
			return nil
		}
		if runner.cfg.ClientStop == nil {
			return errOperatorProcessExited
		}
		if err := runner.cfg.ClientStop(ctx); err != nil {
			return commandFailure(ctx)
		}
		runner.mu.Lock()
		runner.clientOwned = false
		runner.mu.Unlock()
		return nil
	case componentSpeech:
		if operation.Action == actionStart {
			return runner.startSpeech(ctx)
		}
		return runner.stopSpeech(ctx)
	case componentPostgres:
		if operation.Action == actionMigrate {
			return runner.runCompose(ctx, "postgres-migrate", "")
		}
		if operation.Action == actionStart {
			return runner.runCompose(ctx, "postgres-start", componentPostgres)
		}
	case componentGateway, componentWorker:
		if operation.Action == actionStart {
			return runner.runCompose(ctx, string(operation.Component)+"-start", operation.Component)
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
		return runner.requiredEnvironment(fixedOperation{Component: componentPostgres, Action: actionStart}) && runner.runFixed(readyCtx, runner.composeCommand(), "postgres-ready") == nil
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
	if component == componentClient {
		return runner.clientOwned
	}
	_, owned := runner.composeOwned[component]
	return owned
}

func (runner *localOperatorRunner) startSpeech(ctx context.Context) error {
	runner.mu.Lock()
	if _, started := runner.children[componentSpeech]; started {
		runner.mu.Unlock()
		return nil
	}
	runner.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}
	cmd := runner.newCommand(context.Background(), runner.cfg.SpeechCommand, runner.cfg.SpeechProfile)
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
	operation := fixedOperation{Component: component, Action: actionStart}
	if component == "" {
		operation = fixedOperation{Component: componentPostgres, Action: actionMigrate}
	}
	if !runner.requiredEnvironment(operation) {
		return errOperatorMissingConfiguration
	}
	before := ""
	if component != "" {
		before = runner.composeID(ctx, component)
	}
	err := runner.runFixed(ctx, runner.composeCommand(), verb)
	if component == "" {
		return err
	}
	after := runner.composeID(context.Background(), component)
	if after != "" && after != before {
		if writeErr := runner.writeOwner(component, after); writeErr != nil {
			return errOperatorProcessExited
		}
		runner.mu.Lock()
		runner.composeOwned[component] = after
		runner.mu.Unlock()
	}
	return err
}

func (runner *localOperatorRunner) stopCompose(ctx context.Context, component operatorComponent) error {
	runner.mu.Lock()
	id, owned := runner.composeOwned[component]
	runner.mu.Unlock()
	if !owned || id == "" || !runner.ownerMatches(component, id) || runner.composeID(ctx, component) != id {
		return errOperatorProcessExited
	}
	if !runner.requiredEnvironment(fixedOperation{Component: component, Action: actionStop}) {
		return errOperatorMissingConfiguration
	}
	if err := runner.runFixed(ctx, runner.composeCommand(), string(component)+"-stop"); err != nil {
		return err
	}
	runner.mu.Lock()
	delete(runner.composeOwned, component)
	runner.mu.Unlock()
	_ = os.Remove(runner.ownerPath(component))
	return nil
}

func (runner *localOperatorRunner) composeID(ctx context.Context, component operatorComponent) string {
	cmd := runner.newCommand(ctx, runner.composeCommand(), string(component)+"-id")
	output, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(output))
}

var operatorRequiredEnvironment = map[operatorComponent][]string{
	componentPostgres: {"POSTGRES_PASSWORD"},
	componentGateway:  {"POSTGRES_PASSWORD", "S2S_API_KEY", "LIVE_STATIC_BEARER_TOKEN", "LIVE_ADMIN_BEARER_TOKEN", "LIVE_PRIVACY_HMAC_KEY", "LIVE_RETENTION_HMAC_KEY", "LIVE_RECONNECT_HMAC_KEY", "LIVE_TOOL_ENCRYPTION_KEY", "LIVE_EXPECTED_CONFIG_HASH", "S2S_IMAGE", "S2S_CONFIG_PATH"},
	componentWorker:   {"POSTGRES_PASSWORD", "DEEPSEEK_API_KEY", "LIVE_STATIC_BEARER_TOKEN", "LIVE_ADMIN_BEARER_TOKEN", "LIVE_PRIVACY_HMAC_KEY", "LIVE_RETENTION_HMAC_KEY", "LIVE_TOOL_ENCRYPTION_KEY", "LIVE_EXPECTED_CONFIG_HASH", "S2S_IMAGE", "S2S_CONFIG_PATH"},
}

func (runner *localOperatorRunner) requiredEnvironment(operation fixedOperation) bool {
	for _, name := range operatorRequiredEnvironment[operation.Component] {
		if value, ok := os.LookupEnv(name); !ok || value == "" {
			return false
		}
	}
	return true
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

func (runner *localOperatorRunner) ownerPath(component operatorComponent) string {
	return filepath.Join(runner.cfg.StateDir, string(component)+".owner")
}

func (runner *localOperatorRunner) writeOwner(component operatorComponent, id string) error {
	file, err := os.OpenFile(runner.ownerPath(component), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.WriteString(id + "\n"); err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil || info.Mode().Perm() != 0o600 {
		return errors.New("unsafe operator ownership record")
	}
	return nil
}

func (runner *localOperatorRunner) ownerMatches(component operatorComponent, id string) bool {
	info, err := os.Lstat(runner.ownerPath(component))
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return false
	}
	data, err := os.ReadFile(runner.ownerPath(component))
	return err == nil && string(data) == id+"\n"
}

func (runner *localOperatorRunner) writePID(component operatorComponent, child *ownedChild) error {
	path := runner.pidPath(component)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.Write([]byte(strconv.Itoa(child.pid) + " " + child.marker + "\n")); err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil || info.Mode().Perm() != 0o600 {
		return errors.New("unsafe operator pid record")
	}
	return nil
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
	client := &http.Client{Transport: transport, Timeout: operatorReadinessTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
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
