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
	operatorIdentityRetry    = 5 * time.Millisecond
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
	stateRoot  *os.Root
	stateFile  os.FileInfo
	records    operatorRecordOperations

	composeIdentity   func(context.Context, operatorComponent) (string, bool, error)
	identityTimeout   time.Duration
	identityRetryWait time.Duration
	terminateWait     time.Duration

	mu               sync.Mutex
	children         map[operatorComponent]*ownedChild
	composeOwned     map[operatorComponent]string
	composeEphemeral map[operatorComponent]string
	composePending   map[operatorComponent]bool
	clientOwned      bool
	speechFile       os.FileInfo
	profileFile      os.FileInfo
}

type ownedChild struct {
	cmd    *exec.Cmd
	pid    int
	marker string
	done   chan struct{}
}

type operatorRecordOperations struct {
	write         func(*os.File, []byte) error
	syncFile      func(*os.File) error
	install       func(*os.Root, string, string) error
	syncDirectory func(*os.Root) error
}

func newLocalOperatorRunner(cfg operatorConfig, factory commandFactory, probe readinessProbe) (operatorRunner, error) {
	if err := cfg.validate(); err != nil {
		return nil, errOperatorMissingConfiguration
	}
	root, _ := filepath.EvalSymlinks(cfg.RepositoryRoot)
	speechCommand, _ := filepath.EvalSymlinks(cfg.SpeechCommand)
	speechProfile, _ := filepath.EvalSymlinks(cfg.SpeechProfile)
	speechFile, _ := os.Stat(speechCommand)
	profileFile, _ := os.Stat(speechProfile)
	stateParent, _ := filepath.EvalSymlinks(filepath.Dir(cfg.StateDir))
	cfg.RepositoryRoot, cfg.SpeechCommand, cfg.SpeechProfile = root, speechCommand, speechProfile
	cfg.StateDir = filepath.Join(stateParent, filepath.Base(cfg.StateDir))
	if err := prepareOperatorStateDir(cfg.StateDir); err != nil {
		return nil, errOperatorMissingConfiguration
	}
	stateFile, err := os.Lstat(cfg.StateDir)
	if err != nil {
		return nil, errOperatorMissingConfiguration
	}
	stateRoot, err := os.OpenRoot(cfg.StateDir)
	if err != nil {
		return nil, errOperatorMissingConfiguration
	}
	rootFile, rootErr := stateRoot.Stat(".")
	currentFile, currentErr := os.Lstat(cfg.StateDir)
	if rootErr != nil || currentErr != nil || !os.SameFile(stateFile, rootFile) || !os.SameFile(stateFile, currentFile) {
		_ = stateRoot.Close()
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
	runner := &localOperatorRunner{
		cfg: cfg, newCommand: factory, probe: probe,
		stateRoot: stateRoot, stateFile: stateFile, records: defaultOperatorRecordOperations(),
		identityTimeout: operatorReadinessTimeout, identityRetryWait: operatorIdentityRetry, terminateWait: operatorTerminateWait,
		children: make(map[operatorComponent]*ownedChild), composeOwned: make(map[operatorComponent]string), composeEphemeral: make(map[operatorComponent]string), composePending: make(map[operatorComponent]bool), clientOwned: cfg.ClientStop != nil, speechFile: speechFile, profileFile: profileFile,
	}
	runner.composeIdentity = runner.queryComposeIdentity
	return runner, nil
}

func defaultOperatorRecordOperations() operatorRecordOperations {
	return operatorRecordOperations{
		write: func(file *os.File, data []byte) error {
			written, err := file.Write(data)
			if err == nil && written != len(data) {
				return io.ErrShortWrite
			}
			return err
		},
		syncFile: (*os.File).Sync,
		install: func(root *os.Root, temporary, final string) error {
			return root.Link(temporary, final)
		},
		syncDirectory: func(root *os.Root) error {
			directory, err := root.Open(".")
			if err != nil {
				return err
			}
			defer directory.Close()
			return directory.Sync()
		},
	}
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
	trustedTemp, tempErr := filepath.EvalSymlinks(os.TempDir())
	if tempErr != nil {
		return errors.New("unsafe operator state directory")
	}
	for parent != "." && parent != string(filepath.Separator) {
		info, err := os.Lstat(parent)
		if err != nil || info == nil {
			return errors.New("unsafe operator state directory")
		}
		unsafeWritable := info.Mode().Perm()&0o022 != 0 && !(info.Mode()&os.ModeSticky != 0 && parent == trustedTemp)
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || unsafeWritable {
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
	return owned || runner.composePending[component]
}

func (runner *localOperatorRunner) startSpeech(ctx context.Context) error {
	if !runner.requiredEnvironment(fixedOperation{Component: componentSpeech, Action: actionStart}) {
		return errOperatorMissingConfiguration
	}
	runner.mu.Lock()
	if _, started := runner.children[componentSpeech]; started {
		runner.mu.Unlock()
		return nil
	}
	runner.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}
	if !runner.revalidateSpeechFiles() {
		return errOperatorMissingConfiguration
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
		runner.removeRecordIfMatch(string(componentSpeech)+".pid", strconv.Itoa(child.pid)+" "+child.marker+"\n")
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
	waitCtx, cancel := context.WithTimeout(nonNilContext(ctx), runner.terminateWait)
	defer cancel()
	select {
	case <-child.done:
	case <-waitCtx.Done():
		_ = child.cmd.Process.Kill()
		select {
		case <-child.done:
		case <-time.After(runner.terminateWait):
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
		runner.mu.Lock()
		pending := runner.composePending[component]
		runner.mu.Unlock()
		if pending {
			return errOperatorProcessExited
		}
		var exists bool
		var queryErr error
		before, exists, queryErr = runner.composeID(ctx, component)
		if queryErr != nil {
			return commandFailure(ctx)
		}
		if exists {
			if !runner.composeOwnershipMatches(component, before) {
				return errOperatorProcessExited
			}
		} else {
			before = ""
		}
	}
	err := runner.runFixed(ctx, runner.composeCommand(), verb)
	if component == "" {
		return err
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), runner.identityTimeout)
	defer cancel()
	after, exists, queryErr := runner.discoverComposeIdentity(cleanupCtx, component)
	if queryErr != nil {
		if before == "" {
			runner.mu.Lock()
			runner.composePending[component] = true
			runner.mu.Unlock()
		}
		if err != nil {
			return err
		}
		return errOperatorProcessExited
	}
	if !exists {
		after = ""
	}
	if after == "" && err == nil {
		return errOperatorProcessExited
	}
	if before == "" && after != "" {
		if writeErr := runner.writeOwner(component, after); writeErr != nil {
			runner.mu.Lock()
			runner.composeOwned[component] = after
			runner.composeEphemeral[component] = after
			delete(runner.composePending, component)
			runner.mu.Unlock()
			return errOperatorProcessExited
		}
		runner.mu.Lock()
		runner.composeOwned[component] = after
		delete(runner.composeEphemeral, component)
		delete(runner.composePending, component)
		runner.mu.Unlock()
	}
	return err
}

func (runner *localOperatorRunner) composeOwnershipMatches(component operatorComponent, id string) bool {
	runner.mu.Lock()
	ownedID := runner.composeOwned[component]
	ephemeral := runner.composeEphemeral[component] == id
	runner.mu.Unlock()
	return id != "" && ownedID == id && (ephemeral || runner.ownerMatches(component, id))
}

func (runner *localOperatorRunner) stopCompose(ctx context.Context, component operatorComponent) error {
	runner.mu.Lock()
	id, owned := runner.composeOwned[component]
	pending := runner.composePending[component]
	runner.mu.Unlock()
	if !runner.requiredEnvironment(fixedOperation{Component: component, Action: actionStop}) {
		return errOperatorMissingConfiguration
	}
	var current string
	var exists bool
	var queryErr error
	if pending {
		identityCtx, cancel := context.WithTimeout(nonNilContext(ctx), runner.identityTimeout)
		current, exists, queryErr = runner.discoverComposeIdentity(identityCtx, component)
		cancel()
		if queryErr != nil {
			return errOperatorProcessExited
		}
		if !exists {
			runner.mu.Lock()
			delete(runner.composePending, component)
			runner.mu.Unlock()
			return nil
		}
		id, owned = current, true
		runner.mu.Lock()
		runner.composeOwned[component] = id
		runner.composeEphemeral[component] = id
		delete(runner.composePending, component)
		runner.mu.Unlock()
	} else {
		current, exists, queryErr = runner.composeID(ctx, component)
	}
	runner.mu.Lock()
	ephemeral := runner.composeEphemeral[component] == id
	runner.mu.Unlock()
	if !owned || id == "" || (!runner.ownerMatches(component, id) && !ephemeral) || queryErr != nil || !exists || current != id {
		return errOperatorProcessExited
	}
	if err := runner.runFixed(ctx, runner.composeCommand(), string(component)+"-stop"); err != nil {
		return err
	}
	runner.mu.Lock()
	delete(runner.composeOwned, component)
	delete(runner.composeEphemeral, component)
	delete(runner.composePending, component)
	runner.mu.Unlock()
	runner.removeRecordIfMatch(string(component)+".owner", id+"\n")
	return nil
}

func (runner *localOperatorRunner) queryComposeIdentity(ctx context.Context, component operatorComponent) (string, bool, error) {
	cmd := runner.newCommand(ctx, runner.composeCommand(), string(component)+"-id")
	output, err := cmd.Output()
	if err != nil {
		return "", false, err
	}
	id := strings.TrimSpace(string(output))
	return id, id != "", nil
}

func (runner *localOperatorRunner) composeID(ctx context.Context, component operatorComponent) (string, bool, error) {
	return runner.composeIdentity(ctx, component)
}

func (runner *localOperatorRunner) discoverComposeIdentity(ctx context.Context, component operatorComponent) (string, bool, error) {
	consecutiveAbsences := 0
	for {
		id, exists, err := runner.composeID(ctx, component)
		if err == nil && exists {
			return id, true, nil
		}
		if err == nil {
			consecutiveAbsences++
		} else {
			consecutiveAbsences = 0
		}
		if ctx.Err() != nil {
			if consecutiveAbsences >= 2 {
				return "", false, nil
			}
			return "", false, ctx.Err()
		}
		timer := time.NewTimer(runner.identityRetryWait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			if consecutiveAbsences >= 2 {
				return "", false, nil
			}
			return "", false, ctx.Err()
		case <-timer.C:
		}
	}
}

var operatorRequiredEnvironment = map[operatorComponent][]string{
	componentSpeech:   {"S2S_API_KEY"},
	componentPostgres: {"POSTGRES_PASSWORD"},
	componentGateway:  {"POSTGRES_PASSWORD", "S2S_API_KEY", "LIVE_STATIC_BEARER_TOKEN", "LIVE_ADMIN_BEARER_TOKEN", "LIVE_PRIVACY_HMAC_KEY", "LIVE_RETENTION_HMAC_KEY", "LIVE_RECONNECT_HMAC_KEY", "LIVE_TOOL_ENCRYPTION_KEY", "LIVE_EXPECTED_CONFIG_HASH", "S2S_IMAGE", "S2S_CONFIG_PATH"},
	componentWorker:   {"POSTGRES_PASSWORD", "DEEPSEEK_API_KEY", "LIVE_STATIC_BEARER_TOKEN", "LIVE_ADMIN_BEARER_TOKEN", "LIVE_PRIVACY_HMAC_KEY", "LIVE_RETENTION_HMAC_KEY", "LIVE_TOOL_ENCRYPTION_KEY", "LIVE_EXPECTED_CONFIG_HASH", "S2S_IMAGE", "S2S_CONFIG_PATH"},
}

func (runner *localOperatorRunner) requiredEnvironment(operation fixedOperation) bool {
	if operation.Action == actionStop {
		return true
	}
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

func (runner *localOperatorRunner) revalidateSpeechFiles() bool {
	commandInfo, commandErr := os.Stat(runner.cfg.SpeechCommand)
	profileInfo, profileErr := os.Stat(runner.cfg.SpeechProfile)
	return commandErr == nil && profileErr == nil && os.SameFile(commandInfo, runner.speechFile) && os.SameFile(profileInfo, runner.profileFile) && commandInfo.Mode().Perm()&0o111 != 0 && profileInfo.Mode().IsRegular()
}

func (runner *localOperatorRunner) pidPath(component operatorComponent) string {
	return filepath.Join(runner.cfg.StateDir, string(component)+".pid")
}

func (runner *localOperatorRunner) ownerPath(component operatorComponent) string {
	return filepath.Join(runner.cfg.StateDir, string(component)+".owner")
}

func (runner *localOperatorRunner) writeOwner(component operatorComponent, id string) error {
	return runner.writeRecord(string(component)+".owner", []byte(id+"\n"))
}

func (runner *localOperatorRunner) ownerMatches(component operatorComponent, id string) bool {
	return runner.recordMatches(string(component)+".owner", id+"\n")
}

func (runner *localOperatorRunner) writePID(component operatorComponent, child *ownedChild) error {
	return runner.writeRecord(string(component)+".pid", []byte(strconv.Itoa(child.pid)+" "+child.marker+"\n"))
}

func (runner *localOperatorRunner) pidMatches(component operatorComponent, child *ownedChild) bool {
	return runner.recordMatches(string(component)+".pid", strconv.Itoa(child.pid)+" "+child.marker+"\n")
}

func (runner *localOperatorRunner) writeRecord(final string, data []byte) error {
	if !runner.stateUnchanged() {
		return errors.New("unsafe operator record directory")
	}
	marker, err := newOperatorMarker()
	if err != nil {
		return errors.New("operator record unavailable")
	}
	temporary := "." + final + "." + marker + ".tmp"
	file, err := runner.stateRoot.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return errors.New("operator record unavailable")
	}
	temporaryPresent := true
	defer func() {
		_ = file.Close()
		if temporaryPresent {
			_ = runner.stateRoot.Remove(temporary)
		}
	}()
	if err := file.Chmod(0o600); err != nil {
		return errors.New("operator record unavailable")
	}
	if err := runner.records.write(file, data); err != nil {
		return errors.New("operator record unavailable")
	}
	temporaryInfo, err := file.Stat()
	if err != nil || !temporaryInfo.Mode().IsRegular() || temporaryInfo.Mode().Perm() != 0o600 {
		return errors.New("unsafe operator record")
	}
	if err := runner.records.syncFile(file); err != nil {
		return errors.New("operator record unavailable")
	}
	if err := file.Close(); err != nil {
		return errors.New("operator record unavailable")
	}
	if !runner.stateUnchanged() {
		return errors.New("unsafe operator record directory")
	}
	if err := runner.records.install(runner.stateRoot, temporary, final); err != nil {
		if current, statErr := runner.stateRoot.Lstat(final); statErr == nil && os.SameFile(temporaryInfo, current) {
			_ = runner.stateRoot.Remove(final)
			_ = runner.records.syncDirectory(runner.stateRoot)
		}
		return errors.New("operator record unavailable")
	}
	finalInfo, err := runner.stateRoot.Lstat(final)
	if err != nil || !os.SameFile(temporaryInfo, finalInfo) || !finalInfo.Mode().IsRegular() || finalInfo.Mode().Perm() != 0o600 {
		return errors.New("unsafe operator record")
	}
	if err := runner.records.syncDirectory(runner.stateRoot); err != nil {
		if current, statErr := runner.stateRoot.Lstat(final); statErr == nil && os.SameFile(temporaryInfo, current) {
			_ = runner.stateRoot.Remove(final)
			_ = runner.records.syncDirectory(runner.stateRoot)
		}
		return errors.New("operator record unavailable")
	}
	if err := runner.stateRoot.Remove(temporary); err != nil {
		if current, statErr := runner.stateRoot.Lstat(final); statErr == nil && os.SameFile(temporaryInfo, current) {
			_ = runner.stateRoot.Remove(final)
		}
		return errors.New("operator record unavailable")
	}
	temporaryPresent = false
	if err := runner.records.syncDirectory(runner.stateRoot); err != nil {
		if current, statErr := runner.stateRoot.Lstat(final); statErr == nil && os.SameFile(temporaryInfo, current) {
			_ = runner.stateRoot.Remove(final)
			_ = runner.records.syncDirectory(runner.stateRoot)
		}
		return errors.New("operator record unavailable")
	}
	return nil
}

func (runner *localOperatorRunner) recordMatches(name, expected string) bool {
	if !runner.stateUnchanged() {
		return false
	}
	file, err := runner.stateRoot.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return false
	}
	defer file.Close()
	before, err := file.Stat()
	if err != nil || !before.Mode().IsRegular() || before.Mode().Perm() != 0o600 {
		return false
	}
	data, err := io.ReadAll(io.LimitReader(file, int64(len(expected)+1)))
	if err != nil || string(data) != expected {
		return false
	}
	after, err := file.Stat()
	return err == nil && os.SameFile(before, after) && runner.stateUnchanged()
}

func (runner *localOperatorRunner) removeRecordIfMatch(name, expected string) {
	if runner.recordMatches(name, expected) {
		_ = runner.stateRoot.Remove(name)
	}
}

func (runner *localOperatorRunner) stateUnchanged() bool {
	current, err := os.Lstat(runner.cfg.StateDir)
	return err == nil && current.IsDir() && current.Mode()&os.ModeSymlink == 0 && current.Mode().Perm()&0o077 == 0 && os.SameFile(runner.stateFile, current)
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
