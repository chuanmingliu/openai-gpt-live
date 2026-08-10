package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/releasegate"
)

type repeatedFlag []string

var commandForRecord = exec.Command

func (values *repeatedFlag) String() string { return strings.Join(*values, ",") }
func (values *repeatedFlag) Set(value string) error {
	if value == "" {
		return errors.New("flag value must not be empty")
	}
	*values = append(*values, value)
	return nil
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "release gate:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: live-release-gate record|collect|snapshot-unsigned|finalize-archive|verify|verify-archive|statement-hash")
	}
	switch args[0] {
	case "record":
		return record(args[1:])
	case "collect":
		return collect(args[1:])
	case "snapshot-unsigned":
		return snapshotUnsigned(args[1:])
	case "finalize-archive":
		return finalizeArchive(args[1:])
	case "verify":
		return verify(args[1:])
	case "verify-archive":
		return verifyArchive(args[1:])
	case "statement-hash":
		return statementHash(args[1:])
	default:
		return fmt.Errorf("unknown subcommand %q", args[0])
	}
}

func record(args []string) error {
	separator := -1
	for index, argument := range args {
		if argument == "--" {
			separator = index
			break
		}
	}
	if separator < 0 || separator == len(args)-1 {
		return errors.New("record requires a command after --")
	}
	flags := flag.NewFlagSet("record", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	gateID := flags.String("gate-id", "", "required gate ID")
	producer := flags.String("producer", "", "versioned expected producer identity")
	artifactRoot := flags.String("artifact-root", "", "absolute evidence bundle root")
	artifactPath := flags.String("artifact", "", "safe bundle-relative artifact path")
	evidenceOut := flags.String("evidence-out", "", "absolute gate evidence JSON path")
	if err := flags.Parse(args[:separator]); err != nil {
		return err
	}
	if flags.NArg() != 0 || *gateID == "" || *producer == "" || *artifactRoot == "" || *artifactPath == "" || *evidenceOut == "" {
		return errors.New("record requires --gate-id, --producer, --artifact-root, --artifact, --evidence-out, and no positional flag arguments")
	}
	if releasegate.ExpectedProducers[*gateID] != *producer {
		return errors.New("producer does not match the versioned gate contract")
	}
	root, err := filepath.Abs(*artifactRoot)
	if err != nil || root != *artifactRoot {
		return errors.New("artifact-root must be an absolute normalized path")
	}
	if filepath.IsAbs(*artifactPath) || filepath.ToSlash(filepath.Clean(filepath.FromSlash(*artifactPath))) != *artifactPath || *artifactPath == "." || strings.HasPrefix(*artifactPath, "../") || strings.Contains(*artifactPath, `\`) {
		return errors.New("artifact must be a safe normalized relative path")
	}
	rootHandle, err := os.OpenRoot(root)
	if err != nil {
		return fmt.Errorf("open artifact root: %w", err)
	}
	defer rootHandle.Close()
	artifactDirectory := filepath.ToSlash(filepath.Dir(filepath.FromSlash(*artifactPath)))
	if err := ensureRootDirectory(rootHandle, artifactDirectory); err != nil {
		return err
	}
	evidenceRelative, err := relativeInRoot(root, *evidenceOut)
	if err != nil {
		return err
	}
	commandArgs := append([]string(nil), args[separator+1:]...)
	for _, argument := range commandArgs {
		if err := releasegate.ScanContent([]byte(argument)); err != nil {
			return fmt.Errorf("command argument contains prohibited evidence content: %w", err)
		}
	}
	startedAt := time.Now().UTC()
	command := commandForRecord(commandArgs[0], commandArgs[1:]...)
	capture := &boundedScanner{maximum: releasegate.MaximumArtifactBytes}
	command.Stdout = capture
	command.Stderr = capture
	if err := command.Start(); err != nil {
		capture.setError(err)
	} else {
		capture.setKill(func() { _ = command.Process.Kill() })
	}
	runErr := command.Wait()
	finishedAt := time.Now().UTC()
	exitCode := 0
	if runErr != nil {
		var exitError *exec.ExitError
		if errors.As(runErr, &exitError) {
			exitCode = exitError.ExitCode()
		} else {
			exitCode = 127
		}
	}
	status := "passed"
	if runErr != nil || capture.errValue() != nil {
		status = "failed"
	}
	summary := releasegate.ArtifactSummary{
		SchemaVersion: releasegate.SummarySchemaVersion, GateID: *gateID,
		Producer: *producer, Status: status, OutputBytes: capture.bytesWritten(),
	}
	document, err := releasegate.CanonicalJSON(summary)
	if err != nil {
		return err
	}
	artifact, err := rootHandle.OpenFile(*artifactPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return fmt.Errorf("exclusively create artifact: %w", err)
	}
	if _, err := artifact.Write(document); err != nil {
		artifact.Close()
		return fmt.Errorf("write artifact: %w", err)
	}
	if err := artifact.Sync(); err != nil {
		artifact.Close()
		return fmt.Errorf("sync artifact: %w", err)
	}
	if err := artifact.Close(); err != nil {
		return fmt.Errorf("close artifact: %w", err)
	}
	digest := sha256.Sum256(document)
	gate := releasegate.GateEvidence{
		GateID: *gateID, Producer: *producer, Command: commandArgs, StartedAt: startedAt, FinishedAt: finishedAt,
		ExitCode: exitCode, ArtifactPath: *artifactPath, ArtifactSHA256: hex.EncodeToString(digest[:]),
	}
	gateDocument, err := releasegate.CanonicalJSON(gate)
	if err != nil {
		return err
	}
	if err := atomicWriteRoot(rootHandle, evidenceRelative, gateDocument, false); err != nil {
		return err
	}
	if captureErr := capture.errValue(); captureErr != nil {
		return fmt.Errorf("gate %q output limit/content scan failed: %w", *gateID, captureErr)
	}
	if runErr != nil {
		return fmt.Errorf("gate %q command failed with exit code %d", *gateID, exitCode)
	}
	return nil
}

type boundedScanner struct {
	mu      sync.Mutex
	maximum int64
	written int64
	tail    []byte
	err     error
	kill    func()
}

func (writer *boundedScanner) Write(chunk []byte) (int, error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.err != nil {
		return 0, writer.err
	}
	writer.written += int64(len(chunk))
	if writer.written > writer.maximum {
		writer.err = fmt.Errorf("producer output exceeds %d-byte output limit", writer.maximum)
		if writer.kill != nil {
			writer.kill()
		}
		return 0, writer.err
	}
	combined := append(append([]byte(nil), writer.tail...), chunk...)
	if err := releasegate.ScanContent(combined); err != nil {
		writer.err = err
		if writer.kill != nil {
			writer.kill()
		}
		return 0, err
	}
	const tailBytes = 64
	if len(combined) > tailBytes {
		writer.tail = append(writer.tail[:0], combined[len(combined)-tailBytes:]...)
	} else {
		writer.tail = append(writer.tail[:0], combined...)
	}
	return len(chunk), nil
}

func (writer *boundedScanner) setKill(kill func()) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	writer.kill = kill
	if writer.err != nil {
		kill()
	}
}

func (writer *boundedScanner) setError(err error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	writer.err = err
}

func (writer *boundedScanner) errValue() error {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.err
}

func (writer *boundedScanner) bytesWritten() int64 {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.written
}

func collect(args []string) error {
	flags := flag.NewFlagSet("collect", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	commit := flags.String("commit", "", "exact Git commit")
	configHash := flags.String("config-hash", "", "immutable non-secret config hash")
	evidenceOut := flags.String("evidence-out", "", "exact evidence.json output path")
	preserveGeneratedAt := flags.Bool("preserve-generated-at", false, "preserve an existing bundle timestamp while attaching an attestation")
	var gatePaths, attestationPaths, releaseInputSpecs, artifactSpecs repeatedFlag
	flags.Var(&gatePaths, "gate-evidence", "canonical gate evidence JSON (repeat in required order)")
	flags.Var(&attestationPaths, "attestation", "signed attestation JSON (repeat)")
	flags.Var(&releaseInputSpecs, "release-input", "reviewed non-secret release input as name=value (repeat sorted)")
	flags.Var(&artifactSpecs, "artifact", "content-free bundle artifact as name=relative/path (repeat sorted)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *commit == "" || *configHash == "" || *evidenceOut == "" {
		return errors.New("collect requires --commit, --config-hash, --evidence-out, and no positional arguments")
	}
	if len(gatePaths) != len(releasegate.RequiredGateIDs) {
		return fmt.Errorf("collect requires exactly %d --gate-evidence values", len(releasegate.RequiredGateIDs))
	}
	evidence := releasegate.Evidence{
		SchemaVersion: releasegate.SchemaVersion,
		Commit:        *commit,
		ConfigHash:    *configHash,
		GeneratedAt:   time.Now().UTC(),
	}
	releaseInputs, err := parseReleaseInputs(releaseInputSpecs)
	if err != nil {
		return err
	}
	evidence.ReleaseInputs = releaseInputs
	artifactRootPath := filepath.Dir(*evidenceOut)
	artifactRoot, err := os.OpenRoot(artifactRootPath)
	if err != nil {
		return fmt.Errorf("open collection artifact root: %w", err)
	}
	defer artifactRoot.Close()
	parsedArtifacts, err := parseArtifactSpecifications(artifactSpecs)
	if err != nil {
		return err
	}
	for _, artifact := range parsedArtifacts {
		name, relative := artifact.name, artifact.relative
		file, err := artifactRoot.OpenFile(relative, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
		if err != nil {
			return fmt.Errorf("open collection artifact %q: %w", name, err)
		}
		info, err := file.Stat()
		if err != nil || !info.Mode().IsRegular() || info.Size() > releasegate.MaximumArtifactBytes {
			file.Close()
			return fmt.Errorf("collection artifact %q has invalid type or size", name)
		}
		document, err := io.ReadAll(io.LimitReader(file, releasegate.MaximumArtifactBytes+1))
		file.Close()
		if err != nil {
			return err
		}
		if err := releasegate.ValidateArtifact(name, document); err != nil {
			return fmt.Errorf("collection artifact %q: %w", name, err)
		}
		digest := sha256.Sum256(document)
		evidence.Artifacts = append(evidence.Artifacts, releasegate.ArtifactEvidence{Name: name, Path: relative, SHA256: hex.EncodeToString(digest[:])})
	}
	for index, path := range gatePaths {
		relative, err := relativeInRoot(artifactRootPath, path)
		if err != nil {
			return fmt.Errorf("gate evidence %d is outside bundle: %w", index+1, err)
		}
		document, err := readRootFile(artifactRoot, relative, releasegate.MaximumArtifactBytes)
		if err != nil {
			return fmt.Errorf("read gate evidence %d: %w", index+1, err)
		}
		gate, err := releasegate.DecodeGateEvidence(document)
		if err != nil {
			return fmt.Errorf("gate evidence %d: %w", index+1, err)
		}
		if gate.GateID != releasegate.RequiredGateIDs[index] {
			return fmt.Errorf("gate evidence %d must be %q, found %q", index+1, releasegate.RequiredGateIDs[index], gate.GateID)
		}
		evidence.Gates = append(evidence.Gates, gate)
	}
	for index, path := range attestationPaths {
		document, err := readBoundedNoFollow(path, releasegate.MaximumAttestationBytes)
		if err != nil {
			return fmt.Errorf("read attestation %d: %w", index+1, err)
		}
		attestation, err := releasegate.DecodeAttestation(document)
		if err != nil {
			return fmt.Errorf("attestation %d: %w", index+1, err)
		}
		evidence.Attestations = append(evidence.Attestations, attestation)
	}
	if *preserveGeneratedAt {
		existing, err := readExistingEvidenceForPreservation(artifactRoot, filepath.Base(*evidenceOut))
		if err != nil {
			return err
		}
		if existing.SchemaVersion != evidence.SchemaVersion || existing.Commit != evidence.Commit || existing.ConfigHash != evidence.ConfigHash || !reflect.DeepEqual(existing.ReleaseInputs, evidence.ReleaseInputs) || !reflect.DeepEqual(existing.Gates, evidence.Gates) || !reflect.DeepEqual(existing.Artifacts, evidence.Artifacts) {
			return errors.New("existing evidence bindings or gate records changed; generated_at cannot be preserved")
		}
		evidence.GeneratedAt = existing.GeneratedAt
	}
	document, err := releasegate.CanonicalJSON(evidence)
	if err != nil {
		return err
	}
	return atomicWriteRoot(artifactRoot, filepath.Base(*evidenceOut), document, true)
}

type artifactSpecification struct {
	name     string
	relative string
}

func parseArtifactSpecifications(specs []string) ([]artifactSpecification, error) {
	parsed := make([]artifactSpecification, 0, len(specs))
	previous := ""
	for _, spec := range specs {
		name, relative, ok := strings.Cut(spec, "=")
		if !ok || name == "" || relative == "" {
			return nil, errors.New("artifact must use name=relative/path")
		}
		if previous != "" && name <= previous {
			return nil, errors.New("artifact names must be unique and strictly C-sorted")
		}
		parsed = append(parsed, artifactSpecification{name: name, relative: relative})
		previous = name
	}
	return parsed, nil
}

func readExistingEvidenceForPreservation(root *os.Root, relative string) (releasegate.Evidence, error) {
	document, err := readRootFile(root, relative, releasegate.MaximumEvidenceBytes)
	if err != nil {
		return releasegate.Evidence{}, fmt.Errorf("read existing evidence for timestamp preservation: %w", err)
	}
	evidence, err := releasegate.DecodeEvidence(document)
	if err != nil {
		return releasegate.Evidence{}, fmt.Errorf("decode existing evidence for timestamp preservation: %w", err)
	}
	return evidence, nil
}

func readRootFile(root *os.Root, relative string, maximum int64) ([]byte, error) {
	file, err := root.OpenFile(relative, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > maximum {
		return nil, errors.New("root-scoped file has invalid type or size")
	}
	return io.ReadAll(io.LimitReader(file, maximum+1))
}

func verify(args []string) error {
	flags := flag.NewFlagSet("verify", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	evidencePath := flags.String("evidence", "", "canonical evidence.json path")
	archiveOut := flags.String("archive-out", "", "exclusive immutable verified bundle archive output")
	commit := flags.String("commit", "", "expected Git commit")
	configHash := flags.String("config-hash", "", "expected immutable config hash")
	var keySpecs repeatedFlag
	var releaseInputSpecs repeatedFlag
	flags.Var(&keySpecs, "attestation-key", "trusted signer and Ed25519 public key file as signer=path")
	flags.Var(&releaseInputSpecs, "release-input", "exact reviewed release input as name=value (repeat sorted)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *evidencePath == "" || *commit == "" || *configHash == "" {
		return errors.New("verify requires --evidence, --commit, --config-hash, and no positional arguments")
	}
	absoluteEvidence, err := filepath.Abs(*evidencePath)
	if err != nil || absoluteEvidence != filepath.Clean(*evidencePath) || filepath.Base(absoluteEvidence) != "evidence.json" {
		return errors.New("verify --evidence must be an absolute normalized evidence.json path")
	}
	expectedInputs, err := parseReleaseInputs(releaseInputSpecs)
	if err != nil {
		return err
	}
	keys, err := loadKeys(keySpecs)
	if err != nil {
		return err
	}
	options := releasegate.VerifyOptions{
		Now: time.Now().UTC(), Commit: *commit, ConfigHash: *configHash,
		ArtifactRoot: filepath.Dir(absoluteEvidence), AttestationKeys: keys,
	}
	var evidence releasegate.Evidence
	if *archiveOut == "" {
		evidence, err = releasegate.VerifyBundle(options)
	} else {
		absoluteArchive, pathErr := filepath.Abs(*archiveOut)
		if pathErr != nil || absoluteArchive != filepath.Clean(*archiveOut) {
			return errors.New("verify --archive-out must be an absolute normalized path")
		}
		relative, pathErr := filepath.Rel(options.ArtifactRoot, absoluteArchive)
		if pathErr != nil || relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))) {
			return errors.New("verified archive output must be outside the mutable source bundle")
		}
		var archive bytes.Buffer
		evidence, err = releasegate.WriteVerifiedArchive(&archive, options)
		if err == nil && int64(archive.Len()) > releasegate.MaximumArchiveBytes {
			err = errors.New("verified archive exceeds its bounded output limit")
		}
		if err == nil && !reflect.DeepEqual(evidence.ReleaseInputs, expectedInputs) {
			err = errors.New("reviewed release inputs do not exactly match the source collection run")
		}
		if err == nil {
			err = writeExclusiveArchive(absoluteArchive, archive.Bytes())
		}
	}
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(evidence.ReleaseInputs, expectedInputs) {
		return errors.New("reviewed release inputs do not exactly match the source collection run")
	}
	return nil
}

func verifyArchive(args []string) error {
	flags := flag.NewFlagSet("verify-archive", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	archivePath := flags.String("archive", "", "canonical verified bundle archive")
	commit := flags.String("commit", "", "expected Git commit")
	configHash := flags.String("config-hash", "", "expected immutable config hash")
	var keySpecs, releaseInputSpecs repeatedFlag
	flags.Var(&keySpecs, "attestation-key", "trusted signer and Ed25519 public key file as signer=path")
	flags.Var(&releaseInputSpecs, "release-input", "exact reviewed release input as name=value (repeat sorted)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *archivePath == "" || *commit == "" || *configHash == "" {
		return errors.New("verify-archive requires --archive, --commit, --config-hash, and no positional arguments")
	}
	expectedInputs, err := parseReleaseInputs(releaseInputSpecs)
	if err != nil {
		return err
	}
	keys, err := loadKeys(keySpecs)
	if err != nil {
		return err
	}
	document, err := readBoundedNoFollow(*archivePath, releasegate.MaximumArchiveBytes)
	if err != nil {
		return fmt.Errorf("read immutable archive: %w", err)
	}
	evidence, err := releasegate.VerifyArchive(document, releasegate.VerifyOptions{
		Now: time.Now().UTC(), Commit: *commit, ConfigHash: *configHash, AttestationKeys: keys,
	})
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(evidence.ReleaseInputs, expectedInputs) {
		return errors.New("reviewed release inputs do not exactly match the archived source collection run")
	}
	return nil
}

func writeExclusiveArchive(path string, document []byte) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return fmt.Errorf("exclusively create verified archive: %w", err)
	}
	if _, err := file.Write(document); err != nil {
		file.Close()
		return fmt.Errorf("write verified archive: %w", err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("sync verified archive: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close verified archive: %w", err)
	}
	return nil
}

func snapshotUnsigned(args []string) error {
	flags := flag.NewFlagSet("snapshot-unsigned", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	evidencePath := flags.String("evidence", "", "canonical unsigned evidence.json path")
	archiveOut := flags.String("archive-out", "", "exclusive canonical unsigned archive output")
	commit := flags.String("commit", "", "expected Git commit")
	configHash := flags.String("config-hash", "", "expected release config hash")
	signer := flags.String("signer", "", "trusted attestation signer identity")
	signedAtRaw := flags.String("signed-at", "", "exact RFC3339 UTC signing time")
	var releaseInputSpecs repeatedFlag
	flags.Var(&releaseInputSpecs, "release-input", "exact reviewed release input as name=value (repeat sorted)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *evidencePath == "" || *archiveOut == "" || *commit == "" || *configHash == "" || *signer == "" || *signedAtRaw == "" {
		return errors.New("snapshot-unsigned requires --evidence, --archive-out, --commit, --config-hash, --signer, --signed-at, and no positional arguments")
	}
	signedAt, err := time.Parse(time.RFC3339Nano, *signedAtRaw)
	if err != nil || signedAt.Location() != time.UTC {
		return errors.New("snapshot-unsigned --signed-at must be an RFC3339 UTC timestamp")
	}
	absoluteEvidence, err := filepath.Abs(*evidencePath)
	if err != nil || absoluteEvidence != filepath.Clean(*evidencePath) || filepath.Base(absoluteEvidence) != "evidence.json" {
		return errors.New("snapshot-unsigned --evidence must be an absolute normalized evidence.json path")
	}
	absoluteArchive, err := filepath.Abs(*archiveOut)
	if err != nil || absoluteArchive != filepath.Clean(*archiveOut) {
		return errors.New("snapshot-unsigned --archive-out must be an absolute normalized path")
	}
	relative, err := filepath.Rel(filepath.Dir(absoluteEvidence), absoluteArchive)
	if err != nil || relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))) {
		return errors.New("unsigned archive output must be outside the mutable source bundle")
	}
	expectedInputs, err := parseReleaseInputs(releaseInputSpecs)
	if err != nil {
		return err
	}
	var archive bytes.Buffer
	evidence, err := releasegate.WriteVerifiedUnsignedArchive(&archive, releasegate.VerifyOptions{
		Now: time.Now().UTC(), Commit: *commit, ConfigHash: *configHash, ArtifactRoot: filepath.Dir(absoluteEvidence),
	})
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(evidence.ReleaseInputs, expectedInputs) {
		return errors.New("reviewed release inputs do not exactly match the unsigned collection run")
	}
	statement, err := releasegate.AttestationEnvelopeSHA256(evidence, releasegate.PrivacyAttestationGateID, *signer, signedAt)
	if err != nil {
		return err
	}
	if err := writeExclusiveArchive(absoluteArchive, archive.Bytes()); err != nil {
		return err
	}
	fmt.Println(statement)
	return nil
}

func finalizeArchive(args []string) error {
	flags := flag.NewFlagSet("finalize-archive", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	unsignedPath := flags.String("unsigned-archive", "", "canonical verified unsigned archive")
	attestationPath := flags.String("attestation", "", "authorized signed attestation JSON")
	archiveOut := flags.String("archive-out", "", "exclusive canonical signed archive output")
	commit := flags.String("commit", "", "expected Git commit")
	configHash := flags.String("config-hash", "", "expected release config hash")
	var keySpecs, releaseInputSpecs repeatedFlag
	flags.Var(&keySpecs, "attestation-key", "trusted signer and Ed25519 public key file as signer=path")
	flags.Var(&releaseInputSpecs, "release-input", "exact reviewed release input as name=value (repeat sorted)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *unsignedPath == "" || *attestationPath == "" || *archiveOut == "" || *commit == "" || *configHash == "" {
		return errors.New("finalize-archive requires --unsigned-archive, --attestation, --archive-out, --commit, --config-hash, and no positional arguments")
	}
	unsignedDocument, err := readBoundedNoFollow(*unsignedPath, releasegate.MaximumArchiveBytes)
	if err != nil {
		return fmt.Errorf("read unsigned immutable archive: %w", err)
	}
	attestationDocument, err := readBoundedNoFollow(*attestationPath, releasegate.MaximumAttestationBytes)
	if err != nil {
		return fmt.Errorf("read authorized attestation: %w", err)
	}
	attestation, err := releasegate.DecodeAttestation(attestationDocument)
	if err != nil {
		return err
	}
	keys, err := loadKeys(keySpecs)
	if err != nil {
		return err
	}
	expectedInputs, err := parseReleaseInputs(releaseInputSpecs)
	if err != nil {
		return err
	}
	absoluteArchive, err := filepath.Abs(*archiveOut)
	if err != nil || absoluteArchive != filepath.Clean(*archiveOut) || absoluteArchive == filepath.Clean(*unsignedPath) {
		return errors.New("finalize-archive --archive-out must be a distinct absolute normalized path")
	}
	var archive bytes.Buffer
	evidence, err := releasegate.FinalizeVerifiedArchive(&archive, unsignedDocument, attestation, releasegate.VerifyOptions{
		Now: time.Now().UTC(), Commit: *commit, ConfigHash: *configHash, AttestationKeys: keys,
	})
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(evidence.ReleaseInputs, expectedInputs) {
		return errors.New("reviewed release inputs do not exactly match the unsigned archive")
	}
	return writeExclusiveArchive(absoluteArchive, archive.Bytes())
}

func statementHash(args []string) error {
	flags := flag.NewFlagSet("statement-hash", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	evidencePath := flags.String("evidence", "", "evidence.json path")
	signer := flags.String("signer", "", "trusted attestation signer identity")
	signedAtRaw := flags.String("signed-at", "", "exact RFC3339 UTC signing time")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *evidencePath == "" || *signer == "" || *signedAtRaw == "" {
		return errors.New("statement-hash requires --evidence, --signer, --signed-at, and no positional arguments")
	}
	signedAt, err := time.Parse(time.RFC3339Nano, *signedAtRaw)
	if err != nil || signedAt.Location() != time.UTC {
		return errors.New("statement-hash --signed-at must be an RFC3339 UTC timestamp")
	}
	document, err := readBoundedNoFollow(*evidencePath, releasegate.MaximumEvidenceBytes)
	if err != nil {
		return fmt.Errorf("read evidence: %w", err)
	}
	evidence, err := releasegate.DecodeEvidence(document)
	if err != nil {
		return err
	}
	canonical, err := releasegate.CanonicalJSON(evidence)
	if err != nil {
		return err
	}
	if string(document) != string(canonical) {
		return errors.New("evidence.json is not canonical JSON")
	}
	if err := releasegate.VerifyUnsigned(evidence, releasegate.VerifyOptions{
		Now: time.Now().UTC(), Commit: evidence.Commit, ConfigHash: evidence.ConfigHash,
		ArtifactRoot: filepath.Dir(*evidencePath),
	}); err != nil {
		return fmt.Errorf("bundle is not signable: %w", err)
	}
	hash, err := releasegate.AttestationEnvelopeSHA256(evidence, releasegate.PrivacyAttestationGateID, *signer, signedAt)
	if err != nil {
		return err
	}
	fmt.Println(hash)
	return nil
}

func parseReleaseInputs(specs []string) ([]releasegate.ReleaseInput, error) {
	if len(specs) == 0 {
		return nil, errors.New("at least one --release-input is required")
	}
	inputs := make([]releasegate.ReleaseInput, 0, len(specs))
	for _, spec := range specs {
		name, value, ok := strings.Cut(spec, "=")
		if !ok || name == "" || value == "" {
			return nil, errors.New("release input must use nonempty name=value")
		}
		inputs = append(inputs, releasegate.ReleaseInput{Name: name, Value: value})
	}
	return inputs, nil
}

func loadKeys(specs []string) (map[string]ed25519.PublicKey, error) {
	keys := make(map[string]ed25519.PublicKey, len(specs))
	for _, spec := range specs {
		signer, path, ok := strings.Cut(spec, "=")
		if !ok || signer == "" || path == "" {
			return nil, errors.New("attestation key must use signer=path")
		}
		if _, duplicate := keys[signer]; duplicate {
			return nil, fmt.Errorf("attestation signer %q is duplicated", signer)
		}
		document, err := readBoundedNoFollow(path, releasegate.MaximumKeyBytes)
		if err != nil {
			return nil, fmt.Errorf("read attestation key for %q: %w", signer, err)
		}
		decoded, err := base64.StdEncoding.Strict().DecodeString(strings.TrimSpace(string(document)))
		if err != nil || len(decoded) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("attestation key for %q is not a base64 Ed25519 public key", signer)
		}
		keys[signer] = ed25519.PublicKey(decoded)
	}
	return keys, nil
}

func readBoundedNoFollow(path string, maximum int64) ([]byte, error) {
	return releasegate.ReadBoundedNoFollow(path, maximum)
}

func relativeInRoot(root, absolute string) (string, error) {
	if !filepath.IsAbs(absolute) {
		return "", errors.New("evidence-out must be absolute")
	}
	relative, err := filepath.Rel(root, absolute)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", errors.New("evidence-out must remain inside artifact-root")
	}
	relative = filepath.ToSlash(relative)
	if filepath.ToSlash(filepath.Clean(filepath.FromSlash(relative))) != relative {
		return "", errors.New("evidence-out must be normalized")
	}
	return relative, nil
}

func ensureRootDirectory(root *os.Root, relative string) error {
	if relative == "." {
		return nil
	}
	parts := strings.Split(relative, "/")
	for index := range parts {
		component := strings.Join(parts[:index+1], "/")
		info, err := root.Lstat(component)
		if errors.Is(err, os.ErrNotExist) {
			if err := root.Mkdir(component, 0o700); err != nil {
				return fmt.Errorf("create root-scoped directory: %w", err)
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("lstat root-scoped directory: %w", err)
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("root-scoped directory path contains a non-directory or symbolic link")
		}
	}
	return nil
}

func atomicWriteRoot(root *os.Root, relative string, document []byte, replace bool) error {
	directory := filepath.ToSlash(filepath.Dir(filepath.FromSlash(relative)))
	if err := ensureRootDirectory(root, directory); err != nil {
		return err
	}
	if !replace {
		if _, err := root.Lstat(relative); err == nil {
			return errors.New("evidence output already exists")
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("lstat evidence output: %w", err)
		}
	} else if info, err := root.Lstat(relative); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return errors.New("evidence output is a symbolic link")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("lstat evidence output: %w", err)
	}
	random := make([]byte, 12)
	if _, err := rand.Read(random); err != nil {
		return fmt.Errorf("generate evidence temporary name: %w", err)
	}
	temporary := filepath.ToSlash(filepath.Join(filepath.FromSlash(directory), ".evidence-"+hex.EncodeToString(random)+".json"))
	file, err := root.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return fmt.Errorf("create root-scoped temporary evidence: %w", err)
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = root.Remove(temporary)
		}
	}()
	if _, err := file.Write(document); err != nil {
		file.Close()
		return fmt.Errorf("write root-scoped evidence: %w", err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("sync root-scoped evidence: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close root-scoped evidence: %w", err)
	}
	if err := root.Rename(temporary, relative); err != nil {
		return fmt.Errorf("publish root-scoped evidence: %w", err)
	}
	cleanup = false
	return nil
}
