package releasegate

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"net/url"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	maximumEvidenceAge = 24 * time.Hour
	maximumFutureSkew  = 5 * time.Minute
)

var (
	commitPattern         = regexp.MustCompile(`^[0-9a-f]{40}$`)
	configPattern         = regexp.MustCompile(`^v1:[0-9a-f]{64}$`)
	digestPattern         = regexp.MustCompile(`^[0-9a-f]{64}$`)
	imageDigestPattern    = regexp.MustCompile(`^ghcr\.io/chuanmingliu/openai-gpt-live/(gateway|migrate|worker)@sha256:[0-9a-f]{64}$`)
	inputNamePattern      = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
	regionPattern         = regexp.MustCompile(`^[a-z]{2}(?:-gov)?-[a-z]+-[1-9][0-9]?$`)
	containerIDPattern    = regexp.MustCompile(`^[0-9a-f]{64}$`)
	eventIDPattern        = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	dnsNamePattern        = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?$`)
	kubernetesNamePattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?$`)
)

const PinnedPostgresImage = "postgres:16-alpine@sha256:57c72fd2a128e416c7fcc499958864df5301e940bca0a56f58fddf30ffc07777"

var RequiredFormalArtifactNames = []string{
	"formal_topology_identity",
	"formal_manifest",
	"formal_unit_race",
	"formal_capacity_slo_10m",
	"formal_barge_20",
	"formal_chaos_postgresql_disconnect",
	"formal_chaos_deepseek_429",
	"formal_chaos_slow_client_backpressure",
	"formal_chaos_stalled_voice_provider",
	"formal_chaos_killed_worker_lease_recovery",
	"formal_chaos_lost_notification_poll_repair",
	"formal_soak_2h",
}

var prohibitedArtifactContent = [][]byte{
	[]byte("audio_base64"), []byte("raw_audio"), []byte("reasoning_content"),
	[]byte("tool_arguments"), []byte("tool_results"), []byte(`"transcript":`), []byte(`"pcm":`),
	[]byte(`"api_key"`), []byte(`"credential"`), []byte(`"password"`), []byte(`"authorization"`),
	[]byte("sk_live_"), []byte("-----begin private key-----"),
}

type VerifyOptions struct {
	Now             time.Time
	Commit          string
	ConfigHash      string
	ArtifactRoot    string
	AttestationKeys map[string]ed25519.PublicKey
}

func Verify(evidence Evidence, options VerifyOptions) error {
	options = normalizeVerifyOptions(options)
	if err := verifyBundle(evidence, options); err != nil {
		return err
	}
	return verifyAttestations(evidence, options)
}

// VerifyUnsigned proves a bundle is complete, fresh, content-safe, and
// zero-exit before a human is allowed to sign its whole-bundle digest.
func VerifyUnsigned(evidence Evidence, options VerifyOptions) error {
	options = normalizeVerifyOptions(options)
	if len(evidence.Attestations) != 0 {
		return errors.New("unsigned bundle must not contain attestations")
	}
	return verifyBundle(evidence, options)
}

func normalizeVerifyOptions(options VerifyOptions) VerifyOptions {
	if options.Now.IsZero() {
		options.Now = time.Now().UTC()
	}
	return options
}

func verifyBundle(evidence Evidence, options VerifyOptions) error {
	if options.ArtifactRoot == "" {
		return errors.New("artifact root is required")
	}
	snapshot, err := captureBundleSnapshot(options.ArtifactRoot)
	if err != nil {
		return err
	}
	return verifyBundleSnapshot(evidence, options, snapshot)
}

func verifyBundleSnapshot(evidence Evidence, options VerifyOptions, snapshot bundleSnapshot) error {
	if evidence.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported evidence schema %q", evidence.SchemaVersion)
	}
	if !commitPattern.MatchString(evidence.Commit) || !commitPattern.MatchString(options.Commit) {
		return errors.New("commit binding is malformed")
	}
	if evidence.Commit != options.Commit {
		return errors.New("evidence commit does not match requested commit")
	}
	if !configPattern.MatchString(evidence.ConfigHash) || !configPattern.MatchString(options.ConfigHash) {
		return errors.New("config hash binding is malformed")
	}
	if evidence.ConfigHash != options.ConfigHash {
		return errors.New("evidence config hash does not match requested config hash")
	}
	if evidence.GeneratedAt.IsZero() || evidence.GeneratedAt.Location() != time.UTC {
		return errors.New("evidence generated_at must be a UTC timestamp")
	}
	if evidence.GeneratedAt.Before(options.Now.Add(-maximumEvidenceAge)) {
		return errors.New("evidence is older than 24 hours")
	}
	if evidence.GeneratedAt.After(options.Now.Add(maximumFutureSkew)) {
		return errors.New("evidence generated_at is too far in the future")
	}
	if err := verifyReleaseInputs(evidence.ReleaseInputs); err != nil {
		return err
	}
	if len(evidence.Gates) != len(RequiredGateIDs) {
		return fmt.Errorf("expected %d gates, found %d", len(RequiredGateIDs), len(evidence.Gates))
	}
	if err := verifyBundleArtifacts(evidence, snapshot); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(evidence.Gates))
	for index, gate := range evidence.Gates {
		if gate.GateID != RequiredGateIDs[index] {
			return fmt.Errorf("gate %d must be %q, found %q", index+1, RequiredGateIDs[index], gate.GateID)
		}
		if _, duplicate := seen[gate.GateID]; duplicate {
			return fmt.Errorf("gate %q is duplicated", gate.GateID)
		}
		seen[gate.GateID] = struct{}{}
		if err := verifyGate(gate, evidence.GeneratedAt, options.Now, snapshot); err != nil {
			return fmt.Errorf("gate %q: %w", gate.GateID, err)
		}
		if err := verifyGateRecord(gate, snapshot); err != nil {
			return fmt.Errorf("gate record %q: %w", gate.GateID, err)
		}
		if index > 0 && gate.StartedAt.Before(evidence.Gates[index-1].FinishedAt) {
			return fmt.Errorf("gate %q overlaps or precedes gate %q", gate.GateID, evidence.Gates[index-1].GateID)
		}
	}
	canonicalEvidence, err := CanonicalJSON(evidence)
	if err != nil {
		return err
	}
	evidenceDocument, err := snapshot.read("evidence.json", MaximumEvidenceBytes)
	if err != nil {
		return fmt.Errorf("read canonical evidence.json from bundle snapshot: %w", err)
	}
	if !bytes.Equal(evidenceDocument, canonicalEvidence) {
		return errors.New("bundle evidence.json is not the canonical verified evidence")
	}
	if err := verifyClosedWorld(snapshot, evidence); err != nil {
		return err
	}
	return nil
}

func verifyBundleArtifacts(evidence Evidence, snapshot bundleSnapshot) error {
	artifacts := evidence.Artifacts
	required := map[string]bool{
		"vulnerability_report": false, "migration_rollback": false, "dashboard_snapshot": false,
		"canary_snapshot": false, "production_manifest": false,
		"gateway_sbom": false, "gateway_provenance": false,
		"worker_sbom": false, "worker_provenance": false,
		"migrate_sbom": false, "migrate_provenance": false,
	}
	for _, name := range RequiredFormalArtifactNames {
		required[name] = false
	}
	if len(artifacts) < len(required) || len(artifacts) > 256 {
		return errors.New("bundle artifact manifest is incomplete or too large")
	}
	previous := ""
	for _, artifact := range artifacts {
		if !inputNamePattern.MatchString(artifact.Name) || artifact.Name <= previous || !digestPattern.MatchString(artifact.SHA256) {
			return errors.New("bundle artifacts must have valid, unique, sorted names and digests")
		}
		previous = artifact.Name
		if _, ok := required[artifact.Name]; ok {
			required[artifact.Name] = true
		}
		path, err := safeArtifactPath(artifact.Path)
		if err != nil {
			return fmt.Errorf("bundle artifact %q: %w", artifact.Name, err)
		}
		document, err := snapshot.read(filepath.ToSlash(path), MaximumArtifactBytes)
		if err != nil {
			return fmt.Errorf("read bundle artifact %q: %w", artifact.Name, err)
		}
		if err := ValidateArtifact(artifact.Name, document); err != nil {
			return fmt.Errorf("bundle artifact %q: %w", artifact.Name, err)
		}
		if err := validateArtifactContext(artifact.Name, document, evidence, snapshot); err != nil {
			return fmt.Errorf("bundle artifact %q context: %w", artifact.Name, err)
		}
		digest := sha256.Sum256(document)
		if hex.EncodeToString(digest[:]) != artifact.SHA256 {
			return fmt.Errorf("bundle artifact %q sha256 mismatch", artifact.Name)
		}
	}
	for name, found := range required {
		if !found {
			return fmt.Errorf("required bundle artifact %q is missing", name)
		}
	}
	return nil
}

type baseArtifact struct {
	SchemaVersion string `json:"schema_version"`
	ArtifactType  string `json:"artifact_type"`
	Status        string `json:"status"`
}

type vulnerabilityArtifact struct {
	SchemaVersion string `json:"schema_version"`
	ArtifactType  string `json:"artifact_type"`
	Status        string `json:"status"`
	Suite         string `json:"suite"`
}

type migrationArtifact struct {
	SchemaVersion       string               `json:"schema_version"`
	ArtifactType        string               `json:"artifact_type"`
	Status              string               `json:"status"`
	Migration           string               `json:"migration"`
	CandidateImage      string               `json:"candidate_image"`
	Executions          []migrationExecution `json:"executions"`
	DatabaseImage       string               `json:"database_image"`
	DatabaseContainerID string               `json:"database_container_id"`
	DatabaseServerMajor int                  `json:"database_server_major"`
	StartedAt           time.Time            `json:"started_at"`
	FinishedAt          time.Time            `json:"finished_at"`
}

type migrationExecution struct {
	Direction   string `json:"direction"`
	ContainerID string `json:"container_id"`
	ExitCode    int    `json:"exit_code"`
}

type digestArtifact struct {
	SchemaVersion string `json:"schema_version"`
	ArtifactType  string `json:"artifact_type"`
	Status        string `json:"status"`
	SourceSHA256  string `json:"source_sha256"`
}

type canaryArtifact struct {
	SchemaVersion             string    `json:"schema_version"`
	ArtifactType              string    `json:"artifact_type"`
	Status                    string    `json:"status"`
	Observation               string    `json:"observation"`
	Retention                 string    `json:"retention"`
	Route                     string    `json:"route"`
	Sentinels                 int       `json:"sentinels"`
	Region                    string    `json:"region"`
	ConfigHash                string    `json:"config_hash"`
	GatewayImage              string    `json:"gateway_image"`
	ResponseIDCommitments     []string  `json:"response_id_commitments"`
	PostTerminalObservationMS int       `json:"post_terminal_observation_ms"`
	ObservedAt                time.Time `json:"observed_at"`
}

type supplyArtifact struct {
	SchemaVersion  string   `json:"schema_version"`
	ArtifactType   string   `json:"artifact_type"`
	Status         string   `json:"status"`
	Subject        string   `json:"subject"`
	SourceSHA      string   `json:"source_sha"`
	DocumentSHA256 string   `json:"document_sha256"`
	Platforms      []string `json:"platforms,omitempty"`
}

type formalArtifact struct {
	SchemaVersion            string                    `json:"schema_version"`
	ArtifactType             string                    `json:"artifact_type"`
	Status                   string                    `json:"status"`
	SourceName               string                    `json:"source_name"`
	SourceSHA256             string                    `json:"source_sha256"`
	Selector                 string                    `json:"selector"`
	StartedAt                time.Time                 `json:"started_at"`
	FinishedAt               time.Time                 `json:"finished_at"`
	Commit                   string                    `json:"commit"`
	ReleaseConfigHash        string                    `json:"release_config_hash"`
	RuntimeConfigHash        string                    `json:"runtime_config_hash"`
	FormalConfigSHA256       string                    `json:"formal_config_sha256"`
	ExecutionEnvironment     string                    `json:"execution_environment"`
	Region                   string                    `json:"region"`
	CandidateGatewayImage    string                    `json:"candidate_gateway_image"`
	CandidateMigrateImage    string                    `json:"candidate_migrate_image"`
	CandidateWorkerImage     string                    `json:"candidate_worker_image"`
	TopologySHA256           string                    `json:"topology_sha256"`
	FormalManifestSHA256     string                    `json:"formal_manifest_sha256"`
	ProductionManifestSHA256 string                    `json:"production_manifest_sha256"`
	DurationSeconds          int                       `json:"duration_seconds"`
	SampleCount              int                       `json:"sample_count"`
	ChaosCase                string                    `json:"chaos_case"`
	Containers               formalContainerIdentities `json:"containers"`
}

type formalContainerIdentities struct {
	Postgres  string `json:"postgres"`
	Toxiproxy string `json:"toxiproxy"`
	Gateway   string `json:"gateway"`
	Worker    string `json:"worker"`
	OIDC      string `json:"oidc"`
	Voice     string `json:"voice"`
	DeepSeek  string `json:"deepseek"`
}

// ValidateArtifact applies a closed, content-free schema selected by the
// manifest name. JSON key decoding catches whitespace, escaping and nesting
// tricks; arbitrary text and binary media are never valid release evidence.
func ValidateArtifact(name string, document []byte) error {
	if int64(len(document)) > MaximumArtifactBytes || !utf8.Valid(document) || bytes.IndexByte(document, 0) >= 0 {
		return errors.New("artifact is not bounded UTF-8 JSON")
	}
	if err := ScanContent(document); err != nil {
		return err
	}
	validateBase := func(base baseArtifact) error {
		if base.SchemaVersion != "release-artifact-v2" || base.ArtifactType != name || base.Status != "passed" {
			return errors.New("artifact identity or passed status is invalid")
		}
		return nil
	}
	var value any
	switch {
	case name == "vulnerability_report":
		var artifact vulnerabilityArtifact
		value = &artifact
		if err := strictDecode(document, value); err != nil {
			return fmt.Errorf("decode vulnerability artifact: %w", err)
		}
		if err := validateBase(baseArtifact{artifact.SchemaVersion, artifact.ArtifactType, artifact.Status}); err != nil {
			return err
		}
		if artifact.Suite != "govulncheck" {
			return errors.New("vulnerability suite must be govulncheck")
		}
		return nil
	case name == "migration_rollback":
		var artifact migrationArtifact
		value = &artifact
		if err := strictDecode(document, value); err != nil {
			return fmt.Errorf("decode migration artifact: %w", err)
		}
		if err := validateBase(baseArtifact{artifact.SchemaVersion, artifact.ArtifactType, artifact.Status}); err != nil {
			return err
		}
		if artifact.Migration != "up-down-up" || artifact.DatabaseImage != PinnedPostgresImage || !exactCandidateImage("migrate", artifact.CandidateImage) || !validMigrationExecutions(artifact.Executions) || !containerIDPattern.MatchString(artifact.DatabaseContainerID) || artifact.DatabaseServerMajor != 16 || artifact.StartedAt.IsZero() || artifact.FinishedAt.Before(artifact.StartedAt) || artifact.StartedAt.Location() != time.UTC || artifact.FinishedAt.Location() != time.UTC {
			return errors.New("migration evidence does not bind a pinned up-down-up database")
		}
		return nil
	case name == "dashboard_snapshot" || name == "production_manifest":
		var artifact digestArtifact
		value = &artifact
		if err := strictDecode(document, value); err != nil {
			return fmt.Errorf("decode digest artifact: %w", err)
		}
		if err := validateBase(baseArtifact{artifact.SchemaVersion, artifact.ArtifactType, artifact.Status}); err != nil {
			return err
		}
		if !digestPattern.MatchString(artifact.SourceSHA256) {
			return errors.New("artifact source digest is malformed")
		}
		return nil
	case name == "canary_snapshot":
		var artifact canaryArtifact
		value = &artifact
		if err := strictDecode(document, value); err != nil {
			return fmt.Errorf("decode canary artifact: %w", err)
		}
		if err := validateBase(baseArtifact{artifact.SchemaVersion, artifact.ArtifactType, artifact.Status}); err != nil {
			return err
		}
		if artifact.Observation != "isolated-two-session" || artifact.Retention != "none" || artifact.Route != "wss" || artifact.Sentinels != 2 || !regionPattern.MatchString(artifact.Region) || !configPattern.MatchString(artifact.ConfigHash) || !exactCandidateImage("gateway", artifact.GatewayImage) || len(artifact.ResponseIDCommitments) != 2 || artifact.ResponseIDCommitments[0] == artifact.ResponseIDCommitments[1] || !digestPattern.MatchString(artifact.ResponseIDCommitments[0]) || !digestPattern.MatchString(artifact.ResponseIDCommitments[1]) || artifact.PostTerminalObservationMS < 250 || artifact.PostTerminalObservationMS > 5000 || artifact.ObservedAt.IsZero() || artifact.ObservedAt.Location() != time.UTC {
			return errors.New("canary observation is not the bounded production contract")
		}
		return nil
	case strings.HasSuffix(name, "_sbom") || strings.HasSuffix(name, "_provenance"):
		var artifact supplyArtifact
		value = &artifact
		if err := strictDecode(document, value); err != nil {
			return fmt.Errorf("decode supply-chain artifact: %w", err)
		}
		if err := validateBase(baseArtifact{artifact.SchemaVersion, artifact.ArtifactType, artifact.Status}); err != nil {
			return err
		}
		target := strings.TrimSuffix(strings.TrimSuffix(name, "_sbom"), "_provenance")
		if !exactCandidateImage(target, artifact.Subject) || !commitPattern.MatchString(artifact.SourceSHA) || !digestPattern.MatchString(artifact.DocumentSHA256) {
			return errors.New("supply-chain subject or source binding is malformed")
		}
		if strings.HasSuffix(name, "_provenance") && !slices.Equal(artifact.Platforms, []string{"linux/amd64", "linux/arm64"}) {
			return errors.New("provenance platforms must be exact amd64 and arm64")
		}
		if strings.HasSuffix(name, "_sbom") && len(artifact.Platforms) != 0 {
			return errors.New("SBOM artifact cannot declare provenance platforms")
		}
		return nil
	case strings.HasPrefix(name, "formal_"):
		var artifact formalArtifact
		value = &artifact
		if err := strictDecode(document, value); err != nil {
			return fmt.Errorf("decode formal artifact: %w", err)
		}
		if err := validateBase(baseArtifact{artifact.SchemaVersion, artifact.ArtifactType, artifact.Status}); err != nil {
			return err
		}
		contract, ok := formalContracts[name]
		if !ok || artifact.SourceName != contract.SourceName || artifact.Selector != contract.Selector || !digestPattern.MatchString(artifact.SourceSHA256) || artifact.StartedAt.IsZero() || artifact.FinishedAt.Before(artifact.StartedAt) || artifact.StartedAt.Location() != time.UTC || artifact.FinishedAt.Location() != time.UTC || !commitPattern.MatchString(artifact.Commit) || !configPattern.MatchString(artifact.ReleaseConfigHash) || !configPattern.MatchString(artifact.RuntimeConfigHash) || !digestPattern.MatchString(artifact.FormalConfigSHA256) || artifact.ExecutionEnvironment != "formal-chaos" || !regionPattern.MatchString(artifact.Region) || !exactCandidateImage("gateway", artifact.CandidateGatewayImage) || !exactCandidateImage("migrate", artifact.CandidateMigrateImage) || !exactCandidateImage("worker", artifact.CandidateWorkerImage) || !digestPattern.MatchString(artifact.TopologySHA256) || !digestPattern.MatchString(artifact.FormalManifestSHA256) || !digestPattern.MatchString(artifact.ProductionManifestSHA256) || artifact.DurationSeconds != contract.DurationSeconds || artifact.SampleCount != contract.SampleCount || artifact.ChaosCase != contract.ChaosCase || !validFormalContainers(artifact.Containers) {
			return errors.New("formal artifact source binding is malformed")
		}
		return nil
	default:
		return fmt.Errorf("artifact name %q has no allowlisted schema", name)
	}
}

func validMigrationExecutions(values []migrationExecution) bool {
	if len(values) != 3 {
		return false
	}
	directions := []string{"up", "down", "up"}
	seen := make(map[string]struct{}, len(values))
	for index, value := range values {
		if value.Direction != directions[index] || value.ExitCode != 0 || !containerIDPattern.MatchString(value.ContainerID) {
			return false
		}
		seen[value.ContainerID] = struct{}{}
	}
	return len(seen) == len(values)
}

func exactCandidateImage(target, value string) bool {
	if !imageDigestPattern.MatchString(value) {
		return false
	}
	wantPrefix := "ghcr.io/chuanmingliu/openai-gpt-live/" + target + "@sha256:"
	return strings.HasPrefix(value, wantPrefix)
}

type formalContract struct {
	SourceName      string
	Selector        string
	DurationSeconds int
	SampleCount     int
	ChaosCase       string
}

var formalContracts = map[string]formalContract{
	"formal_topology_identity":                   {"topology-identity.json", "inspected Compose images, container runtime config, and mounted formal config", 0, 0, ""},
	"formal_manifest":                            {"manifest.json", "formal evidence manifest", 0, 0, ""},
	"formal_unit_race":                           {"unit-race.json", "go test -json -race -count=1 ./cmd/live-load ./internal/loadgen", 0, 0, ""},
	"formal_capacity_slo_10m":                    {"capacity-profile-slo-10m.json", "go test -json -count=1 -tags=load -run ^TestGateway250Sessions$ -timeout=20m ./test/load/...", 600, 0, ""},
	"formal_barge_20":                            {"barge-in-20.json", "go test -json -count=1 -tags=load -run ^TestBargeInAcknowledgedFenceSuppressesEveryStaleFrame$ -timeout=2m ./test/load/...", 0, 20, ""},
	"formal_chaos_postgresql_disconnect":         {"chaos-postgresql-disconnect.json", "go test -json -count=1 -tags=chaos -run ^TestPostgreSQLDisconnect$ -timeout=30m ./test/chaos/...", 0, 0, "postgresql_disconnect"},
	"formal_chaos_deepseek_429":                  {"chaos-deepseek-429.json", "go test -json -count=1 -tags=chaos -run ^TestDeepSeek429HonorsBoundedRetryAfter$ -timeout=30m ./test/chaos/...", 0, 0, "deepseek_429"},
	"formal_chaos_slow_client_backpressure":      {"chaos-slow-client-backpressure.json", "go test -json -count=1 -tags=chaos -run ^TestSlowClientBackpressure$ -timeout=30m ./test/chaos/...", 0, 0, "slow_client_backpressure"},
	"formal_chaos_stalled_voice_provider":        {"chaos-stalled-voice-provider.json", "go test -json -count=1 -tags=chaos -run ^TestStalledVoiceProvider$ -timeout=30m ./test/chaos/...", 0, 0, "stalled_voice_provider"},
	"formal_chaos_killed_worker_lease_recovery":  {"chaos-killed-worker-lease-recovery.json", "go test -json -count=1 -tags=chaos -run ^TestKilledWorkerLeaseRecovery$ -timeout=30m ./test/chaos/...", 0, 0, "killed_worker_lease_recovery"},
	"formal_chaos_lost_notification_poll_repair": {"chaos-lost-notification-poll-repair.json", "go test -json -count=1 -tags=chaos -run ^TestLostNotificationPollRepair$ -timeout=30m ./test/chaos/...", 0, 0, "lost_notification_poll_repair"},
	"formal_soak_2h":                             {"soak-2h.json", "go test -json -count=1 -tags=soak -run ^TestGatewaySoak$ -timeout=150m ./test/load/...", 7200, 0, ""},
}

func validFormalContainers(containers formalContainerIdentities) bool {
	return containerIDPattern.MatchString(containers.Postgres) &&
		containerIDPattern.MatchString(containers.Toxiproxy) &&
		containerIDPattern.MatchString(containers.Gateway) &&
		containerIDPattern.MatchString(containers.Worker) &&
		containerIDPattern.MatchString(containers.OIDC) &&
		containerIDPattern.MatchString(containers.Voice) &&
		containerIDPattern.MatchString(containers.DeepSeek)
}

func validateArtifactContext(name string, document []byte, evidence Evidence, snapshot bundleSnapshot) error {
	inputs := make(map[string]string, len(evidence.ReleaseInputs))
	for _, input := range evidence.ReleaseInputs {
		inputs[input.Name] = input.Value
	}
	switch {
	case name == "migration_rollback":
		var artifact migrationArtifact
		if err := strictDecode(document, &artifact); err != nil {
			return err
		}
		if artifact.CandidateImage != inputs["candidate_migrate_image"] {
			return errors.New("migration did not execute the signed migrate candidate")
		}
		lastGate := evidence.Gates[len(evidence.Gates)-1]
		if artifact.StartedAt.Before(lastGate.FinishedAt) || artifact.FinishedAt.After(evidence.GeneratedAt) {
			return errors.New("migration observation is outside the ordered post-gate window")
		}
	case name == "production_manifest":
		var artifact digestArtifact
		if err := strictDecode(document, &artifact); err != nil {
			return err
		}
		if artifact.SourceSHA256 != inputs["manifest_sha256"] {
			return errors.New("production manifest digest differs from the signed release input")
		}
	case name == "canary_snapshot":
		var artifact canaryArtifact
		if err := strictDecode(document, &artifact); err != nil {
			return err
		}
		if artifact.ConfigHash != evidence.ConfigHash || artifact.Region != inputs["deployment_region"] || artifact.GatewayImage != inputs["candidate_gateway_image"] {
			return errors.New("canary runtime identity differs from the signed release inputs")
		}
		if artifact.ObservedAt.Before(evidence.GeneratedAt.Add(-maximumEvidenceAge)) || artifact.ObservedAt.After(evidence.GeneratedAt) {
			return errors.New("canary observation is outside the fresh bundle window")
		}
	case strings.HasSuffix(name, "_sbom") || strings.HasSuffix(name, "_provenance"):
		var artifact supplyArtifact
		if err := strictDecode(document, &artifact); err != nil {
			return err
		}
		target := strings.TrimSuffix(strings.TrimSuffix(name, "_sbom"), "_provenance")
		if artifact.Subject != inputs["candidate_"+target+"_image"] || artifact.SourceSHA != evidence.Commit {
			return errors.New("supply-chain statement differs from the signed candidate or commit")
		}
	case strings.HasPrefix(name, "formal_"):
		return validateFormalArtifactContext(name, document, evidence, inputs, snapshot)
	}
	return nil
}

func validateFormalArtifactContext(name string, document []byte, evidence Evidence, inputs map[string]string, snapshot bundleSnapshot) error {
	var artifact formalArtifact
	if err := strictDecode(document, &artifact); err != nil {
		return err
	}
	if artifact.Commit != evidence.Commit || artifact.ReleaseConfigHash != evidence.ConfigHash || artifact.Region != inputs["deployment_region"] || artifact.CandidateGatewayImage != inputs["candidate_gateway_image"] || artifact.CandidateMigrateImage != inputs["candidate_migrate_image"] || artifact.CandidateWorkerImage != inputs["candidate_worker_image"] || artifact.ProductionManifestSHA256 != inputs["manifest_sha256"] {
		return errors.New("formal report differs from the signed release context")
	}
	var topologyDigest, manifestDigest, runtimeConfigHash, formalConfigSHA256 string
	for _, descriptor := range evidence.Artifacts {
		if descriptor.Name != "formal_topology_identity" && descriptor.Name != "formal_manifest" {
			continue
		}
		anchorDocument, err := snapshot.read(descriptor.Path, MaximumArtifactBytes)
		if err != nil {
			return fmt.Errorf("read formal anchor %q: %w", descriptor.Name, err)
		}
		var anchor formalArtifact
		if err := strictDecode(anchorDocument, &anchor); err != nil {
			return fmt.Errorf("decode formal anchor %q: %w", descriptor.Name, err)
		}
		if descriptor.Name == "formal_topology_identity" {
			topologyDigest = anchor.SourceSHA256
			runtimeConfigHash = anchor.RuntimeConfigHash
			formalConfigSHA256 = anchor.FormalConfigSHA256
		} else {
			manifestDigest = anchor.SourceSHA256
		}
	}
	if artifact.TopologySHA256 != topologyDigest || artifact.FormalManifestSHA256 != manifestDigest || artifact.RuntimeConfigHash != runtimeConfigHash || artifact.FormalConfigSHA256 != formalConfigSHA256 {
		return errors.New("formal report topology or manifest cross-reference is not the signed descriptor digest")
	}
	var sloGate GateEvidence
	for _, gate := range evidence.Gates {
		if gate.GateID == "slo-load" {
			sloGate = gate
			break
		}
	}
	if sloGate.GateID == "" || artifact.StartedAt.Before(sloGate.StartedAt) || artifact.FinishedAt.After(sloGate.FinishedAt) {
		return errors.New("formal report timing is outside the signed slo-load gate")
	}
	return nil
}

func verifyClosedWorld(snapshot bundleSnapshot, evidence Evidence) error {
	allowed := map[string]struct{}{"evidence.json": {}}
	for _, gate := range evidence.Gates {
		allowed[filepath.ToSlash(filepath.Join("gates", gate.GateID+".json"))] = struct{}{}
		allowed[gate.ArtifactPath] = struct{}{}
	}
	for _, artifact := range evidence.Artifacts {
		allowed[artifact.Path] = struct{}{}
	}
	if len(snapshot.files) != len(allowed) {
		return fmt.Errorf("bundle undeclared or missing file inventory mismatch: found %d files, expected %d", len(snapshot.files), len(allowed))
	}
	allowedDirectories := make(map[string]struct{})
	for path := range allowed {
		for directory := filepath.ToSlash(filepath.Dir(path)); directory != "."; directory = filepath.ToSlash(filepath.Dir(directory)) {
			allowedDirectories[directory] = struct{}{}
		}
	}
	for relative := range snapshot.files {
		if _, ok := allowed[relative]; !ok {
			return fmt.Errorf("bundle contains undeclared file %q", relative)
		}
	}
	if len(snapshot.directories) != len(allowedDirectories) {
		return fmt.Errorf("bundle directory inventory mismatch: found %d directories, expected %d", len(snapshot.directories), len(allowedDirectories))
	}
	for relative := range snapshot.directories {
		if _, ok := allowedDirectories[relative]; !ok {
			return fmt.Errorf("bundle contains undeclared directory %q", relative)
		}
	}
	return nil
}

func verifyReleaseInputs(inputs []ReleaseInput) error {
	validators := map[string]func(string) bool{
		"candidate_gateway_image":  func(value string) bool { return exactCandidateImage("gateway", value) },
		"candidate_migrate_image":  func(value string) bool { return exactCandidateImage("migrate", value) },
		"candidate_worker_image":   func(value string) bool { return exactCandidateImage("worker", value) },
		"deployment_region":        regionPattern.MatchString,
		"egress_proxy_cidr":        validCanonicalPrefix,
		"manifest_sha256":          digestPattern.MatchString,
		"oidc_issuer":              func(value string) bool { return validReleaseURL(value, "https") },
		"optional_provider_result": func(value string) bool { return value == "passed" || value == "not-requested" },
		"production_host":          validDNSName,
		"production_tls_secret":    kubernetesNamePattern.MatchString,
		"run_provider_tests":       func(value string) bool { return value == "true" || value == "false" },
		"trusted_proxy_cidr":       validCanonicalPrefix,
		"voice_url":                func(value string) bool { return validReleaseURL(value, "wss") },
	}
	if len(inputs) != len(validators) {
		return fmt.Errorf("release_inputs must contain the exact %d reviewed fields", len(validators))
	}
	names := make([]string, len(inputs))
	for index, input := range inputs {
		validate, allowed := validators[input.Name]
		if !allowed || !inputNamePattern.MatchString(input.Name) || input.Value == "" || len(input.Value) > 2048 || strings.ContainsAny(input.Value, "\x00\r\n") || !validate(input.Value) {
			return fmt.Errorf("release input %d is malformed", index+1)
		}
		names[index] = input.Name
		if index > 0 && inputs[index-1].Name >= input.Name {
			return errors.New("release_inputs must be uniquely sorted by name")
		}
	}
	if !sort.StringsAreSorted(names) {
		return errors.New("release_inputs must be sorted by name")
	}
	return nil
}

func validCanonicalPrefix(value string) bool {
	prefix, err := netip.ParsePrefix(value)
	return err == nil && prefix.String() == value
}

func validDNSName(value string) bool {
	if len(value) > 253 || !dnsNamePattern.MatchString(value) || strings.Contains(value, "..") {
		return false
	}
	for _, label := range strings.Split(value, ".") {
		if len(label) == 0 || len(label) > 63 || strings.HasPrefix(label, "-") || strings.HasSuffix(label, "-") {
			return false
		}
	}
	return true
}

func validReleaseURL(value, scheme string) bool {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != scheme || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Opaque != "" {
		return false
	}
	return parsed.Hostname() != "" && validDNSName(parsed.Hostname())
}

// ScanContent is shared by collection and verification so an unscanned
// generated artifact can never enter a signable bundle.
func ScanContent(document []byte) error {
	lower := bytes.ToLower(document)
	for _, marker := range prohibitedArtifactContent {
		if bytes.Contains(lower, marker) {
			return fmt.Errorf("artifact contains prohibited content marker %q", marker)
		}
	}
	if json.Valid(document) {
		var value any
		decoder := json.NewDecoder(bytes.NewReader(document))
		if err := decoder.Decode(&value); err == nil {
			if err := scanJSONValue(value, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func scanJSONValue(value any, isKey bool) error {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if err := scanJSONValue(key, true); err != nil {
				return err
			}
			if err := scanJSONValue(child, false); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range typed {
			if err := scanJSONValue(child, false); err != nil {
				return err
			}
		}
	case string:
		lower := strings.ToLower(typed)
		normalized := strings.NewReplacer("_", "", "-", "", ".", "", " ", "").Replace(lower)
		if isKey {
			for _, marker := range []string{"audio", "pcm", "transcript", "reasoning", "toolargument", "toolresult", "apikey", "credential", "password", "authorization", "secret"} {
				if strings.Contains(normalized, marker) {
					return fmt.Errorf("artifact contains prohibited JSON field %q", typed)
				}
			}
		}
		for _, marker := range []string{"sk_live_", "sk-", "bearer ", "-----begin private key-----"} {
			if strings.Contains(lower, marker) {
				return errors.New("artifact contains a prohibited credential-shaped string")
			}
		}
	}
	return nil
}

func strictDecode(document []byte, target any) error {
	if err := rejectDuplicateKeys(document); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return requireEOF(decoder)
}

func verifyGate(gate GateEvidence, generatedAt, now time.Time, snapshot bundleSnapshot) error {
	wantProducer := ExpectedProducers[gate.GateID]
	if wantProducer == "" || gate.Producer != wantProducer {
		return fmt.Errorf("producer must be %q", wantProducer)
	}
	wantCommand, ok := ExpectedCommand(gate.GateID)
	if !ok || !slices.Equal(gate.Command, wantCommand) {
		return errors.New("command does not match the versioned producer contract")
	}
	for _, argument := range gate.Command {
		if argument == "" || len(argument) > 4096 || strings.ContainsAny(argument, "\x00\r\n") {
			return errors.New("command contains an invalid argument")
		}
	}
	if gate.StartedAt.IsZero() || gate.FinishedAt.IsZero() || gate.StartedAt.Location() != time.UTC || gate.FinishedAt.Location() != time.UTC {
		return errors.New("start and finish must be UTC timestamps")
	}
	if gate.FinishedAt.Before(gate.StartedAt) {
		return errors.New("finished_at precedes started_at")
	}
	oldest := now.Add(-maximumEvidenceAge)
	if gate.StartedAt.Before(oldest) || gate.FinishedAt.Before(oldest) {
		return errors.New("gate execution is older than 24 hours")
	}
	if gate.StartedAt.After(now.Add(maximumFutureSkew)) || gate.FinishedAt.After(now.Add(maximumFutureSkew)) {
		return errors.New("gate execution is too far in the future")
	}
	if gate.FinishedAt.After(generatedAt) {
		return errors.New("finished_at is after bundle generation")
	}
	if gate.ExitCode != 0 {
		return fmt.Errorf("command exited with status %d", gate.ExitCode)
	}
	if !digestPattern.MatchString(gate.ArtifactSHA256) {
		return errors.New("artifact sha256 is missing or malformed")
	}
	if gate.ArtifactPath != "gates/"+gate.GateID+".summary.json" {
		return errors.New("artifact path does not match the gate summary contract")
	}
	document, err := snapshot.read(gate.ArtifactPath, MaximumArtifactBytes)
	if err != nil {
		return fmt.Errorf("read artifact summary: %w", err)
	}
	digest := sha256.Sum256(document)
	actual := hex.EncodeToString(digest[:])
	if actual != gate.ArtifactSHA256 {
		return fmt.Errorf("artifact sha256 mismatch: got %s", actual)
	}
	var summary ArtifactSummary
	if err := strictDecode(document, &summary); err != nil {
		return fmt.Errorf("decode content-free artifact summary: %w", err)
	}
	if summary.SchemaVersion != SummarySchemaVersion || summary.GateID != gate.GateID || summary.Producer != gate.Producer || summary.Status != "passed" || summary.OutputBytes < 0 {
		return errors.New("artifact summary does not satisfy the content-free schema")
	}
	if err := ScanContent(document); err != nil {
		return err
	}
	return nil
}

func verifyGateRecord(gate GateEvidence, snapshot bundleSnapshot) error {
	relative := "gates/" + gate.GateID + ".json"
	document, err := snapshot.read(relative, MaximumArtifactBytes)
	if err != nil {
		return fmt.Errorf("read exact gate record: %w", err)
	}
	if err := ScanContent(document); err != nil {
		return err
	}
	decoded, err := DecodeGateEvidence(document)
	if err != nil {
		return err
	}
	canonical, err := CanonicalJSON(decoded)
	if err != nil {
		return err
	}
	if !bytes.Equal(document, canonical) {
		return errors.New("gate record is not canonical JSON")
	}
	want, err := CanonicalJSON(gate)
	if err != nil {
		return err
	}
	if !bytes.Equal(document, want) {
		return errors.New("gate record does not equal the signed embedded record")
	}
	return nil
}

func safeArtifactPath(relative string) (string, error) {
	if relative == "" || strings.Contains(relative, `\`) || strings.ContainsRune(relative, '\x00') || filepath.IsAbs(relative) {
		return "", errors.New("artifact path is unsafe")
	}
	clean := filepath.Clean(filepath.FromSlash(relative))
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) || filepath.ToSlash(clean) != relative {
		return "", errors.New("artifact path is unsafe")
	}
	return clean, nil
}

func verifyAttestations(evidence Evidence, options VerifyOptions) error {
	if len(evidence.Attestations) != 1 {
		return errors.New("exactly one privacy human attestation is required")
	}
	attestation := evidence.Attestations[0]
	if attestation.GateID != PrivacyAttestationGateID {
		return fmt.Errorf("attestation gate must be %q", PrivacyAttestationGateID)
	}
	if attestation.Signer == "" || len(attestation.Signer) > 128 || strings.ContainsAny(attestation.Signer, "\x00\r\n") {
		return errors.New("attestation signer is invalid")
	}
	publicKey, trusted := options.AttestationKeys[attestation.Signer]
	if !trusted || len(publicKey) != ed25519.PublicKeySize {
		return fmt.Errorf("attestation signer %q is not trusted", attestation.Signer)
	}
	if attestation.SignedAt.IsZero() || attestation.SignedAt.Location() != time.UTC {
		return errors.New("attestation signed_at must be a UTC timestamp")
	}
	if attestation.SignedAt.Before(evidence.GeneratedAt) || attestation.SignedAt.After(options.Now.Add(maximumFutureSkew)) {
		return errors.New("attestation signed_at is outside the accepted window")
	}
	wantStatement, err := AttestationEnvelopeSHA256(evidence, PrivacyAttestationGateID, attestation.Signer, attestation.SignedAt)
	if err != nil {
		return err
	}
	if attestation.StatementSHA256 != wantStatement || !digestPattern.MatchString(attestation.StatementSHA256) {
		return errors.New("attestation signature envelope sha256 does not match evidence")
	}
	statementBytes, err := hex.DecodeString(attestation.StatementSHA256)
	if err != nil {
		return errors.New("attestation statement sha256 is invalid")
	}
	signature, err := base64.StdEncoding.Strict().DecodeString(attestation.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return errors.New("attestation signature is malformed")
	}
	if !ed25519.Verify(publicKey, statementBytes, signature) {
		return errors.New("attestation signature verification failed")
	}
	return nil
}
