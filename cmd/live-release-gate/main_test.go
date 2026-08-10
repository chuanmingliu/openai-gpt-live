package main

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/releasegate"
	"golang.org/x/sys/unix"
)

const (
	cliGatewayRef = "ghcr.io/chuanmingliu/openai-gpt-live/gateway@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	cliWorkerRef  = "ghcr.io/chuanmingliu/openai-gpt-live/worker@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	cliMigrateRef = "ghcr.io/chuanmingliu/openai-gpt-live/migrate@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
)

func TestCLIHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_RELEASE_GATE_HELPER") != "1" {
		return
	}
	separator := 0
	for separator < len(os.Args) && os.Args[separator] != "--" {
		separator++
	}
	if separator == len(os.Args) {
		os.Exit(97)
	}
	if err := run(os.Args[separator+1:]); err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}

func TestCLISubprocessRejectsHostilePreexistingSymlink(t *testing.T) {
	root := realCLITempDir(t)
	if err := os.Mkdir(filepath.Join(root, "gates"), 0o700); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(realCLITempDir(t), "outside")
	if err := os.WriteFile(outside, []byte("unchanged"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "gates/simulator-voice.summary.json")); err != nil {
		t.Fatal(err)
	}
	args := []string{"-test.run=TestCLIHelperProcess", "--", "record", "--gate-id", "simulator-voice", "--producer", releasegate.ExpectedProducers["simulator-voice"], "--artifact-root", root, "--artifact", "gates/simulator-voice.summary.json", "--evidence-out", filepath.Join(root, "gates/simulator-voice.json"), "--", "true"}
	command := exec.Command(os.Args[0], args...)
	command.Env = append(os.Environ(), "GO_WANT_RELEASE_GATE_HELPER=1")
	if err := command.Run(); err == nil {
		t.Fatal("hostile symlink fixture succeeded in CLI subprocess")
	}
	contents, err := os.ReadFile(outside)
	if err != nil || string(contents) != "unchanged" {
		t.Fatalf("outside target changed: %q, %v", contents, err)
	}
}

func TestTwoPhaseBundleBindsExactReviewedInputs(t *testing.T) {
	originalCommand := commandForRecord
	commandForRecord = func(_ string, _ ...string) *exec.Cmd { return exec.Command("true") }
	t.Cleanup(func() { commandForRecord = originalCommand })
	root := realCLITempDir(t)
	gates := filepath.Join(root, "gates")
	if err := os.Mkdir(gates, 0o700); err != nil {
		t.Fatal(err)
	}
	var gateArgs []string
	var gatesByID = make(map[string]releasegate.GateEvidence)
	for _, gateID := range releasegate.RequiredGateIDs {
		artifact := "gates/" + gateID + ".summary.json"
		gatePath := filepath.Join(gates, gateID+".json")
		exactCommand, _ := releasegate.ExpectedCommand(gateID)
		args := []string{"--gate-id", gateID, "--producer", releasegate.ExpectedProducers[gateID], "--artifact-root", root, "--artifact", artifact, "--evidence-out", gatePath, "--"}
		args = append(args, exactCommand...)
		if err := record(args); err != nil {
			t.Fatal(err)
		}
		gateDocument, err := os.ReadFile(gatePath)
		if err != nil {
			t.Fatal(err)
		}
		gate, err := releasegate.DecodeGateEvidence(gateDocument)
		if err != nil {
			t.Fatal(err)
		}
		gatesByID[gateID] = gate
		gateArgs = append(gateArgs, "--gate-evidence", gatePath)
	}
	artifactNames := []string{"canary_snapshot", "dashboard_snapshot", "gateway_provenance", "gateway_sbom", "migrate_provenance", "migrate_sbom", "migration_rollback", "production_manifest", "vulnerability_report", "worker_provenance", "worker_sbom"}
	artifactNames = append(artifactNames, releasegate.RequiredFormalArtifactNames...)
	sort.Strings(artifactNames)
	if err := os.Mkdir(filepath.Join(root, "artifacts"), 0o700); err != nil {
		t.Fatal(err)
	}
	context := cliArtifactContext{
		CanaryObservedAt:    time.Now().UTC(),
		MigrationStartedAt:  gatesByID["deployment-drain"].FinishedAt,
		MigrationFinishedAt: time.Now().UTC(),
		FormalStartedAt:     gatesByID["slo-load"].StartedAt,
		FormalFinishedAt:    gatesByID["slo-load"].FinishedAt,
	}
	var artifactArgs []string
	for _, name := range artifactNames {
		relative := "artifacts/" + name + ".json"
		if err := os.WriteFile(filepath.Join(root, relative), validCLIArtifact(t, name, context), 0o600); err != nil {
			t.Fatal(err)
		}
		artifactArgs = append(artifactArgs, "--artifact", name+"="+relative)
	}
	evidencePath := filepath.Join(root, "evidence.json")
	commit := strings.Repeat("a", 40)
	configHash := "v1:" + strings.Repeat("a", 64)
	collectArgs := []string{"--commit", commit, "--config-hash", configHash, "--evidence-out", evidencePath}
	collectArgs = append(collectArgs, cliReleaseInputArgs()...)
	collectArgs = append(collectArgs, gateArgs...)
	collectArgs = append(collectArgs, artifactArgs...)
	if err := collect(collectArgs); err != nil {
		t.Fatal(err)
	}
	document, err := os.ReadFile(evidencePath)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := releasegate.DecodeEvidence(document)
	if err != nil {
		t.Fatal(err)
	}
	signedAt := evidence.GeneratedAt.Add(time.Nanosecond)
	if err := statementHash([]string{"--evidence", evidencePath, "--signer", "test-operator", "--signed-at", signedAt.Format(time.RFC3339Nano)}); err != nil {
		t.Fatalf("statement-hash rejected the collected ordered evidence: %v", err)
	}
	statement, err := releasegate.AttestationEnvelopeSHA256(evidence, releasegate.PrivacyAttestationGateID, "test-operator", signedAt)
	if err != nil {
		t.Fatal(err)
	}
	seed := sha256.Sum256([]byte("cli-two-phase-test-key"))
	privateKey := ed25519.NewKeyFromSeed(seed[:])
	statementBytes, _ := hex.DecodeString(statement)
	attestation := releasegate.SignedAttestation{GateID: releasegate.PrivacyAttestationGateID, Signer: "test-operator", StatementSHA256: statement, Signature: base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, statementBytes)), SignedAt: signedAt}
	attestationDocument, _ := releasegate.CanonicalJSON(attestation)
	attestationPath := filepath.Join(realCLITempDir(t), "attestation.json")
	if err := os.WriteFile(attestationPath, attestationDocument, 0o600); err != nil {
		t.Fatal(err)
	}
	phaseTwo := append([]string(nil), collectArgs...)
	phaseTwo = append(phaseTwo, "--preserve-generated-at", "--attestation", attestationPath)
	if err := collect(phaseTwo); err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(realCLITempDir(t), "key")
	key := base64.StdEncoding.EncodeToString(privateKey.Public().(ed25519.PublicKey))
	if err := os.WriteFile(keyPath, []byte(key+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	verifyArgs := []string{"--evidence", evidencePath, "--commit", commit, "--config-hash", configHash}
	verifyArgs = append(verifyArgs, cliReleaseInputArgs()...)
	verifyArgs = append(verifyArgs, "--attestation-key", "test-operator="+keyPath)
	if err := verify(verifyArgs); err != nil {
		t.Fatal(err)
	}
	wrongArgs := append([]string(nil), verifyArgs...)
	for index, argument := range wrongArgs {
		if argument == "deployment_region=us-east-1" {
			wrongArgs[index] = "deployment_region=eu-west-1"
		}
	}
	if err := verify(wrongArgs); err == nil {
		t.Fatal("phase two accepted different reviewed inputs")
	}

	archivePath := filepath.Join(realCLITempDir(t), "signed-release-evidence.tar")
	archiveArgs := append(append([]string(nil), verifyArgs...), "--archive-out", archivePath)
	if err := verify(archiveArgs); err != nil {
		t.Fatalf("verify with immutable archive: %v", err)
	}
	archiveVerifyArgs := []string{"--archive", archivePath, "--commit", commit, "--config-hash", configHash}
	archiveVerifyArgs = append(archiveVerifyArgs, cliReleaseInputArgs()...)
	archiveVerifyArgs = append(archiveVerifyArgs, "--attestation-key", "test-operator="+keyPath)
	if err := verifyArchive(archiveVerifyArgs); err != nil {
		t.Fatalf("verify immutable archive: %v", err)
	}
	if err := os.WriteFile(evidencePath, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := verifyArchive(archiveVerifyArgs); err != nil {
		t.Fatalf("archive changed after source mutation: %v", err)
	}
}

func TestParseArtifactSpecificationsRejectsUnsortedAndDuplicateNames(t *testing.T) {
	for name, specs := range map[string][]string{
		"unsorted":  {"zeta=artifacts/zeta.json", "alpha=artifacts/alpha.json"},
		"duplicate": {"alpha=artifacts/alpha.json", "alpha=artifacts/other.json"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseArtifactSpecifications(specs); err == nil {
				t.Fatal("invalid artifact specification order was accepted")
			}
		})
	}
}

func TestArchiveCommandsPreserveUnsignedReviewBytesAcrossAttestationFinalization(t *testing.T) {
	for _, name := range []string{"snapshot-unsigned", "finalize-archive"} {
		err := run([]string{name})
		if err == nil || strings.Contains(err.Error(), "usage: live-release-gate") || strings.Contains(err.Error(), "unknown subcommand") {
			t.Fatalf("archive command %q is not wired: %v", name, err)
		}
	}
}

func TestReadBoundedNoFollowRejectsOversizeAndSymlink(t *testing.T) {
	root := realCLITempDir(t)
	exact := filepath.Join(root, "exact.json")
	if err := os.WriteFile(exact, make([]byte, releasegate.MaximumEvidenceBytes), 0o600); err != nil {
		t.Fatal(err)
	}
	if document, err := readBoundedNoFollow(exact, releasegate.MaximumEvidenceBytes); err != nil || int64(len(document)) != releasegate.MaximumEvidenceBytes {
		t.Fatalf("exact-limit read len=%d error=%v", len(document), err)
	}
	large := filepath.Join(root, "large.json")
	if err := os.WriteFile(large, make([]byte, releasegate.MaximumEvidenceBytes+1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readBoundedNoFollow(large, releasegate.MaximumEvidenceBytes); err == nil || !strings.Contains(err.Error(), "size") {
		t.Fatalf("oversized read error = %v", err)
	}
	target := filepath.Join(root, "target.json")
	if err := os.WriteFile(target, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link.json")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readBoundedNoFollow(link, releasegate.MaximumEvidenceBytes); err == nil {
		t.Fatal("symlinked top-level input was followed")
	}
}

func TestReadBoundedNoFollowRejectsSymlinkedParentAndFIFOWithoutBlocking(t *testing.T) {
	realRoot := realCLITempDir(t)
	path := filepath.Join(realRoot, "evidence.json")
	if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	aliasRoot := filepath.Join(realCLITempDir(t), "alias")
	if err := os.Symlink(realRoot, aliasRoot); err != nil {
		t.Fatal(err)
	}
	if _, err := readBoundedNoFollow(filepath.Join(aliasRoot, "evidence.json"), releasegate.MaximumEvidenceBytes); err == nil {
		t.Fatal("bounded evidence reader followed a symlinked parent")
	}

	fifo := filepath.Join(realRoot, "special.fifo")
	if err := unix.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := readBoundedNoFollow(fifo, releasegate.MaximumEvidenceBytes)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "type") {
			t.Fatalf("FIFO read error = %v, want non-regular type rejection", err)
		}
	case <-time.After(time.Second):
		writer, _ := unix.Open(fifo, unix.O_WRONLY|unix.O_NONBLOCK, 0)
		if writer >= 0 {
			_ = unix.Close(writer)
		}
		t.Fatal("bounded evidence reader blocked on a FIFO")
	}
}

func TestPreservationReadAcceptsEvidenceAboveArtifactCap(t *testing.T) {
	rootPath := realCLITempDir(t)
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	evidence := releasegate.Evidence{
		SchemaVersion: releasegate.SchemaVersion,
		Attestations: []releasegate.SignedAttestation{{
			Signature: strings.Repeat("A", int(releasegate.MaximumArtifactBytes)+(128<<10)),
		}},
	}
	document, err := releasegate.CanonicalJSON(evidence)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(document)) <= releasegate.MaximumArtifactBytes || int64(len(document)) > releasegate.MaximumEvidenceBytes {
		t.Fatalf("fixture size = %d, want (%d,%d]", len(document), releasegate.MaximumArtifactBytes, releasegate.MaximumEvidenceBytes)
	}
	if err := os.WriteFile(filepath.Join(rootPath, "evidence.json"), document, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readExistingEvidenceForPreservation(root, "evidence.json"); err != nil {
		t.Fatalf("phase-two evidence between artifact and evidence caps was rejected: %v", err)
	}
}

type cliArtifactContext struct {
	CanaryObservedAt    time.Time
	MigrationStartedAt  time.Time
	MigrationFinishedAt time.Time
	FormalStartedAt     time.Time
	FormalFinishedAt    time.Time
}

type cliFormalContract struct {
	SourceName      string
	Selector        string
	DurationSeconds int
	SampleCount     int
	ChaosCase       string
}

var cliFormalContracts = map[string]cliFormalContract{
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

func validCLIArtifact(t *testing.T, name string, context cliArtifactContext) []byte {
	t.Helper()
	digest := strings.Repeat("a", 64)
	base := map[string]any{"schema_version": "release-artifact-v2", "artifact_type": name, "status": "passed"}
	switch name {
	case "vulnerability_report":
		base["suite"] = "govulncheck"
	case "migration_rollback":
		base["migration"] = "up-down-up"
		base["candidate_image"] = cliMigrateRef
		base["executions"] = []map[string]any{
			{"direction": "up", "container_id": strings.Repeat("1", 64), "exit_code": 0},
			{"direction": "down", "container_id": strings.Repeat("3", 64), "exit_code": 0},
			{"direction": "up", "container_id": strings.Repeat("4", 64), "exit_code": 0},
		}
		base["database_image"] = releasegate.PinnedPostgresImage
		base["database_container_id"] = strings.Repeat("2", 64)
		base["database_server_major"] = 16
		base["started_at"] = context.MigrationStartedAt
		base["finished_at"] = context.MigrationFinishedAt
	case "dashboard_snapshot":
		base["source_sha256"] = digest
	case "canary_snapshot":
		base["observation"] = "isolated-two-session"
		base["retention"] = "none"
		base["route"] = "wss"
		base["sentinels"] = 2
		base["region"] = "us-east-1"
		base["config_hash"] = "v1:" + digest
		base["gateway_image"] = cliGatewayRef
		base["response_id_commitments"] = []string{
			"19cfe453f4352e12750875044e9fd4a1201da7a326a802da451d7914cb8fe94a",
			"2ae39b4c87e5f71cb3791f64395ad35d293c37b92c04c220800993a939222c72",
		}
		base["post_terminal_observation_ms"] = 500
		base["observed_at"] = context.CanaryObservedAt
	case "production_manifest":
		base["source_sha256"] = strings.Repeat("b", 64)
	default:
		if contract, ok := cliFormalContracts[name]; ok {
			sourceDigest := strings.Repeat("c", 64)
			if name == "formal_topology_identity" {
				sourceDigest = strings.Repeat("a", 64)
			} else if name == "formal_manifest" {
				sourceDigest = strings.Repeat("b", 64)
			}
			base["source_name"] = contract.SourceName
			base["source_sha256"] = sourceDigest
			base["selector"] = contract.Selector
			base["started_at"] = context.FormalStartedAt
			base["finished_at"] = context.FormalFinishedAt
			base["commit"] = strings.Repeat("a", 40)
			base["release_config_hash"] = "v1:" + digest
			base["runtime_config_hash"] = "v1:" + strings.Repeat("f", 64)
			base["formal_config_sha256"] = strings.Repeat("9", 64)
			base["execution_environment"] = "formal-chaos"
			base["region"] = "us-east-1"
			base["candidate_gateway_image"] = cliGatewayRef
			base["candidate_migrate_image"] = cliMigrateRef
			base["candidate_worker_image"] = cliWorkerRef
			base["topology_sha256"] = strings.Repeat("a", 64)
			base["formal_manifest_sha256"] = strings.Repeat("b", 64)
			base["production_manifest_sha256"] = strings.Repeat("b", 64)
			base["duration_seconds"] = contract.DurationSeconds
			base["sample_count"] = contract.SampleCount
			base["chaos_case"] = contract.ChaosCase
			base["containers"] = map[string]string{
				"postgres": strings.Repeat("1", 64), "toxiproxy": strings.Repeat("2", 64), "gateway": strings.Repeat("3", 64), "worker": strings.Repeat("4", 64), "oidc": strings.Repeat("5", 64), "voice": strings.Repeat("6", 64), "deepseek": strings.Repeat("7", 64),
			}
		} else if strings.HasSuffix(name, "_sbom") || strings.HasSuffix(name, "_provenance") {
			target := strings.TrimSuffix(strings.TrimSuffix(name, "_sbom"), "_provenance")
			refs := map[string]string{"gateway": cliGatewayRef, "worker": cliWorkerRef, "migrate": cliMigrateRef}
			base["subject"] = refs[target]
			base["source_sha"] = strings.Repeat("a", 40)
			base["document_sha256"] = digest
			if strings.HasSuffix(name, "_provenance") {
				base["platforms"] = []string{"linux/amd64", "linux/arm64"}
			}
		} else {
			t.Fatalf("unknown CLI artifact %q", name)
		}
	}
	document, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	return append(document, '\n')
}

func cliReleaseInputArgs() []string {
	values := []string{
		"candidate_gateway_image=" + cliGatewayRef,
		"candidate_migrate_image=" + cliMigrateRef,
		"candidate_worker_image=" + cliWorkerRef,
		"deployment_region=us-east-1",
		"egress_proxy_cidr=198.51.100.0/24",
		"manifest_sha256=" + strings.Repeat("b", 64),
		"oidc_issuer=https://issuer.example.com",
		"optional_provider_result=not-requested",
		"production_host=live.example.com",
		"production_tls_secret=live-production-tls",
		"run_provider_tests=false",
		"trusted_proxy_cidr=203.0.113.0/24",
		"voice_url=wss://voice.example.com/v1/realtime",
	}
	arguments := make([]string, 0, len(values)*2)
	for _, value := range values {
		arguments = append(arguments, "--release-input", value)
	}
	return arguments
}

func TestStatementHashRejectsIncompleteBundle(t *testing.T) {
	root := realCLITempDir(t)
	contents := []byte("{}\n")
	digest := sha256.Sum256(contents)
	evidence := releasegate.Evidence{
		SchemaVersion: releasegate.SchemaVersion,
		Commit:        strings.Repeat("a", 40), ConfigHash: "v1:" + strings.Repeat("a", 64),
		GeneratedAt: time.Now().UTC(), ReleaseInputs: []releasegate.ReleaseInput{{Name: "deployment_region", Value: "us-east-1"}},
		Gates: []releasegate.GateEvidence{{
			GateID:   releasegate.PrivacyAttestationGateID,
			Producer: releasegate.ExpectedProducers[releasegate.PrivacyAttestationGateID],
			Command:  []string{"true"}, StartedAt: time.Now().UTC(), FinishedAt: time.Now().UTC(),
			ArtifactPath: "gates/privacy-no-audio-persistence.summary.json", ArtifactSHA256: hex.EncodeToString(digest[:]),
		}},
	}
	document, err := releasegate.CanonicalJSON(evidence)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "evidence.json")
	if err := os.WriteFile(path, document, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := statementHash([]string{"--evidence", path, "--signer", "release-operator", "--signed-at", "2026-08-09T12:00:00Z"}); err == nil {
		t.Fatal("incomplete bundle produced a signable digest")
	}
}

func TestRecordWritesBoundedContentFreeSummary(t *testing.T) {
	root := realCLITempDir(t)
	err := record([]string{
		"--gate-id", "simulator-voice",
		"--producer", releasegate.ExpectedProducers["simulator-voice"],
		"--artifact-root", root,
		"--artifact", "gates/simulator-voice.summary.json",
		"--evidence-out", filepath.Join(root, "gates/simulator-voice.json"),
		"--", "sh", "-c", "printf 'ordinary diagnostic output'",
	})
	if err != nil {
		t.Fatal(err)
	}
	document, err := os.ReadFile(filepath.Join(root, "gates/simulator-voice.summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(document), "ordinary diagnostic output") {
		t.Fatal("raw producer output was retained")
	}
	if !strings.Contains(string(document), releasegate.SummarySchemaVersion) {
		t.Fatal("schema-validated summary was not written")
	}
}

func TestRecordRejectsProhibitedCommandArgumentsBeforeWritingEvidence(t *testing.T) {
	root := realCLITempDir(t)
	err := record([]string{
		"--gate-id", "simulator-voice",
		"--producer", releasegate.ExpectedProducers["simulator-voice"],
		"--artifact-root", root,
		"--artifact", "gates/simulator-voice.summary.json",
		"--evidence-out", filepath.Join(root, "gates/simulator-voice.json"),
		"--", "printf", `"transcript":"forbidden"`,
	})
	if err == nil || !strings.Contains(err.Error(), "command") {
		t.Fatalf("record() error = %v, want prohibited command rejection", err)
	}
	if _, err := os.Stat(filepath.Join(root, "gates/simulator-voice.json")); !os.IsNotExist(err) {
		t.Fatalf("prohibited command wrote gate evidence: %v", err)
	}
}

func TestRecordRejectsPreexistingArtifactSymlink(t *testing.T) {
	root := realCLITempDir(t)
	if err := os.Mkdir(filepath.Join(root, "gates"), 0o700); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(realCLITempDir(t), "outside")
	if err := os.WriteFile(outside, []byte("unchanged"), 0o600); err != nil {
		t.Fatal(err)
	}
	artifact := filepath.Join(root, "gates/simulator-voice.summary.json")
	if err := os.Symlink(outside, artifact); err != nil {
		t.Fatal(err)
	}
	err := record([]string{
		"--gate-id", "simulator-voice", "--producer", releasegate.ExpectedProducers["simulator-voice"],
		"--artifact-root", root, "--artifact", "gates/simulator-voice.summary.json",
		"--evidence-out", filepath.Join(root, "gates/simulator-voice.json"), "--", "true",
	})
	if err == nil {
		t.Fatal("preexisting artifact symlink was accepted")
	}
	contents, err := os.ReadFile(outside)
	if err != nil || string(contents) != "unchanged" {
		t.Fatalf("outside file changed: %q, %v", contents, err)
	}
}

func TestRecordTerminatesProducerOnOutputOverflow(t *testing.T) {
	root := realCLITempDir(t)
	err := record([]string{
		"--gate-id", "simulator-voice", "--producer", releasegate.ExpectedProducers["simulator-voice"],
		"--artifact-root", root, "--artifact", "gates/simulator-voice.summary.json",
		"--evidence-out", filepath.Join(root, "gates/simulator-voice.json"),
		"--", "sh", "-c", "yes x | head -c 2000000",
	})
	if err == nil || !strings.Contains(err.Error(), "output limit") {
		t.Fatalf("record() error = %v, want output limit", err)
	}
}

func realCLITempDir(t *testing.T) string {
	t.Helper()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return directory
}
