package releasegate

import (
	"archive/tar"
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestVerifiedArchiveReplaysTheAuthenticatedImmutableSnapshot(t *testing.T) {
	evidence, options := signedFixture(t)
	var archive bytes.Buffer
	archivedSource, err := WriteVerifiedArchive(&archive, options)
	if err != nil {
		t.Fatalf("WriteVerifiedArchive() error = %v", err)
	}
	if archivedSource.Commit != evidence.Commit {
		t.Fatalf("archived source commit = %q", archivedSource.Commit)
	}

	// The archive must remain independently verifiable even if the source tree
	// changes after the one authenticated descriptor snapshot was captured.
	if err := os.WriteFile(filepath.Join(options.ArtifactRoot, "evidence.json"), []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	archiveOptions := options
	archiveOptions.ArtifactRoot = ""
	archived, err := VerifyArchive(archive.Bytes(), archiveOptions)
	if err != nil {
		t.Fatalf("VerifyArchive() error = %v", err)
	}
	if archived.Commit != evidence.Commit || archived.ConfigHash != evidence.ConfigHash {
		t.Fatalf("archive bindings = %q/%q", archived.Commit, archived.ConfigHash)
	}

	tampered := append([]byte(nil), archive.Bytes()...)
	tampered[len(tampered)/2] ^= 1
	if _, err := VerifyArchive(tampered, archiveOptions); err == nil {
		t.Fatal("tampered immutable archive was accepted")
	}
}

func TestUnsignedArchiveFinalizationChangesOnlyTheAuthorizedAttestation(t *testing.T) {
	evidence, options := signedFixture(t)
	unsigned := evidence
	unsigned.Attestations = nil
	writeBundleInventory(t, unsigned, options.ArtifactRoot)

	var unsignedArchive bytes.Buffer
	archivedUnsigned, err := WriteVerifiedUnsignedArchive(&unsignedArchive, options)
	if err != nil {
		t.Fatalf("WriteVerifiedUnsignedArchive() error = %v", err)
	}
	if !reflect.DeepEqual(archivedUnsigned, unsigned) {
		t.Fatal("unsigned archive changed the reviewed evidence")
	}
	unsignedOptions := options
	unsignedOptions.ArtifactRoot = ""
	if _, err := VerifyUnsignedArchive(unsignedArchive.Bytes(), unsignedOptions); err != nil {
		t.Fatalf("VerifyUnsignedArchive() error = %v", err)
	}

	seed := sha256.Sum256([]byte("gpt-live-release-gate-test-only-ed25519-seed"))
	privateKey := ed25519.NewKeyFromSeed(seed[:])
	signedAt := unsigned.GeneratedAt.Add(time.Second)
	statement, err := AttestationEnvelopeSHA256(unsigned, PrivacyAttestationGateID, "release-operator", signedAt)
	if err != nil {
		t.Fatal(err)
	}
	statementBytes, err := hex.DecodeString(statement)
	if err != nil {
		t.Fatal(err)
	}
	attestation := SignedAttestation{
		GateID: PrivacyAttestationGateID, Signer: "release-operator", StatementSHA256: statement,
		Signature: base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, statementBytes)), SignedAt: signedAt,
	}

	// Finalization consumes only the immutable archive bytes; changing the old
	// source directory after review cannot alter the signed canonical archive.
	if err := os.WriteFile(filepath.Join(options.ArtifactRoot, "evidence.json"), []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var signedArchive bytes.Buffer
	finalEvidence, err := FinalizeVerifiedArchive(&signedArchive, unsignedArchive.Bytes(), attestation, unsignedOptions)
	if err != nil {
		t.Fatalf("FinalizeVerifiedArchive() error = %v", err)
	}
	if len(finalEvidence.Attestations) != 1 || !reflect.DeepEqual(finalEvidence.Attestations[0], attestation) {
		t.Fatal("final archive does not contain exactly the authorized attestation")
	}
	verified, err := VerifyArchive(signedArchive.Bytes(), unsignedOptions)
	if err != nil {
		t.Fatalf("VerifyArchive(final) error = %v", err)
	}
	verified.Attestations = nil
	if !reflect.DeepEqual(verified, unsigned) {
		t.Fatal("finalization changed reviewed evidence outside attestations")
	}

	unsignedSnapshot, err := readCanonicalArchive(unsignedArchive.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	signedSnapshot, err := readCanonicalArchive(signedArchive.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	for name, document := range unsignedSnapshot.files {
		if name == "evidence.json" {
			continue
		}
		if !bytes.Equal(document, signedSnapshot.files[name]) {
			t.Fatalf("finalization changed reviewed archive member %q", name)
		}
	}
}

var testNow = time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)

const (
	testGatewayRef  = "ghcr.io/chuanmingliu/openai-gpt-live/gateway@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	testWorkerRef   = "ghcr.io/chuanmingliu/openai-gpt-live/worker@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	testMigrateRef  = "ghcr.io/chuanmingliu/openai-gpt-live/migrate@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	testPostgresRef = "postgres:16-alpine@sha256:57c72fd2a128e416c7fcc499958864df5301e940bca0a56f58fddf30ffc07777"
)

func TestVerifyAcceptsCompleteCanonicalEvidence(t *testing.T) {
	evidence, options := signedFixture(t)
	if err := Verify(evidence, options); err != nil {
		t.Fatalf("Verify() error = %v", err)
	}

	encoded, err := CanonicalJSON(evidence)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeEvidence(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if err := Verify(decoded, options); err != nil {
		t.Fatalf("canonical round trip failed: %v", err)
	}
}

func TestDecodeEvidenceRejectsUnknownAndNonCanonicalJSON(t *testing.T) {
	for name, document := range map[string]string{
		"unknown field":   `{"schema_version":"release-evidence-v1","unexpected":true}`,
		"duplicate field": `{"schema_version":"release-evidence-v1","schema_version":"release-evidence-v1"}`,
		"trailing value":  `{}` + "\n{}",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeEvidence([]byte(document)); err == nil {
				t.Fatal("malformed evidence was accepted")
			}
		})
	}
}

func TestVerifyRejectsStaleOrFutureEvidence(t *testing.T) {
	for name, generatedAt := range map[string]time.Time{
		"older than 24 hours": testNow.Add(-24*time.Hour - time.Nanosecond),
		"future dated":        testNow.Add(5*time.Minute + time.Nanosecond),
	} {
		t.Run(name, func(t *testing.T) {
			evidence, options := signedFixture(t)
			evidence.GeneratedAt = generatedAt
			if err := Verify(evidence, options); err == nil {
				t.Fatal("invalid evidence time was accepted")
			}
		})
	}
}

func TestVerifyRejectsStaleOrPostGenerationGate(t *testing.T) {
	for name, mutate := range map[string]func(*Evidence){
		"stale start":     func(e *Evidence) { e.Gates[0].StartedAt = testNow.Add(-24*time.Hour - time.Nanosecond) },
		"stale finish":    func(e *Evidence) { e.Gates[0].FinishedAt = testNow.Add(-24*time.Hour - time.Nanosecond) },
		"post generation": func(e *Evidence) { e.Gates[0].FinishedAt = e.GeneratedAt.Add(time.Nanosecond) },
	} {
		t.Run(name, func(t *testing.T) {
			evidence, options := signedFixture(t)
			mutate(&evidence)
			resignFixture(t, &evidence)
			if err := Verify(evidence, options); err == nil {
				t.Fatal("invalid gate execution time was accepted")
			}
		})
	}
}

func TestVerifyRejectsOverlappingOrReverseGateExecution(t *testing.T) {
	for name, mutate := range map[string]func(*Evidence){
		"overlap": func(e *Evidence) { e.Gates[1].StartedAt = e.Gates[0].FinishedAt.Add(-time.Nanosecond) },
		"reverse": func(e *Evidence) {
			e.Gates[1].StartedAt = e.Gates[0].StartedAt.Add(-time.Minute)
			e.Gates[1].FinishedAt = e.Gates[0].StartedAt
		},
	} {
		t.Run(name, func(t *testing.T) {
			evidence, options := signedFixture(t)
			mutate(&evidence)
			resignFixture(t, &evidence)
			writeBundleInventory(t, evidence, options.ArtifactRoot)
			if err := Verify(evidence, options); err == nil || !strings.Contains(err.Error(), "overlap") {
				t.Fatalf("Verify() error = %v, want cross-gate overlap rejection", err)
			}
		})
	}
}

func TestVerifyDefaultsZeroClockOnceForBundleAndAttestation(t *testing.T) {
	evidence, options := signedFixture(t)
	now := time.Now().UTC()
	evidence.GeneratedAt = now.Add(-5 * time.Second)
	for index := range evidence.Gates {
		evidence.Gates[index].StartedAt = now.Add(time.Duration(index-len(evidence.Gates)-20) * time.Second)
		evidence.Gates[index].FinishedAt = now.Add(time.Duration(index-len(evidence.Gates)-19) * time.Second)
	}
	var sloGate GateEvidence
	for _, gate := range evidence.Gates {
		if gate.GateID == "slo-load" {
			sloGate = gate
			break
		}
	}
	for _, descriptor := range append([]ArtifactEvidence(nil), evidence.Artifacts...) {
		document, err := os.ReadFile(filepath.Join(options.ArtifactRoot, filepath.FromSlash(descriptor.Path)))
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case strings.HasPrefix(descriptor.Name, "formal_"):
			var artifact formalArtifact
			mustStrictDecode(t, document, &artifact)
			artifact.StartedAt = sloGate.StartedAt.Add(time.Nanosecond)
			artifact.FinishedAt = sloGate.FinishedAt.Add(-time.Nanosecond)
			replaceFixtureArtifact(t, &evidence, options.ArtifactRoot, descriptor.Name, mustCanonical(t, artifact))
		case descriptor.Name == "migration_rollback":
			var artifact migrationArtifact
			mustStrictDecode(t, document, &artifact)
			artifact.StartedAt = evidence.Gates[len(evidence.Gates)-1].FinishedAt.Add(time.Second)
			artifact.FinishedAt = evidence.GeneratedAt.Add(-time.Nanosecond)
			replaceFixtureArtifact(t, &evidence, options.ArtifactRoot, descriptor.Name, mustCanonical(t, artifact))
		case descriptor.Name == "canary_snapshot":
			var artifact canaryArtifact
			mustStrictDecode(t, document, &artifact)
			artifact.ObservedAt = evidence.GeneratedAt.Add(-time.Second)
			replaceFixtureArtifact(t, &evidence, options.ArtifactRoot, descriptor.Name, mustCanonical(t, artifact))
		}
	}
	resignFixtureAt(t, &evidence, now.Add(-time.Second))
	writeBundleInventory(t, evidence, options.ArtifactRoot)
	options.Now = time.Time{}
	if err := Verify(evidence, options); err != nil {
		t.Fatalf("Verify() with zero clock error = %v", err)
	}
}

func TestValidateArtifactRejectsContentAndUnknownFields(t *testing.T) {
	adversaries := map[string][]byte{
		"whitespace transcript":  []byte(`{"schema_version":"release-artifact-v2","artifact_type":"vulnerability_report","status":"passed","suite":"govulncheck","transcript" : "customer words"}`),
		"nested tool result":     []byte(`{"schema_version":"release-artifact-v2","artifact_type":"vulnerability_report","status":"passed","suite":"govulncheck","details":{"tool_results":["private"]}}`),
		"escaped credential key": []byte(`{"schema_version":"release-artifact-v2","artifact_type":"vulnerability_report","status":"passed","suite":"govulncheck","api_\u006bey":"secret"}`),
		"binary pcm":             append([]byte{0, 1, 2, 3}, make([]byte, 400)...),
	}
	for name, document := range adversaries {
		t.Run(name, func(t *testing.T) {
			if err := ValidateArtifact("vulnerability_report", document); err == nil {
				t.Fatal("hostile artifact was accepted")
			}
		})
	}
}

func TestValidateArtifactRejectsOpaqueCredentialsInEveryFreeString(t *testing.T) {
	for name, fixture := range map[string]struct {
		artifact string
		mutate   func([]byte) []byte
	}{
		"supply subject": {"gateway_sbom", func(document []byte) []byte {
			var artifact supplyArtifact
			mustStrictDecode(t, document, &artifact)
			artifact.Subject = "sk_live_secret@sha256:" + strings.Repeat("a", 64)
			return mustCanonical(t, artifact)
		}},
		"database image": {"migration_rollback", func(document []byte) []byte {
			var artifact migrationArtifact
			mustStrictDecode(t, document, &artifact)
			artifact.DatabaseImage = "opaque_token@sha256:" + strings.Repeat("a", 64)
			return mustCanonical(t, artifact)
		}},
		"formal source": {"formal_unit_race", func(document []byte) []byte {
			var artifact formalArtifact
			mustStrictDecode(t, document, &artifact)
			artifact.SourceName = "sk_live_secret"
			return mustCanonical(t, artifact)
		}},
	} {
		t.Run(name, func(t *testing.T) {
			document := fixture.mutate(validArtifactDocument(t, fixture.artifact))
			if err := ValidateArtifact(fixture.artifact, document); err == nil {
				t.Fatal("opaque credential-bearing artifact string was accepted")
			}
		})
	}
}

func TestCanaryArtifactAcceptsOnlyTwoDistinctLowercaseSHA256ResponseCommitments(t *testing.T) {
	valid := validArtifactDocument(t, "canary_snapshot")
	if err := ValidateArtifact("canary_snapshot", valid); err != nil {
		t.Fatalf("valid committed response identities rejected: %v", err)
	}

	for name, document := range map[string][]byte{
		"raw credential shaped IDs": []byte(`{"schema_version":"release-artifact-v2","artifact_type":"canary_snapshot","status":"passed","observation":"isolated-two-session","retention":"none","route":"wss","sentinels":2,"region":"us-east-1","config_hash":"v1:` + strings.Repeat("a", 64) + `","gateway_image":"` + testGatewayRef + `","response_ids":["github_pat_AAAAAAAAAAAAAAAAAAAA","sk_live_BBBBBBBBBBBBBBBBBBBB"],"post_terminal_observation_ms":500,"observed_at":"2026-08-09T11:50:00Z"}`),
		"duplicate commitment":      bytes.Replace(valid, []byte(`"2ae39b4c87e5f71cb3791f64395ad35d293c37b92c04c220800993a939222c72"`), []byte(`"19cfe453f4352e12750875044e9fd4a1201da7a326a802da451d7914cb8fe94a"`), 1),
		"uppercase commitment":      bytes.Replace(valid, []byte(`19cfe453f4352e12750875044e9fd4a1201da7a326a802da451d7914cb8fe94a`), []byte(`19CFE453F4352E12750875044E9FD4A1201DA7A326A802DA451D7914CB8FE94A`), 1),
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidateArtifact("canary_snapshot", document); err == nil {
				t.Fatal("invalid response identity evidence was accepted")
			}
		})
	}
}

func TestCaptureBundleSnapshotBoundsWideAndDeepEmptyDirectoryTrees(t *testing.T) {
	t.Run("wide", func(t *testing.T) {
		root := realTempDir(t)
		for index := 0; index <= maximumBundleEntries; index++ {
			if err := os.Mkdir(filepath.Join(root, fmt.Sprintf("entry-%04d", index)), 0o700); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := captureBundleSnapshot(root); err == nil || !strings.Contains(err.Error(), "entries") {
			t.Fatalf("captureBundleSnapshot() error = %v, want total-entry rejection", err)
		}
	})

	t.Run("deep", func(t *testing.T) {
		root := realTempDir(t)
		current := root
		for depth := 0; depth <= maximumBundleDepth; depth++ {
			current = filepath.Join(current, fmt.Sprintf("depth-%02d", depth))
			if err := os.Mkdir(current, 0o700); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := captureBundleSnapshot(root); err == nil || !strings.Contains(err.Error(), "depth") {
			t.Fatalf("captureBundleSnapshot() error = %v, want depth rejection", err)
		}
	})

	t.Run("counts parent siblings before descending", func(t *testing.T) {
		root := realTempDir(t)
		first := filepath.Join(root, "a-first")
		if err := os.Mkdir(first, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("missing-target", filepath.Join(root, "z-sibling")); err != nil {
			t.Fatal(err)
		}
		for index := 0; index < maximumBundleEntries-1; index++ {
			if err := os.Mkdir(filepath.Join(first, fmt.Sprintf("entry-%04d", index)), 0o700); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := captureBundleSnapshot(root); err == nil || !strings.Contains(err.Error(), "entries") {
			t.Fatalf("captureBundleSnapshot() error = %v, want global parent/child entry rejection", err)
		}
	})
}

type constrainedDirectoryReader struct {
	t           *testing.T
	maximumRead int
	entries     []os.DirEntry
	err         error
	requests    []int
}

func (reader *constrainedDirectoryReader) ReadDir(count int) ([]os.DirEntry, error) {
	reader.t.Helper()
	reader.requests = append(reader.requests, count)
	if count <= 0 || count > reader.maximumRead {
		reader.t.Fatalf("ReadDir(%d) exceeded the positive pre-allocation bound %d", count, reader.maximumRead)
	}
	return reader.entries, reader.err
}

func (reader *constrainedDirectoryReader) Fd() uintptr {
	reader.t.Helper()
	reader.t.Fatal("snapshot inspected an entry after the bounded directory read should have returned")
	return 0
}

func TestReadDirectoryEntriesRequestsOnlyRemainingBudgetPlusOne(t *testing.T) {
	t.Run("accepts bounded EOF", func(t *testing.T) {
		reader := &constrainedDirectoryReader{t: t, maximumRead: 8, err: io.EOF}
		snapshot := bundleSnapshot{files: make(map[string][]byte), directories: make(map[string]struct{})}
		var total int64
		entries := maximumBundleEntries - 7
		err := snapshotDirectory(reader, "", 0, &snapshot, &total, &entries)
		if err != nil {
			t.Fatalf("snapshotDirectory() error = %v", err)
		}
		if entries != maximumBundleEntries-7 || !reflect.DeepEqual(reader.requests, []int{8}) {
			t.Fatalf("entry count = %d, requests %v; want %d and [8]", entries, reader.requests, maximumBundleEntries-7)
		}
	})

	t.Run("rejects before inspecting entries", func(t *testing.T) {
		reader := &constrainedDirectoryReader{
			t:           t,
			maximumRead: 8,
			entries:     make([]os.DirEntry, 8),
			err:         io.EOF,
		}
		snapshot := bundleSnapshot{files: make(map[string][]byte), directories: make(map[string]struct{})}
		var total int64
		entries := maximumBundleEntries - 7
		if err := snapshotDirectory(reader, "", 0, &snapshot, &total, &entries); err == nil || !strings.Contains(err.Error(), "entries") {
			t.Fatalf("snapshotDirectory() error = %v, want immediate total-entry rejection", err)
		}
		if !reflect.DeepEqual(reader.requests, []int{8}) {
			t.Fatalf("bounded read requests = %v, want [8]", reader.requests)
		}
	})
}

func TestCanonicalArchiveBoundsTotalEntriesAndDirectoryDepth(t *testing.T) {
	writeArchive := func(t *testing.T, names []string) []byte {
		t.Helper()
		var buffer bytes.Buffer
		writer := tar.NewWriter(&buffer)
		for _, name := range names {
			header := &tar.Header{Name: name, Mode: 0o600, ModTime: canonicalArchiveTime, Format: tar.FormatUSTAR}
			if strings.HasSuffix(name, "/") {
				header.Typeflag = tar.TypeDir
				header.Mode = 0o700
			} else {
				header.Typeflag = tar.TypeReg
			}
			if err := writer.WriteHeader(header); err != nil {
				t.Fatal(err)
			}
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		return buffer.Bytes()
	}

	t.Run("mixed wide", func(t *testing.T) {
		names := make([]string, 0, maximumBundleEntries+1)
		for index := 0; index < 513; index++ {
			names = append(names, fmt.Sprintf("d%04d/", index))
		}
		for index := 0; index < 512; index++ {
			names = append(names, fmt.Sprintf("f%04d", index))
		}
		if _, err := readCanonicalArchive(writeArchive(t, names)); err == nil || !strings.Contains(err.Error(), "entries") {
			t.Fatalf("readCanonicalArchive() error = %v, want total-entry rejection", err)
		}
	})

	t.Run("deep", func(t *testing.T) {
		if _, err := readCanonicalArchive(writeArchive(t, []string{"a/", "a/b/", "a/b/c/"})); err == nil || !strings.Contains(err.Error(), "depth") {
			t.Fatalf("readCanonicalArchive() error = %v, want depth rejection", err)
		}
	})
}

func TestMigrationArtifactRequiresThreeExactCandidateExecutions(t *testing.T) {
	valid := validArtifactDocument(t, "migration_rollback")
	if err := ValidateArtifact("migration_rollback", valid); err != nil {
		t.Fatalf("valid directional migration evidence rejected: %v", err)
	}
	var artifact migrationArtifact
	mustStrictDecode(t, valid, &artifact)
	for name, mutate := range map[string]func(*migrationArtifact){
		"missing direction": func(value *migrationArtifact) { value.Executions = value.Executions[:2] },
		"wrong order":       func(value *migrationArtifact) { value.Executions[1].Direction = "up" },
		"nonzero down":      func(value *migrationArtifact) { value.Executions[1].ExitCode = 17 },
	} {
		t.Run(name, func(t *testing.T) {
			changed := artifact
			changed.Executions = append([]migrationExecution(nil), artifact.Executions...)
			mutate(&changed)
			if err := ValidateArtifact("migration_rollback", mustCanonical(t, changed)); err == nil {
				t.Fatal("invalid directional migration evidence was accepted")
			}
		})
	}
}

func TestFormalArtifactsDistinguishReleaseConfigFromObservedChaosRuntime(t *testing.T) {
	valid := validArtifactDocument(t, "formal_capacity_slo_10m")
	if err := ValidateArtifact("formal_capacity_slo_10m", valid); err != nil {
		t.Fatalf("valid formal runtime binding rejected: %v", err)
	}
	var artifact formalArtifact
	mustStrictDecode(t, valid, &artifact)
	for name, mutate := range map[string]func(*formalArtifact){
		"missing release config":         func(value *formalArtifact) { value.ReleaseConfigHash = "" },
		"missing observed runtime":       func(value *formalArtifact) { value.RuntimeConfigHash = "" },
		"missing formal config document": func(value *formalArtifact) { value.FormalConfigSHA256 = "" },
		"claims production execution":    func(value *formalArtifact) { value.ExecutionEnvironment = "production" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := artifact
			mutate(&changed)
			if err := ValidateArtifact("formal_capacity_slo_10m", mustCanonical(t, changed)); err == nil {
				t.Fatal("misattributed formal execution evidence was accepted")
			}
		})
	}
}

func TestVerifyRejectsUndeclaredBundleFiles(t *testing.T) {
	evidence, options := signedFixture(t)
	if err := os.WriteFile(filepath.Join(options.ArtifactRoot, "unlisted.bin"), []byte{0, 1, 2, 3}, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Verify(evidence, options); err == nil || !strings.Contains(err.Error(), "undeclared") {
		t.Fatalf("Verify() error = %v, want undeclared file rejection", err)
	}
}

func TestVerifyRequiresAndAuthenticatesEveryGateRecordFile(t *testing.T) {
	for name, mutate := range map[string]func(string) error{
		"missing":               func(path string) error { return os.Remove(path) },
		"noncanonical mismatch": func(path string) error { return os.WriteFile(path, []byte("{}\n"), 0o600) },
		"prohibited content":    func(path string) error { return os.WriteFile(path, []byte(`{"api_key":"opaque"}`), 0o600) },
	} {
		t.Run(name, func(t *testing.T) {
			evidence, options := signedFixture(t)
			path := filepath.Join(options.ArtifactRoot, "gates", RequiredGateIDs[0]+".json")
			if err := mutate(path); err != nil {
				t.Fatal(err)
			}
			if err := Verify(evidence, options); err == nil {
				t.Fatal("missing or unauthenticated gate record was accepted")
			}
		})
	}
}

func TestVerifyRequiresExactFormalEvidenceInventory(t *testing.T) {
	evidence, options := signedFixture(t)
	retained := evidence.Artifacts[:0]
	for _, artifact := range evidence.Artifacts {
		if strings.HasPrefix(artifact.Name, "formal_") {
			if err := os.Remove(filepath.Join(options.ArtifactRoot, filepath.FromSlash(artifact.Path))); err != nil {
				t.Fatal(err)
			}
			continue
		}
		retained = append(retained, artifact)
	}
	evidence.Artifacts = retained
	resignFixture(t, &evidence)
	writeBundleInventory(t, evidence, options.ArtifactRoot)
	if err := Verify(evidence, options); err == nil {
		t.Fatal("bundle without the exact formal topology/unit/capacity/barge/chaos/soak inventory was accepted")
	}
}

func TestVerifyRejectsContextuallyMismatchedArtifacts(t *testing.T) {
	for name, fixture := range map[string]struct {
		artifact string
		mutate   func([]byte) []byte
	}{
		"manifest digest": {"production_manifest", func(document []byte) []byte {
			var artifact digestArtifact
			mustStrictDecode(t, document, &artifact)
			artifact.SourceSHA256 = strings.Repeat("f", 64)
			return mustCanonical(t, artifact)
		}},
		"supply commit and subject": {"gateway_sbom", func(document []byte) []byte {
			var artifact supplyArtifact
			mustStrictDecode(t, document, &artifact)
			artifact.Subject = "ghcr.io/chuanmingliu/openai-gpt-live/gateway@sha256:" + strings.Repeat("f", 64)
			artifact.SourceSHA = strings.Repeat("b", 40)
			return mustCanonical(t, artifact)
		}},
		"canary runtime": {"canary_snapshot", func(document []byte) []byte {
			var artifact canaryArtifact
			mustStrictDecode(t, document, &artifact)
			artifact.Region = "eu-west-1"
			artifact.ConfigHash = "v1:" + strings.Repeat("f", 64)
			artifact.ObservedAt = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
			return mustCanonical(t, artifact)
		}},
	} {
		t.Run(name, func(t *testing.T) {
			evidence, options := signedFixture(t)
			document := fixture.mutate(validArtifactDocument(t, fixture.artifact))
			replaceFixtureArtifact(t, &evidence, options.ArtifactRoot, fixture.artifact, document)
			resignFixture(t, &evidence)
			writeBundleInventory(t, evidence, options.ArtifactRoot)
			if err := Verify(evidence, options); err == nil {
				t.Fatal("artifact unrelated to signed commit/config/region/candidate inputs was accepted")
			}
		})
	}
}

func TestVerifyRejectsSymlinkedBundleRootComponent(t *testing.T) {
	evidence, options := signedFixture(t)
	aliasParent := realTempDir(t)
	alias := filepath.Join(aliasParent, "bundle-alias")
	if err := os.Symlink(options.ArtifactRoot, alias); err != nil {
		t.Fatal(err)
	}
	options.ArtifactRoot = alias
	if err := Verify(evidence, options); err == nil {
		t.Fatal("symlinked bundle root was followed")
	}
}

func TestVerifyRejectsEvidenceMutationAfterCallerDecode(t *testing.T) {
	evidence, options := signedFixture(t)
	mutated := evidence
	mutated.Attestations = append([]SignedAttestation(nil), evidence.Attestations...)
	mutated.Attestations[0].Signature = base64.StdEncoding.EncodeToString(make([]byte, ed25519.SignatureSize))
	document, err := CanonicalJSON(mutated)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(options.ArtifactRoot, "evidence.json"), document, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Verify(evidence, options); err == nil || !strings.Contains(err.Error(), "canonical verified evidence") {
		t.Fatalf("Verify() error = %v, want caller/file snapshot mutation rejection", err)
	}
}

func TestAttestationSignatureBindsSignedAt(t *testing.T) {
	evidence, options := signedFixture(t)
	evidence.Attestations[0].SignedAt = evidence.Attestations[0].SignedAt.Add(time.Second)
	writeBundleInventory(t, evidence, options.ArtifactRoot)
	if err := Verify(evidence, options); err == nil || !strings.Contains(err.Error(), "signature") {
		t.Fatalf("Verify() error = %v, want signed_at signature rejection", err)
	}
}

func TestStatementHashBindsEveryGateAndReleaseInput(t *testing.T) {
	evidence, _ := signedFixture(t)
	want, err := StatementSHA256(evidence, PrivacyAttestationGateID)
	if err != nil {
		t.Fatal(err)
	}
	mutations := map[string]func(*Evidence){
		"other gate producer": func(e *Evidence) { e.Gates[0].Producer += "-substituted" },
		"other gate command":  func(e *Evidence) { e.Gates[0].Command[0] = "true" },
		"other gate time":     func(e *Evidence) { e.Gates[0].StartedAt = e.Gates[0].StartedAt.Add(time.Second) },
		"other gate artifact": func(e *Evidence) { e.Gates[0].ArtifactSHA256 = strings.Repeat("b", 64) },
		"generation time":     func(e *Evidence) { e.GeneratedAt = e.GeneratedAt.Add(time.Second) },
		"release input":       func(e *Evidence) { e.ReleaseInputs[0].Value += "-changed" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := evidence
			changed.Gates = append([]GateEvidence(nil), evidence.Gates...)
			changed.ReleaseInputs = append([]ReleaseInput(nil), evidence.ReleaseInputs...)
			changed.Gates[0].Command = append([]string(nil), evidence.Gates[0].Command...)
			mutate(&changed)
			got, err := StatementSHA256(changed, PrivacyAttestationGateID)
			if err != nil {
				t.Fatal(err)
			}
			if got == want {
				t.Fatal("whole-bundle statement digest did not change")
			}
		})
	}
}

func TestVerifyRejectsUnexpectedProducer(t *testing.T) {
	evidence, options := signedFixture(t)
	evidence.Gates[0].Producer = "release-producer-v1/substituted"
	resignFixture(t, &evidence)
	if err := Verify(evidence, options); err == nil || !strings.Contains(err.Error(), "producer") {
		t.Fatalf("Verify() error = %v, want producer rejection", err)
	}
}

func TestVerifyRejectsUnexpectedProducerCommand(t *testing.T) {
	evidence, options := signedFixture(t)
	evidence.Gates[0].Command = []string{"true"}
	resignFixture(t, &evidence)
	if err := Verify(evidence, options); err == nil || !strings.Contains(err.Error(), "command") {
		t.Fatalf("Verify() error = %v, want command-contract rejection", err)
	}
}

func TestVerifyRejectsOversizedArtifact(t *testing.T) {
	evidence, options := signedFixture(t)
	path := filepath.Join(options.ArtifactRoot, filepath.FromSlash(evidence.Gates[0].ArtifactPath))
	tooLarge := make([]byte, MaximumArtifactBytes+1)
	if err := os.WriteFile(path, tooLarge, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(tooLarge)
	evidence.Gates[0].ArtifactSHA256 = hex.EncodeToString(digest[:])
	resignFixture(t, &evidence)
	if err := Verify(evidence, options); err == nil || !strings.Contains(err.Error(), "size") {
		t.Fatalf("Verify() error = %v, want size rejection", err)
	}
}

func TestVerifyRejectsArtifactTampering(t *testing.T) {
	evidence, options := signedFixture(t)
	path := filepath.Join(options.ArtifactRoot, filepath.FromSlash(evidence.Gates[0].ArtifactPath))
	if err := os.WriteFile(path, []byte("tampered\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Verify(evidence, options); err == nil || !strings.Contains(err.Error(), "sha256") {
		t.Fatalf("Verify() error = %v, want sha256 rejection", err)
	}
}

func TestVerifyRejectsWrongCommitOrConfig(t *testing.T) {
	for name, mutate := range map[string]func(*VerifyOptions){
		"commit": func(options *VerifyOptions) { options.Commit = strings.Repeat("b", 40) },
		"config": func(options *VerifyOptions) { options.ConfigHash = "v1:" + strings.Repeat("b", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			evidence, options := signedFixture(t)
			mutate(&options)
			if err := Verify(evidence, options); err == nil {
				t.Fatal("binding mismatch was accepted")
			}
		})
	}
}

func TestVerifyRejectsDuplicateMissingOrFailedGate(t *testing.T) {
	for name, mutate := range map[string]func(*Evidence){
		"duplicate": func(evidence *Evidence) { evidence.Gates[1].GateID = evidence.Gates[0].GateID },
		"missing":   func(evidence *Evidence) { evidence.Gates = evidence.Gates[:len(evidence.Gates)-1] },
		"nonzero":   func(evidence *Evidence) { evidence.Gates[0].ExitCode = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			evidence, options := signedFixture(t)
			mutate(&evidence)
			if err := Verify(evidence, options); err == nil {
				t.Fatal("incomplete gate evidence was accepted")
			}
		})
	}
}

func TestVerifyRejectsUnsafeArtifactPaths(t *testing.T) {
	for name, path := range map[string]string{
		"absolute":  "/tmp/outside.log",
		"traversal": "../outside.log",
		"backslash": `gates\\outside.log`,
	} {
		t.Run(name, func(t *testing.T) {
			evidence, options := signedFixture(t)
			evidence.Gates[0].ArtifactPath = path
			if err := Verify(evidence, options); err == nil {
				t.Fatal("unsafe artifact path was accepted")
			}
		})
	}
}

func TestVerifyRejectsMissingUnknownOrForgedHumanAttestation(t *testing.T) {
	for name, mutate := range map[string]func(*Evidence, *VerifyOptions){
		"missing": func(evidence *Evidence, _ *VerifyOptions) { evidence.Attestations = nil },
		"unknown signer": func(evidence *Evidence, options *VerifyOptions) {
			options.AttestationKeys = map[string]ed25519.PublicKey{}
		},
		"forged signature": func(evidence *Evidence, _ *VerifyOptions) {
			evidence.Attestations[0].Signature = base64.StdEncoding.EncodeToString(make([]byte, ed25519.SignatureSize))
		},
		"wrong statement": func(evidence *Evidence, _ *VerifyOptions) {
			evidence.Attestations[0].StatementSHA256 = strings.Repeat("0", 64)
		},
		"predates evidence": func(evidence *Evidence, _ *VerifyOptions) {
			evidence.Attestations[0].SignedAt = evidence.GeneratedAt.Add(-time.Nanosecond)
		},
	} {
		t.Run(name, func(t *testing.T) {
			evidence, options := signedFixture(t)
			mutate(&evidence, &options)
			if err := Verify(evidence, options); err == nil {
				t.Fatal("invalid human attestation was accepted")
			}
		})
	}
}

func signedFixture(t *testing.T) (Evidence, VerifyOptions) {
	t.Helper()
	root := realTempDir(t)
	evidence := Evidence{
		SchemaVersion: SchemaVersion,
		Commit:        strings.Repeat("a", 40),
		ConfigHash:    "v1:" + strings.Repeat("a", 64),
		GeneratedAt:   testNow,
		ReleaseInputs: []ReleaseInput{
			{Name: "candidate_gateway_image", Value: testGatewayRef},
			{Name: "candidate_migrate_image", Value: testMigrateRef},
			{Name: "candidate_worker_image", Value: testWorkerRef},
			{Name: "deployment_region", Value: "us-east-1"},
			{Name: "egress_proxy_cidr", Value: "198.51.100.0/24"},
			{Name: "manifest_sha256", Value: strings.Repeat("b", 64)},
			{Name: "oidc_issuer", Value: "https://issuer.example.com"},
			{Name: "optional_provider_result", Value: "not-requested"},
			{Name: "production_host", Value: "live.example.com"},
			{Name: "production_tls_secret", Value: "live-production-tls"},
			{Name: "run_provider_tests", Value: "false"},
			{Name: "trusted_proxy_cidr", Value: "203.0.113.0/24"},
			{Name: "voice_url", Value: "wss://voice.example.com/v1/realtime"},
		},
	}
	artifactNames := []string{"canary_snapshot", "dashboard_snapshot", "gateway_provenance", "gateway_sbom", "migrate_provenance", "migrate_sbom", "migration_rollback", "production_manifest", "vulnerability_report", "worker_provenance", "worker_sbom"}
	artifactNames = append(artifactNames, RequiredFormalArtifactNames...)
	sort.Strings(artifactNames)
	for _, name := range artifactNames {
		contents := validArtifactDocument(t, name)
		relative := "artifacts/" + name + ".json"
		path := filepath.Join(root, filepath.FromSlash(relative))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, contents, 0o600); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(contents)
		evidence.Artifacts = append(evidence.Artifacts, ArtifactEvidence{Name: name, Path: relative, SHA256: hex.EncodeToString(digest[:])})
	}
	for index, gateID := range RequiredGateIDs {
		producer := ExpectedProducers[gateID]
		command, ok := ExpectedCommand(gateID)
		if !ok {
			t.Fatalf("missing expected command for %q", gateID)
		}
		contents := []byte(fmt.Sprintf("{\n  \"schema_version\": \"%s\",\n  \"gate_id\": \"%s\",\n  \"producer\": \"%s\",\n  \"status\": \"passed\",\n  \"output_bytes\": 0\n}\n", SummarySchemaVersion, gateID, producer))
		relative := filepath.ToSlash(filepath.Join("gates", gateID+".summary.json"))
		path := filepath.Join(root, filepath.FromSlash(relative))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, contents, 0o600); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(contents)
		evidence.Gates = append(evidence.Gates, GateEvidence{
			GateID: gateID, Producer: producer, Command: command,
			StartedAt:  testNow.Add(time.Duration(index-len(RequiredGateIDs)-3) * time.Minute),
			FinishedAt: testNow.Add(time.Duration(index-len(RequiredGateIDs)-2) * time.Minute),
			ExitCode:   0, ArtifactPath: relative, ArtifactSHA256: hex.EncodeToString(digest[:]),
		})
	}
	resignFixture(t, &evidence)
	seed := sha256.Sum256([]byte("gpt-live-release-gate-test-only-ed25519-seed"))
	privateKey := ed25519.NewKeyFromSeed(seed[:])
	options := VerifyOptions{
		Now: testNow, Commit: evidence.Commit, ConfigHash: evidence.ConfigHash, ArtifactRoot: root,
		AttestationKeys: map[string]ed25519.PublicKey{"release-operator": privateKey.Public().(ed25519.PublicKey)},
	}
	writeBundleInventory(t, evidence, root)
	return evidence, options
}

func resignFixture(t *testing.T, evidence *Evidence) {
	resignFixtureAt(t, evidence, testNow)
}

func resignFixtureAt(t *testing.T, evidence *Evidence, signedAt time.Time) {
	t.Helper()
	seed := sha256.Sum256([]byte("gpt-live-release-gate-test-only-ed25519-seed"))
	privateKey := ed25519.NewKeyFromSeed(seed[:])
	statement, err := AttestationEnvelopeSHA256(*evidence, PrivacyAttestationGateID, "release-operator", signedAt)
	if err != nil {
		t.Fatal(err)
	}
	statementBytes, err := hex.DecodeString(statement)
	if err != nil {
		t.Fatal(err)
	}
	evidence.Attestations = []SignedAttestation{{
		GateID: PrivacyAttestationGateID, Signer: "release-operator", StatementSHA256: statement,
		Signature: base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, statementBytes)), SignedAt: signedAt,
	}}
}

func validArtifactDocument(t *testing.T, name string) []byte {
	t.Helper()
	documents := map[string]string{
		"vulnerability_report": `{"schema_version":"release-artifact-v2","artifact_type":"vulnerability_report","status":"passed","suite":"govulncheck"}`,
		"migration_rollback":   `{"schema_version":"release-artifact-v2","artifact_type":"migration_rollback","status":"passed","migration":"up-down-up","candidate_image":"` + testMigrateRef + `","executions":[{"direction":"up","container_id":"` + strings.Repeat("1", 64) + `","exit_code":0},{"direction":"down","container_id":"` + strings.Repeat("3", 64) + `","exit_code":0},{"direction":"up","container_id":"` + strings.Repeat("4", 64) + `","exit_code":0}],"database_image":"` + testPostgresRef + `","database_container_id":"` + strings.Repeat("2", 64) + `","database_server_major":16,"started_at":"2026-08-09T11:57:10Z","finished_at":"2026-08-09T11:58:00Z"}`,
		"dashboard_snapshot":   `{"schema_version":"release-artifact-v2","artifact_type":"dashboard_snapshot","status":"passed","source_sha256":"` + strings.Repeat("a", 64) + `"}`,
		"canary_snapshot":      `{"schema_version":"release-artifact-v2","artifact_type":"canary_snapshot","status":"passed","observation":"isolated-two-session","retention":"none","route":"wss","sentinels":2,"region":"us-east-1","config_hash":"v1:` + strings.Repeat("a", 64) + `","gateway_image":"` + testGatewayRef + `","response_id_commitments":["19cfe453f4352e12750875044e9fd4a1201da7a326a802da451d7914cb8fe94a","2ae39b4c87e5f71cb3791f64395ad35d293c37b92c04c220800993a939222c72"],"post_terminal_observation_ms":500,"observed_at":"2026-08-09T11:50:00Z"}`,
		"production_manifest":  `{"schema_version":"release-artifact-v2","artifact_type":"production_manifest","status":"passed","source_sha256":"` + strings.Repeat("b", 64) + `"}`,
	}
	for target, subject := range map[string]string{"gateway": testGatewayRef, "worker": testWorkerRef, "migrate": testMigrateRef} {
		documents[target+"_sbom"] = `{"schema_version":"release-artifact-v2","artifact_type":"` + target + `_sbom","status":"passed","subject":"` + subject + `","source_sha":"` + strings.Repeat("a", 40) + `","document_sha256":"` + strings.Repeat("d", 64) + `"}`
		documents[target+"_provenance"] = `{"schema_version":"release-artifact-v2","artifact_type":"` + target + `_provenance","status":"passed","subject":"` + subject + `","source_sha":"` + strings.Repeat("a", 40) + `","document_sha256":"` + strings.Repeat("e", 64) + `","platforms":["linux/amd64","linux/arm64"]}`
	}
	if contract, ok := formalContracts[name]; ok {
		sourceDigest := strings.Repeat("c", 64)
		if name == "formal_topology_identity" {
			sourceDigest = strings.Repeat("a", 64)
		} else if name == "formal_manifest" {
			sourceDigest = strings.Repeat("b", 64)
		}
		artifact := formalArtifact{
			SchemaVersion: "release-artifact-v2", ArtifactType: name, Status: "passed",
			SourceName: contract.SourceName, SourceSHA256: sourceDigest, Selector: contract.Selector,
			StartedAt: time.Date(2026, 8, 9, 11, 54, 10, 0, time.UTC), FinishedAt: time.Date(2026, 8, 9, 11, 54, 50, 0, time.UTC),
			Commit: strings.Repeat("a", 40), ReleaseConfigHash: "v1:" + strings.Repeat("a", 64),
			RuntimeConfigHash: "v1:" + strings.Repeat("f", 64), FormalConfigSHA256: strings.Repeat("9", 64), ExecutionEnvironment: "formal-chaos", Region: "us-east-1",
			CandidateGatewayImage: testGatewayRef, CandidateMigrateImage: testMigrateRef, CandidateWorkerImage: testWorkerRef,
			TopologySHA256: strings.Repeat("a", 64), FormalManifestSHA256: strings.Repeat("b", 64), ProductionManifestSHA256: strings.Repeat("b", 64),
			DurationSeconds: contract.DurationSeconds, SampleCount: contract.SampleCount, ChaosCase: contract.ChaosCase,
			Containers: formalContainerIdentities{
				Postgres: strings.Repeat("1", 64), Toxiproxy: strings.Repeat("2", 64), Gateway: strings.Repeat("3", 64), Worker: strings.Repeat("4", 64), OIDC: strings.Repeat("5", 64), Voice: strings.Repeat("6", 64), DeepSeek: strings.Repeat("7", 64),
			},
		}
		return mustCanonical(t, artifact)
	}
	document, ok := documents[name]
	if !ok {
		t.Fatalf("no fixture artifact for %q", name)
	}
	return []byte(document + "\n")
}

func mustStrictDecode(t *testing.T, document []byte, target any) {
	t.Helper()
	if err := strictDecode(document, target); err != nil {
		t.Fatal(err)
	}
}

func mustCanonical(t *testing.T, value any) []byte {
	t.Helper()
	document, err := CanonicalJSON(value)
	if err != nil {
		t.Fatal(err)
	}
	return document
}

func realTempDir(t *testing.T) string {
	t.Helper()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return directory
}

func replaceFixtureArtifact(t *testing.T, evidence *Evidence, root, name string, document []byte) {
	t.Helper()
	for index := range evidence.Artifacts {
		if evidence.Artifacts[index].Name != name {
			continue
		}
		path := filepath.Join(root, filepath.FromSlash(evidence.Artifacts[index].Path))
		if err := os.WriteFile(path, document, 0o600); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(document)
		evidence.Artifacts[index].SHA256 = hex.EncodeToString(digest[:])
		return
	}
	t.Fatalf("fixture artifact %q is missing", name)
}

func writeBundleInventory(t *testing.T, evidence Evidence, root string) {
	t.Helper()
	for _, gate := range evidence.Gates {
		document, err := CanonicalJSON(gate)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "gates", gate.GateID+".json"), document, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	document, err := CanonicalJSON(evidence)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "evidence.json"), document, 0o600); err != nil {
		t.Fatal(err)
	}
}
