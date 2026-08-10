package releasegate

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

const SchemaVersion = "release-evidence-v2"

const SummarySchemaVersion = "content-free-summary-v1"

const MaximumArtifactBytes int64 = 1 << 20

const MaximumEvidenceBytes int64 = 4 << 20

const MaximumAttestationBytes int64 = 64 << 10

const MaximumKeyBytes int64 = 4 << 10

const PrivacyAttestationGateID = "privacy-no-audio-persistence"

var RequiredGateIDs = []string{
	"simulator-voice",
	"durable-acceptance",
	"correlation",
	"presentation",
	"steer-cancel",
	"epoch-fencing",
	"reconnect",
	"restart-durability",
	"verification-suites",
	"slo-load",
	PrivacyAttestationGateID,
	"deployment-drain",
}

var ExpectedProducers = map[string]string{
	"simulator-voice":        "release-producer-v1/simulator-voice-contract",
	"durable-acceptance":     "release-producer-v1/durable-acceptance-integration",
	"correlation":            "release-producer-v1/correlation-integration",
	"presentation":           "release-producer-v1/presentation-integration",
	"steer-cancel":           "release-producer-v1/steer-cancel-integration",
	"epoch-fencing":          "release-producer-v1/epoch-fencing-race",
	"reconnect":              "release-producer-v1/reconnect-integration",
	"restart-durability":     "release-producer-v1/restart-durability-integration",
	"verification-suites":    "release-producer-v1/verification-suites",
	"slo-load":               "release-producer-v1/formal-slo-load",
	PrivacyAttestationGateID: "release-producer-v1/privacy-scan",
	"deployment-drain":       "release-producer-v1/deployment-drain",
}

var expectedCommands = map[string][]string{
	"simulator-voice": {
		"sh", "-c", `test -n "${S2S_REALTIME_URL:-}" && go test -json -count=1 -tags=contract ./test/contract/...`,
	},
	"durable-acceptance": {
		"sh", "-c", `test -n "${TEST_DATABASE_URL:-}" && go test -json -count=1 -tags=integration -run "^TestVoiceContinuesDuringDurableHandoff$" ./test/integration/...`,
	},
	"correlation": {
		"sh", "-c", `test -n "${TEST_DATABASE_URL:-}" && go test -json -count=1 -tags=integration -run "^(TestTwoLiveWebSocketsKeepPCMEventsAndResponseEpochsIsolated|TestDuplicateCompletionHasOneExecutorEffectAndOneClientTerminal)$" ./test/integration/...`,
	},
	"presentation": {
		"sh", "-c", `test -n "${TEST_DATABASE_URL:-}" && go test -json -count=1 -tags=integration -run "^(TestDroppedListenerNotificationRepairsThroughPollAndCursor|TestUnsafeWorkerResultSuppressesDurableClientAndProviderPresentation|TestWorkerCompletionDuringSpeechRemainsVisualOnly)$" ./test/integration/...`,
	},
	"steer-cancel": {
		"sh", "-c", `test -n "${TEST_DATABASE_URL:-}" && go test -json -count=1 -tags=integration -run "^(TestProviderSteerAndCancelTraverseDurableWorkerLifecycle|TestPostgresServicePersistsSteeringAndCancelRequest|TestPostgresLeaseExpiryFencesOldOwner)$" ./test/integration/...`,
	},
	"epoch-fencing": {
		"go", "test", "-json", "-race", "-count=1", "-run", "^(TestEpochFenceMonotonicallyAdvancesAndAcceptsOnlyCurrent|TestResponseOutputRequiresCurrentEpochAndIncarnation|TestOldEpochProviderErrorCannotTerminateCurrentResponse)$", "./internal/session/...",
	},
	"reconnect": {
		"sh", "-c", `test -n "${TEST_DATABASE_URL:-}" && go test -json -count=1 -tags=integration -run "^TestReconnectDisconnectsProviderWithQueuedAudioAndFencesStaleOutput$" ./test/integration/...`,
	},
	"restart-durability": {
		"sh", "-c", `test -n "${TEST_DATABASE_URL:-}" && go test -json -count=1 -tags=integration -run "^(TestRunnerCancellationRestartRecoversExpiredLease|TestPostgresDeliveryCursorReadsDurableSuffixInOrder)$" ./test/integration/...`,
	},
	"verification-suites": {
		"sh", "-c", `go vet ./... && go test ./... && go test -race ./... && test -n "${TEST_DATABASE_URL:-}" && go test -tags=integration ./test/integration/... && test -n "${S2S_REALTIME_URL:-}" && go test -tags=contract ./test/contract/... && govulncheck ./...`,
	},
	"slo-load": {
		"sh", "-c", `test -n "${LIVE_EVIDENCE_DIR:-}" && ./deploy/scripts/run-load-evidence.sh formal`,
	},
	PrivacyAttestationGateID: {
		"sh", "-c", "go test -json -count=1 ./internal/safety ./internal/telemetry",
	},
	"deployment-drain": {
		"sh", "-c", "make verify-deploy && go test -json -count=1 ./internal/admin ./internal/lifecycle ./deploy/prestop ./deploy/validate",
	},
}

// ExpectedCommand returns a defensive copy of the exact selector authorized
// by the versioned producer contract for gateID.
func ExpectedCommand(gateID string) ([]string, bool) {
	command, ok := expectedCommands[gateID]
	return append([]string(nil), command...), ok
}

type Evidence struct {
	SchemaVersion string              `json:"schema_version"`
	Commit        string              `json:"commit"`
	ConfigHash    string              `json:"config_hash"`
	GeneratedAt   time.Time           `json:"generated_at"`
	ReleaseInputs []ReleaseInput      `json:"release_inputs"`
	Gates         []GateEvidence      `json:"gates"`
	Artifacts     []ArtifactEvidence  `json:"artifacts"`
	Attestations  []SignedAttestation `json:"attestations"`
}

type GateEvidence struct {
	GateID         string    `json:"gate_id"`
	Producer       string    `json:"producer"`
	Command        []string  `json:"command"`
	StartedAt      time.Time `json:"started_at"`
	FinishedAt     time.Time `json:"finished_at"`
	ExitCode       int       `json:"exit_code"`
	ArtifactPath   string    `json:"artifact_path"`
	ArtifactSHA256 string    `json:"artifact_sha256"`
}

type ReleaseInput struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type ArtifactEvidence struct {
	Name   string `json:"name"`
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type ArtifactSummary struct {
	SchemaVersion string `json:"schema_version"`
	GateID        string `json:"gate_id"`
	Producer      string `json:"producer"`
	Status        string `json:"status"`
	OutputBytes   int64  `json:"output_bytes"`
}

type SignedAttestation struct {
	GateID          string    `json:"gate_id"`
	Signer          string    `json:"signer"`
	StatementSHA256 string    `json:"statement_sha256"`
	Signature       string    `json:"signature"`
	SignedAt        time.Time `json:"signed_at"`
}

func CanonicalJSON(value any) ([]byte, error) {
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		return nil, fmt.Errorf("encode canonical JSON: %w", err)
	}
	return output.Bytes(), nil
}

func DecodeEvidence(document []byte) (Evidence, error) {
	var evidence Evidence
	if err := rejectDuplicateKeys(document); err != nil {
		return Evidence{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&evidence); err != nil {
		return Evidence{}, fmt.Errorf("decode evidence: %w", err)
	}
	if err := requireEOF(decoder); err != nil {
		return Evidence{}, err
	}
	return evidence, nil
}

func DecodeGateEvidence(document []byte) (GateEvidence, error) {
	var evidence GateEvidence
	if err := rejectDuplicateKeys(document); err != nil {
		return GateEvidence{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&evidence); err != nil {
		return GateEvidence{}, fmt.Errorf("decode gate evidence: %w", err)
	}
	if err := requireEOF(decoder); err != nil {
		return GateEvidence{}, err
	}
	return evidence, nil
}

func DecodeAttestation(document []byte) (SignedAttestation, error) {
	var attestation SignedAttestation
	if err := rejectDuplicateKeys(document); err != nil {
		return SignedAttestation{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&attestation); err != nil {
		return SignedAttestation{}, fmt.Errorf("decode attestation: %w", err)
	}
	if err := requireEOF(decoder); err != nil {
		return SignedAttestation{}, err
	}
	return attestation, nil
}

func rejectDuplicateKeys(document []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.UseNumber()
	if err := inspectJSONValue(decoder); err != nil {
		return fmt.Errorf("inspect JSON object keys: %w", err)
	}
	if err := requireEOF(decoder); err != nil {
		return err
	}
	return nil
}

func inspectJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, structured := token.(json.Delim)
	if !structured {
		return nil
	}
	switch delimiter {
	case '{':
		keys := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("object key is not a string")
			}
			if _, duplicate := keys[key]; duplicate {
				return fmt.Errorf("object key %q is duplicated", key)
			}
			keys[key] = struct{}{}
			if err := inspectJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return errors.New("object is not terminated")
		}
	case '[':
		for decoder.More() {
			if err := inspectJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return errors.New("array is not terminated")
		}
	default:
		return errors.New("unexpected JSON delimiter")
	}
	return nil
}

func requireEOF(decoder *json.Decoder) error {
	var trailing any
	err := decoder.Decode(&trailing)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("decode trailing JSON: %w", err)
	}
	return errors.New("JSON document contains a trailing value")
}

type attestationStatement struct {
	SchemaVersion string `json:"schema_version"`
	GateID        string `json:"gate_id"`
	BundleSHA256  string `json:"bundle_sha256"`
	Assertion     string `json:"assertion"`
}

type attestationEnvelope struct {
	SchemaVersion string    `json:"schema_version"`
	GateID        string    `json:"gate_id"`
	Signer        string    `json:"signer"`
	SignedAt      time.Time `json:"signed_at"`
	BundleSHA256  string    `json:"bundle_sha256"`
	Assertion     string    `json:"assertion"`
}

const privacyAssertion = "I verified that no raw audio is persisted or logged and that the attached privacy evidence contains no raw audio, transcript, reasoning, tool argument, tool result, credential, or secret content."

func StatementSHA256(evidence Evidence, gateID string) (string, error) {
	if gateID != PrivacyAttestationGateID {
		return "", fmt.Errorf("gate %q cannot receive the release human attestation", gateID)
	}
	found := false
	for _, gate := range evidence.Gates {
		if gate.GateID == gateID {
			if found {
				return "", fmt.Errorf("gate %q is duplicated", gateID)
			}
			found = true
		}
	}
	if !found {
		return "", fmt.Errorf("gate %q is missing", gateID)
	}
	unsigned := evidence
	unsigned.Attestations = nil
	unsignedDocument, err := CanonicalJSON(unsigned)
	if err != nil {
		return "", err
	}
	bundleDigest := sha256.Sum256(unsignedDocument)
	statement := attestationStatement{
		SchemaVersion: SchemaVersion, GateID: gateID,
		BundleSHA256: hex.EncodeToString(bundleDigest[:]), Assertion: privacyAssertion,
	}
	document, err := CanonicalJSON(statement)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(document)
	return hex.EncodeToString(digest[:]), nil
}

// AttestationEnvelopeSHA256 is the exact digest signed by the human release
// authority. It binds the complete unsigned bundle as well as the signer and
// claimed signing time; only the signature bytes themselves are excluded.
func AttestationEnvelopeSHA256(evidence Evidence, gateID, signer string, signedAt time.Time) (string, error) {
	if gateID != PrivacyAttestationGateID {
		return "", fmt.Errorf("gate %q cannot receive the release human attestation", gateID)
	}
	if signer == "" || len(signer) > 128 || signedAt.IsZero() || signedAt.Location() != time.UTC {
		return "", errors.New("attestation envelope signer and UTC signed_at are required")
	}
	found := false
	for _, gate := range evidence.Gates {
		if gate.GateID == gateID {
			if found {
				return "", fmt.Errorf("gate %q is duplicated", gateID)
			}
			found = true
		}
	}
	if !found {
		return "", fmt.Errorf("gate %q is missing", gateID)
	}
	unsigned := evidence
	unsigned.Attestations = nil
	unsignedDocument, err := CanonicalJSON(unsigned)
	if err != nil {
		return "", err
	}
	bundleDigest := sha256.Sum256(unsignedDocument)
	envelope := attestationEnvelope{
		SchemaVersion: SchemaVersion, GateID: gateID, Signer: signer, SignedAt: signedAt,
		BundleSHA256: hex.EncodeToString(bundleDigest[:]), Assertion: privacyAssertion,
	}
	document, err := CanonicalJSON(envelope)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(document)
	return hex.EncodeToString(digest[:]), nil
}
