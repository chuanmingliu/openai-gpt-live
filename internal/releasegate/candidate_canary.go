package releasegate

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"regexp"
	"strings"
	"time"
)

const (
	CandidateCanaryReceiptSchemaVersion  = "candidate-canary-receipt-v1"
	CandidateCanaryEnvelopeSchemaVersion = "signed-candidate-canary-receipt-v1"
	CandidateCanaryAuthority             = "gpt-live-platform-canary-v1"
	MinimumCandidateCanaryObservation    = 30 * time.Minute
	MaximumCandidateCanaryReceiptBytes   = 16 << 10
)

var candidateCanaryDeploymentPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)

type CandidateCanaryExpected struct {
	Commit, ConfigHash, ManifestSHA256, HaltPolicySHA256, PublicKeySHA256, Region string
	GatewayImage, WorkerImage, MigrateImage                                       string
}

type CandidateCanaryHaltConditions struct {
	ReadinessFailures     int `json:"readiness_failures"`
	SyntheticFailures     int `json:"synthetic_failures"`
	CrossSessionLeaks     int `json:"cross_session_leaks"`
	QueueOverflows        int `json:"queue_overflows"`
	StaleOutputViolations int `json:"stale_output_violations"`
}

type CandidateCanaryReceipt struct {
	SchemaVersion         string                        `json:"schema_version"`
	Authority             string                        `json:"authority"`
	Status                string                        `json:"status"`
	CleanupStatus         string                        `json:"cleanup_status"`
	DeploymentID          string                        `json:"deployment_id"`
	Commit                string                        `json:"commit"`
	ConfigHash            string                        `json:"config_hash"`
	ManifestSHA256        string                        `json:"manifest_sha256"`
	HaltPolicySHA256      string                        `json:"halt_policy_sha256"`
	Region                string                        `json:"region"`
	GatewayImage          string                        `json:"candidate_gateway_image"`
	WorkerImage           string                        `json:"candidate_worker_image"`
	MigrateImage          string                        `json:"candidate_migrate_image"`
	ObservationStartedAt  time.Time                     `json:"observation_started_at"`
	ObservationFinishedAt time.Time                     `json:"observation_finished_at"`
	CleanupCompletedAt    time.Time                     `json:"cleanup_completed_at"`
	ObservationSeconds    int64                         `json:"observation_seconds"`
	ProbesTotal           int64                         `json:"probes_total"`
	HaltConditions        CandidateCanaryHaltConditions `json:"halt_conditions"`
}

type SignedCandidateCanaryReceipt struct {
	SchemaVersion string                 `json:"schema_version"`
	Receipt       CandidateCanaryReceipt `json:"receipt"`
	PublicKey     string                 `json:"public_key"`
	Signature     string                 `json:"signature"`
}

func VerifyCandidateCanaryReceipt(document, encodedSignature, encodedPublicKey []byte, expected CandidateCanaryExpected, now time.Time) (SignedCandidateCanaryReceipt, error) {
	if len(document) == 0 || len(document) > MaximumCandidateCanaryReceiptBytes {
		return SignedCandidateCanaryReceipt{}, errors.New("candidate canary receipt is invalid")
	}
	var receipt CandidateCanaryReceipt
	if err := strictDecode(document, &receipt); err != nil {
		return SignedCandidateCanaryReceipt{}, errors.New("candidate canary receipt is invalid")
	}
	canonical, err := CanonicalJSON(receipt)
	if err != nil || !bytes.Equal(document, canonical) || validateCandidateCanaryReceipt(receipt, expected, now) != nil {
		return SignedCandidateCanaryReceipt{}, errors.New("candidate canary receipt is invalid")
	}
	signature, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(encodedSignature)))
	if err != nil || len(signature) != ed25519.SignatureSize {
		return SignedCandidateCanaryReceipt{}, errors.New("candidate canary signature is invalid")
	}
	publicKey, err := decodeCandidateCanaryPublicKey(strings.TrimSpace(string(encodedPublicKey)), expected.PublicKeySHA256)
	if err != nil || !ed25519.Verify(publicKey, document, signature) {
		return SignedCandidateCanaryReceipt{}, errors.New("candidate canary signature is invalid")
	}
	return SignedCandidateCanaryReceipt{SchemaVersion: CandidateCanaryEnvelopeSchemaVersion, Receipt: receipt, PublicKey: base64.StdEncoding.EncodeToString(publicKey), Signature: base64.StdEncoding.EncodeToString(signature)}, nil
}

func ValidateCandidateCanaryEnvelope(document []byte, expected CandidateCanaryExpected, now time.Time) (SignedCandidateCanaryReceipt, error) {
	if len(document) == 0 || len(document) > MaximumCandidateCanaryReceiptBytes {
		return SignedCandidateCanaryReceipt{}, errors.New("signed candidate canary receipt is invalid")
	}
	var envelope SignedCandidateCanaryReceipt
	if err := strictDecode(document, &envelope); err != nil {
		return SignedCandidateCanaryReceipt{}, errors.New("signed candidate canary receipt is invalid")
	}
	canonical, err := CanonicalJSON(envelope)
	if err != nil || !bytes.Equal(document, canonical) || envelope.SchemaVersion != CandidateCanaryEnvelopeSchemaVersion || validateCandidateCanaryReceipt(envelope.Receipt, expected, now) != nil {
		return SignedCandidateCanaryReceipt{}, errors.New("signed candidate canary receipt is invalid")
	}
	signature, err := base64.StdEncoding.DecodeString(envelope.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize || base64.StdEncoding.EncodeToString(signature) != envelope.Signature {
		return SignedCandidateCanaryReceipt{}, errors.New("signed candidate canary receipt is invalid")
	}
	publicKey, err := decodeCandidateCanaryPublicKey(envelope.PublicKey, expected.PublicKeySHA256)
	if err != nil {
		return SignedCandidateCanaryReceipt{}, errors.New("signed candidate canary receipt is invalid")
	}
	receiptDocument, err := CanonicalJSON(envelope.Receipt)
	if err != nil || !ed25519.Verify(publicKey, receiptDocument, signature) {
		return SignedCandidateCanaryReceipt{}, errors.New("signed candidate canary receipt is invalid")
	}
	return envelope, nil
}

func decodeCandidateCanaryPublicKey(encoded, expectedSHA256 string) (ed25519.PublicKey, error) {
	publicKey, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(publicKey) != ed25519.PublicKeySize || base64.StdEncoding.EncodeToString(publicKey) != encoded || !digestPattern.MatchString(expectedSHA256) {
		return nil, errors.New("candidate canary public key is invalid")
	}
	digest := sha256.Sum256(publicKey)
	if hex.EncodeToString(digest[:]) != expectedSHA256 {
		return nil, errors.New("candidate canary public key is invalid")
	}
	return ed25519.PublicKey(publicKey), nil
}

func validateCandidateCanaryReceipt(receipt CandidateCanaryReceipt, expected CandidateCanaryExpected, now time.Time) error {
	if receipt.SchemaVersion != CandidateCanaryReceiptSchemaVersion || receipt.Authority != CandidateCanaryAuthority ||
		receipt.Status != "passed" || receipt.CleanupStatus != "rolled_back" || !candidateCanaryDeploymentPattern.MatchString(receipt.DeploymentID) ||
		receipt.Commit != expected.Commit || receipt.ConfigHash != expected.ConfigHash || receipt.ManifestSHA256 != expected.ManifestSHA256 ||
		receipt.HaltPolicySHA256 != expected.HaltPolicySHA256 || receipt.Region != expected.Region ||
		receipt.GatewayImage != expected.GatewayImage || receipt.WorkerImage != expected.WorkerImage || receipt.MigrateImage != expected.MigrateImage ||
		!commitPattern.MatchString(receipt.Commit) || !configPattern.MatchString(receipt.ConfigHash) || !digestPattern.MatchString(receipt.ManifestSHA256) ||
		!digestPattern.MatchString(receipt.HaltPolicySHA256) || !regionPattern.MatchString(receipt.Region) ||
		!exactCandidateImage("gateway", receipt.GatewayImage) || !exactCandidateImage("worker", receipt.WorkerImage) || !exactCandidateImage("migrate", receipt.MigrateImage) {
		return errors.New("candidate canary binding is invalid")
	}
	for _, value := range []time.Time{receipt.ObservationStartedAt, receipt.ObservationFinishedAt, receipt.CleanupCompletedAt} {
		if value.IsZero() || value.Location() != time.UTC {
			return errors.New("candidate canary timestamps are invalid")
		}
	}
	duration := receipt.ObservationFinishedAt.Sub(receipt.ObservationStartedAt)
	if duration < MinimumCandidateCanaryObservation || receipt.ObservationSeconds != int64(duration/time.Second) ||
		receipt.ProbesTotal < 1 || receipt.ProbesTotal > 1_000_000 || receipt.CleanupCompletedAt.Before(receipt.ObservationFinishedAt) ||
		receipt.CleanupCompletedAt.After(now.Add(maximumFutureSkew)) || receipt.CleanupCompletedAt.Before(now.Add(-maximumEvidenceAge)) {
		return errors.New("candidate canary observation window is invalid")
	}
	halt := receipt.HaltConditions
	if halt.ReadinessFailures != 0 || halt.SyntheticFailures != 0 || halt.CrossSessionLeaks != 0 || halt.QueueOverflows != 0 || halt.StaleOutputViolations != 0 {
		return errors.New("candidate canary halt condition was observed")
	}
	return nil
}
