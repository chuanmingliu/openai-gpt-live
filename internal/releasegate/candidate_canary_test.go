package releasegate

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestCandidateCanaryReceiptIsSignedContentFreeAndBoundToExactCandidate(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	expected := CandidateCanaryExpected{
		Commit: strings.Repeat("a", 40), ConfigHash: "v1:" + strings.Repeat("b", 64),
		ManifestSHA256: strings.Repeat("c", 64), HaltPolicySHA256: strings.Repeat("d", 64),
		PublicKeySHA256: fmt.Sprintf("%x", sha256.Sum256(publicKey)), Region: "us-east-1",
		GatewayImage: "ghcr.io/chuanmingliu/openai-gpt-live/gateway@sha256:" + strings.Repeat("1", 64),
		WorkerImage:  "ghcr.io/chuanmingliu/openai-gpt-live/worker@sha256:" + strings.Repeat("2", 64),
		MigrateImage: "ghcr.io/chuanmingliu/openai-gpt-live/migrate@sha256:" + strings.Repeat("3", 64),
	}
	started := time.Date(2026, 8, 10, 1, 0, 0, 0, time.UTC)
	receipt := CandidateCanaryReceipt{
		SchemaVersion: CandidateCanaryReceiptSchemaVersion, Authority: CandidateCanaryAuthority,
		Status: "passed", CleanupStatus: "rolled_back", DeploymentID: "canary-run-123",
		Commit: expected.Commit, ConfigHash: expected.ConfigHash, ManifestSHA256: expected.ManifestSHA256,
		HaltPolicySHA256: expected.HaltPolicySHA256, Region: expected.Region,
		GatewayImage: expected.GatewayImage, WorkerImage: expected.WorkerImage, MigrateImage: expected.MigrateImage,
		ObservationStartedAt: started, ObservationFinishedAt: started.Add(30 * time.Minute),
		CleanupCompletedAt: started.Add(31 * time.Minute), ObservationSeconds: 1800, ProbesTotal: 30,
		HaltConditions: CandidateCanaryHaltConditions{},
	}
	document, err := CanonicalJSON(receipt)
	if err != nil {
		t.Fatal(err)
	}
	signature := ed25519.Sign(privateKey, document)
	envelope, err := VerifyCandidateCanaryReceipt(document,
		[]byte(base64.StdEncoding.EncodeToString(signature)+"\n"),
		[]byte(base64.StdEncoding.EncodeToString(publicKey)+"\n"), expected, started.Add(32*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	rawEnvelope, err := CanonicalJSON(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateCandidateCanaryEnvelope(rawEnvelope, expected, started.Add(32*time.Minute)); err != nil {
		t.Fatal(err)
	}
	tamperedEnvelope := envelope
	tamperedSignature, err := base64.StdEncoding.DecodeString(tamperedEnvelope.Signature)
	if err != nil {
		t.Fatal(err)
	}
	tamperedSignature[0] ^= 1
	tamperedEnvelope.Signature = base64.StdEncoding.EncodeToString(tamperedSignature)
	tamperedDocument, err := CanonicalJSON(tamperedEnvelope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateCandidateCanaryEnvelope(tamperedDocument, expected, started.Add(32*time.Minute)); err == nil {
		t.Fatal("self-contained envelope accepted a forged platform signature")
	}
	for _, forbidden := range []string{"audio", "transcript", "prompt", "tool_arguments", "authorization", "bearer", "endpoint", "objective"} {
		if strings.Contains(strings.ToLower(string(rawEnvelope)), forbidden) {
			t.Fatalf("signed receipt contains forbidden content field %q", forbidden)
		}
	}

	for name, mutate := range map[string]func(*CandidateCanaryReceipt){
		"short observation": func(value *CandidateCanaryReceipt) { value.ObservationSeconds = 1799 },
		"wrong candidate": func(value *CandidateCanaryReceipt) {
			value.GatewayImage = strings.Replace(value.GatewayImage, "1111", "ffff", 1)
		},
		"halt observed":   func(value *CandidateCanaryReceipt) { value.HaltConditions.ReadinessFailures = 1 },
		"not rolled back": func(value *CandidateCanaryReceipt) { value.CleanupStatus = "active" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := receipt
			mutate(&changed)
			raw, marshalErr := CanonicalJSON(changed)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			signed := ed25519.Sign(privateKey, raw)
			if _, verifyErr := VerifyCandidateCanaryReceipt(raw, []byte(base64.StdEncoding.EncodeToString(signed)), []byte(base64.StdEncoding.EncodeToString(publicKey)), expected, started.Add(32*time.Minute)); verifyErr == nil {
				t.Fatal("invalid candidate canary receipt passed")
			}
		})
	}
	badSignature := append([]byte(nil), signature...)
	badSignature[0] ^= 1
	if _, err := VerifyCandidateCanaryReceipt(document, []byte(base64.StdEncoding.EncodeToString(badSignature)), []byte(base64.StdEncoding.EncodeToString(publicKey)), expected, started.Add(32*time.Minute)); err == nil {
		t.Fatal("invalid platform signature passed")
	}
}
