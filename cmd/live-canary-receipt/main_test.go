package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/releasegate"
)

func TestExecutePackagesAndRepeatedlyVerifiesExactPlatformReceipt(t *testing.T) {
	root := t.TempDir()
	seed := sha256.Sum256([]byte("candidate-canary-cli-test-only-seed"))
	privateKey := ed25519.NewKeyFromSeed(seed[:])
	publicKey := privateKey.Public().(ed25519.PublicKey)
	publicKeyDigest := sha256.Sum256(publicKey)
	now := time.Date(2026, 8, 10, 2, 0, 0, 0, time.UTC)
	expected := releasegate.CandidateCanaryExpected{
		Commit: strings.Repeat("a", 40), ConfigHash: "v1:" + strings.Repeat("b", 64),
		ManifestSHA256: strings.Repeat("c", 64), HaltPolicySHA256: strings.Repeat("d", 64),
		PublicKeySHA256: fmt.Sprintf("%x", publicKeyDigest), Region: "us-east-1",
		GatewayImage: "ghcr.io/chuanmingliu/openai-gpt-live/gateway@sha256:" + strings.Repeat("1", 64),
		WorkerImage:  "ghcr.io/chuanmingliu/openai-gpt-live/worker@sha256:" + strings.Repeat("2", 64),
		MigrateImage: "ghcr.io/chuanmingliu/openai-gpt-live/migrate@sha256:" + strings.Repeat("3", 64),
	}
	receipt := releasegate.CandidateCanaryReceipt{
		SchemaVersion: releasegate.CandidateCanaryReceiptSchemaVersion, Authority: releasegate.CandidateCanaryAuthority,
		Status: "passed", CleanupStatus: "rolled_back", DeploymentID: "canary-cli-123",
		Commit: expected.Commit, ConfigHash: expected.ConfigHash, ManifestSHA256: expected.ManifestSHA256,
		HaltPolicySHA256: expected.HaltPolicySHA256, Region: expected.Region,
		GatewayImage: expected.GatewayImage, WorkerImage: expected.WorkerImage, MigrateImage: expected.MigrateImage,
		ObservationStartedAt: now.Add(-31 * time.Minute), ObservationFinishedAt: now.Add(-time.Minute),
		CleanupCompletedAt: now, ObservationSeconds: 1800, ProbesTotal: 30,
	}
	receiptDocument, err := releasegate.CanonicalJSON(receipt)
	if err != nil {
		t.Fatal(err)
	}
	receiptPath := filepath.Join(root, "receipt.json")
	signaturePath := filepath.Join(root, "receipt.sig")
	publicKeyPath := filepath.Join(root, "receipt.pub")
	envelopePath := filepath.Join(root, "envelope.json")
	for path, document := range map[string][]byte{
		receiptPath:   receiptDocument,
		signaturePath: []byte(base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, receiptDocument))),
		publicKeyPath: []byte(base64.StdEncoding.EncodeToString(publicKey)),
	} {
		if err := os.WriteFile(path, document, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	common := []string{
		"--public-key", publicKeyPath, "--public-key-sha256", expected.PublicKeySHA256,
		"--commit", expected.Commit, "--config-hash", expected.ConfigHash,
		"--manifest-sha256", expected.ManifestSHA256, "--halt-policy-sha256", expected.HaltPolicySHA256,
		"--region", expected.Region, "--gateway-image", expected.GatewayImage,
		"--worker-image", expected.WorkerImage, "--migrate-image", expected.MigrateImage,
	}
	var stderr bytes.Buffer
	pack := append([]string{"--receipt", receiptPath, "--signature", signaturePath, "--envelope-out", envelopePath}, common...)
	if code := execute(pack, now, &stderr); code != 0 {
		t.Fatalf("pack exit=%d stderr=%q", code, stderr.String())
	}
	stderr.Reset()
	verify := append([]string{"--envelope", envelopePath}, common...)
	if code := execute(verify, now, &stderr); code != 0 {
		t.Fatalf("verify exit=%d stderr=%q", code, stderr.String())
	}
	if code := execute(append([]string{"--envelope", envelopePath}, common[:2]...), now, &stderr); code == 0 {
		t.Fatal("incomplete repeated verification unexpectedly succeeded")
	}
}
