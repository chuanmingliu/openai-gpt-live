// Command live-canary-receipt verifies and packages the external platform's
// content-free candidate canary receipt. It never prints receipt fields.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/releasegate"
)

func main() { os.Exit(execute(os.Args[1:], time.Now().UTC(), os.Stderr)) }

func execute(args []string, now time.Time, stderr io.Writer) int {
	flags := flag.NewFlagSet("live-canary-receipt", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	receiptPath := flags.String("receipt", "", "canonical platform receipt")
	signaturePath := flags.String("signature", "", "detached base64 Ed25519 signature")
	publicKeyPath := flags.String("public-key", "", "base64 Ed25519 verification key")
	publicKeySHA := flags.String("public-key-sha256", "", "trusted Ed25519 verification key digest")
	envelopeInputPath := flags.String("envelope", "", "existing signed receipt envelope to reverify")
	envelopePath := flags.String("envelope-out", "", "new signed receipt envelope")
	commit := flags.String("commit", "", "exact candidate source commit")
	configHash := flags.String("config-hash", "", "exact candidate config hash")
	manifestSHA := flags.String("manifest-sha256", "", "exact rendered manifest digest")
	haltPolicySHA := flags.String("halt-policy-sha256", "", "exact halt policy digest")
	region := flags.String("region", "", "exact reviewed region")
	gatewayImage := flags.String("gateway-image", "", "immutable gateway candidate")
	workerImage := flags.String("worker-image", "", "immutable worker candidate")
	migrateImage := flags.String("migrate-image", "", "immutable migrate candidate")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return preflightFailure(stderr)
	}
	packMode := *envelopeInputPath == "" && *receiptPath != "" && *signaturePath != "" && *envelopePath != ""
	verifyMode := *envelopeInputPath != "" && *receiptPath == "" && *signaturePath == "" && *envelopePath == ""
	if (!packMode && !verifyMode) || !filepath.IsAbs(*publicKeyPath) {
		return preflightFailure(stderr)
	}
	publicKey, err := readRegular(*publicKeyPath, 4096)
	if err != nil {
		return preflightFailure(stderr)
	}
	expected := releasegate.CandidateCanaryExpected{Commit: *commit, ConfigHash: *configHash, ManifestSHA256: *manifestSHA,
		HaltPolicySHA256: *haltPolicySHA, PublicKeySHA256: *publicKeySHA, Region: *region, GatewayImage: *gatewayImage, WorkerImage: *workerImage, MigrateImage: *migrateImage}
	if verifyMode {
		if !filepath.IsAbs(*envelopeInputPath) {
			return preflightFailure(stderr)
		}
		document, readErr := readRegular(*envelopeInputPath, releasegate.MaximumCandidateCanaryReceiptBytes)
		if readErr != nil {
			return preflightFailure(stderr)
		}
		envelope, verifyErr := releasegate.ValidateCandidateCanaryEnvelope(document, expected, now)
		if verifyErr != nil || strings.TrimSpace(string(publicKey)) != envelope.PublicKey {
			return preflightFailure(stderr)
		}
		_, _ = io.WriteString(stderr, "candidate canary receipt verified\n")
		return 0
	}
	for _, path := range []string{*receiptPath, *signaturePath, *envelopePath} {
		if !filepath.IsAbs(path) {
			return preflightFailure(stderr)
		}
	}
	if _, err := os.Lstat(*envelopePath); !errors.Is(err, os.ErrNotExist) {
		return preflightFailure(stderr)
	}
	receipt, err := readRegular(*receiptPath, releasegate.MaximumCandidateCanaryReceiptBytes)
	if err != nil {
		return preflightFailure(stderr)
	}
	signature, err := readRegular(*signaturePath, 4096)
	if err != nil {
		return preflightFailure(stderr)
	}
	envelope, err := releasegate.VerifyCandidateCanaryReceipt(receipt, signature, publicKey, expected, now)
	if err != nil {
		return preflightFailure(stderr)
	}
	document, err := releasegate.CanonicalJSON(envelope)
	if err != nil || len(document) > releasegate.MaximumCandidateCanaryReceiptBytes || os.WriteFile(*envelopePath, document, 0o600) != nil {
		return preflightFailure(stderr)
	}
	_, _ = io.WriteString(stderr, "candidate canary receipt verified\n")
	return 0
}

func readRegular(path string, maximum int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximum {
		return nil, errors.New("invalid receipt input")
	}
	return os.ReadFile(path)
}

func preflightFailure(stderr io.Writer) int {
	_, _ = fmt.Fprintln(stderr, "candidate canary receipt verification failed")
	return 1
}
