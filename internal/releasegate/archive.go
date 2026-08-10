package releasegate

import (
	"archive/tar"
	"bytes"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const MaximumArchiveBytes int64 = 66 << 20

var canonicalArchiveTime = time.Unix(0, 0).UTC()

// WriteVerifiedArchive verifies one descriptor-captured bundle snapshot and
// serializes those exact authenticated bytes. It never walks the source path a
// second time, so a later path mutation cannot change the uploaded archive.
func WriteVerifiedArchive(writer io.Writer, options VerifyOptions) (Evidence, error) {
	if writer == nil {
		return Evidence{}, errors.New("archive writer is required")
	}
	options = normalizeVerifyOptions(options)
	if options.ArtifactRoot == "" {
		return Evidence{}, errors.New("artifact root is required")
	}
	snapshot, err := captureBundleSnapshot(options.ArtifactRoot)
	if err != nil {
		return Evidence{}, err
	}
	evidence, err := verifySnapshotDocument(snapshot, options)
	if err != nil {
		return Evidence{}, err
	}
	if err := writeCanonicalArchive(writer, snapshot); err != nil {
		return Evidence{}, err
	}
	return evidence, nil
}

// WriteVerifiedUnsignedArchive captures, fully verifies, and serializes the
// exact unsigned bytes presented for independent human review. The source tree
// is opened and read once; the returned archive is the sole phase-two input.
func WriteVerifiedUnsignedArchive(writer io.Writer, options VerifyOptions) (Evidence, error) {
	if writer == nil {
		return Evidence{}, errors.New("archive writer is required")
	}
	options = normalizeVerifyOptions(options)
	if options.ArtifactRoot == "" {
		return Evidence{}, errors.New("artifact root is required")
	}
	snapshot, err := captureBundleSnapshot(options.ArtifactRoot)
	if err != nil {
		return Evidence{}, err
	}
	evidence, err := verifyUnsignedSnapshotDocument(snapshot, options)
	if err != nil {
		return Evidence{}, err
	}
	if err := writeCanonicalArchive(writer, snapshot); err != nil {
		return Evidence{}, err
	}
	return evidence, nil
}

// VerifyBundle reads every source file once through the descriptor snapshot and
// uses the captured evidence.json bytes for both parsing and verification.
func VerifyBundle(options VerifyOptions) (Evidence, error) {
	options = normalizeVerifyOptions(options)
	if options.ArtifactRoot == "" {
		return Evidence{}, errors.New("artifact root is required")
	}
	snapshot, err := captureBundleSnapshot(options.ArtifactRoot)
	if err != nil {
		return Evidence{}, err
	}
	return verifySnapshotDocument(snapshot, options)
}

// VerifyArchive validates the canonical single-file bundle archive directly,
// reconstructs its in-memory snapshot, and replays the complete evidence and
// attestation verification without extracting or following filesystem paths.
func VerifyArchive(document []byte, options VerifyOptions) (Evidence, error) {
	if int64(len(document)) > MaximumArchiveBytes {
		return Evidence{}, fmt.Errorf("archive exceeds %d-byte limit", MaximumArchiveBytes)
	}
	snapshot, err := readCanonicalArchive(document)
	if err != nil {
		return Evidence{}, err
	}
	return verifySnapshotDocument(snapshot, normalizeVerifyOptions(options))
}

// VerifyUnsignedArchive authenticates a canonical review archive without
// requiring a human attestation. It rejects any archive that already contains
// an attestation or is otherwise incomplete, stale, failed, or noncanonical.
func VerifyUnsignedArchive(document []byte, options VerifyOptions) (Evidence, error) {
	if int64(len(document)) > MaximumArchiveBytes {
		return Evidence{}, fmt.Errorf("archive exceeds %d-byte limit", MaximumArchiveBytes)
	}
	snapshot, err := readCanonicalArchive(document)
	if err != nil {
		return Evidence{}, err
	}
	return verifyUnsignedSnapshotDocument(snapshot, normalizeVerifyOptions(options))
}

// FinalizeVerifiedArchive derives the signed canonical archive solely from the
// already verified unsigned archive bytes plus one authorized attestation. No
// source pathname is reopened and no reviewed member other than evidence.json
// can change.
func FinalizeVerifiedArchive(writer io.Writer, unsignedDocument []byte, attestation SignedAttestation, options VerifyOptions) (Evidence, error) {
	if writer == nil {
		return Evidence{}, errors.New("archive writer is required")
	}
	if int64(len(unsignedDocument)) > MaximumArchiveBytes {
		return Evidence{}, fmt.Errorf("archive exceeds %d-byte limit", MaximumArchiveBytes)
	}
	options = normalizeVerifyOptions(options)
	snapshot, err := readCanonicalArchive(unsignedDocument)
	if err != nil {
		return Evidence{}, err
	}
	evidence, err := verifyUnsignedSnapshotDocument(snapshot, options)
	if err != nil {
		return Evidence{}, err
	}
	evidence.Attestations = []SignedAttestation{attestation}
	evidenceDocument, err := CanonicalJSON(evidence)
	if err != nil {
		return Evidence{}, err
	}
	snapshot.files["evidence.json"] = evidenceDocument
	verified, err := verifySnapshotDocument(snapshot, options)
	if err != nil {
		return Evidence{}, err
	}
	if err := writeCanonicalArchive(writer, snapshot); err != nil {
		return Evidence{}, err
	}
	return verified, nil
}

func verifySnapshotDocument(snapshot bundleSnapshot, options VerifyOptions) (Evidence, error) {
	evidence, err := decodeCanonicalSnapshotEvidence(snapshot)
	if err != nil {
		return Evidence{}, err
	}
	if err := verifyBundleSnapshot(evidence, options, snapshot); err != nil {
		return Evidence{}, err
	}
	if err := verifyAttestations(evidence, options); err != nil {
		return Evidence{}, err
	}
	return evidence, nil
}

func verifyUnsignedSnapshotDocument(snapshot bundleSnapshot, options VerifyOptions) (Evidence, error) {
	evidence, err := decodeCanonicalSnapshotEvidence(snapshot)
	if err != nil {
		return Evidence{}, err
	}
	if len(evidence.Attestations) != 0 {
		return Evidence{}, errors.New("unsigned archive must not contain attestations")
	}
	if err := verifyBundleSnapshot(evidence, options, snapshot); err != nil {
		return Evidence{}, err
	}
	return evidence, nil
}

func decodeCanonicalSnapshotEvidence(snapshot bundleSnapshot) (Evidence, error) {
	evidenceDocument, err := snapshot.read("evidence.json", MaximumEvidenceBytes)
	if err != nil {
		return Evidence{}, fmt.Errorf("read archived evidence.json: %w", err)
	}
	evidence, err := DecodeEvidence(evidenceDocument)
	if err != nil {
		return Evidence{}, err
	}
	canonical, err := CanonicalJSON(evidence)
	if err != nil {
		return Evidence{}, err
	}
	if !bytes.Equal(evidenceDocument, canonical) {
		return Evidence{}, errors.New("archived evidence.json is not canonical JSON")
	}
	return evidence, nil
}

func writeCanonicalArchive(writer io.Writer, snapshot bundleSnapshot) error {
	type entry struct {
		name      string
		directory bool
	}
	entries := make([]entry, 0, len(snapshot.directories)+len(snapshot.files))
	for name := range snapshot.directories {
		entries = append(entries, entry{name: name + "/", directory: true})
	}
	for name := range snapshot.files {
		entries = append(entries, entry{name: name})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })

	archive := tar.NewWriter(writer)
	for _, item := range entries {
		if len(item.name) > 100 {
			return fmt.Errorf("archive path %q exceeds canonical USTAR name limit", item.name)
		}
		header := &tar.Header{
			Name: item.name, Mode: 0o600, ModTime: canonicalArchiveTime,
			AccessTime: time.Time{}, ChangeTime: time.Time{}, Format: tar.FormatUSTAR,
		}
		if item.directory {
			header.Typeflag = tar.TypeDir
			header.Mode = 0o700
		} else {
			header.Typeflag = tar.TypeReg
			header.Size = int64(len(snapshot.files[item.name]))
		}
		if err := archive.WriteHeader(header); err != nil {
			return fmt.Errorf("write canonical archive header %q: %w", item.name, err)
		}
		if !item.directory {
			if _, err := archive.Write(snapshot.files[item.name]); err != nil {
				return fmt.Errorf("write canonical archive file %q: %w", item.name, err)
			}
		}
	}
	if err := archive.Close(); err != nil {
		return fmt.Errorf("close canonical archive: %w", err)
	}
	return nil
}

func readCanonicalArchive(document []byte) (bundleSnapshot, error) {
	snapshot := bundleSnapshot{files: make(map[string][]byte), directories: make(map[string]struct{})}
	underlying := bytes.NewReader(document)
	archive := tar.NewReader(underlying)
	previous := ""
	var total int64
	entryCount := 0
	for {
		header, err := archive.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return bundleSnapshot{}, fmt.Errorf("read canonical archive header: %w", err)
		}
		if header.Name <= previous || len(header.Name) > 100 || header.Format != tar.FormatUSTAR || header.Linkname != "" || header.Uid != 0 || header.Gid != 0 || header.Uname != "" || header.Gname != "" || !header.ModTime.Equal(canonicalArchiveTime) || !header.AccessTime.IsZero() || !header.ChangeTime.IsZero() || len(header.PAXRecords) != 0 || len(header.Xattrs) != 0 {
			return bundleSnapshot{}, fmt.Errorf("archive entry %q is not canonical", header.Name)
		}
		if entryCount >= maximumBundleEntries {
			return bundleSnapshot{}, fmt.Errorf("archive exceeds %d total entries", maximumBundleEntries)
		}
		entryCount++
		previous = header.Name
		isDirectory := header.Typeflag == tar.TypeDir
		name := header.Name
		if isDirectory {
			if !strings.HasSuffix(name, "/") || header.Mode != 0o700 || header.Size != 0 {
				return bundleSnapshot{}, fmt.Errorf("archive directory %q is not canonical", name)
			}
			name = strings.TrimSuffix(name, "/")
			if strings.Count(name, "/")+1 > maximumBundleDepth {
				return bundleSnapshot{}, fmt.Errorf("archive directory %q exceeds maximum depth %d", name, maximumBundleDepth)
			}
		} else if header.Typeflag != tar.TypeReg || strings.HasSuffix(name, "/") || header.Mode != 0o600 || header.Size < 0 || header.Size > MaximumEvidenceBytes {
			return bundleSnapshot{}, fmt.Errorf("archive file %q has invalid type, mode, or size", name)
		}
		if !isDirectory && strings.Count(name, "/") > maximumBundleDepth {
			return bundleSnapshot{}, fmt.Errorf("archive file %q exceeds maximum depth %d", name, maximumBundleDepth)
		}
		clean, err := safeArtifactPath(name)
		if err != nil || filepath.ToSlash(clean) != name {
			return bundleSnapshot{}, fmt.Errorf("archive entry %q has an unsafe path", name)
		}
		if isDirectory {
			if _, duplicate := snapshot.directories[name]; duplicate {
				return bundleSnapshot{}, fmt.Errorf("archive directory %q is duplicated", name)
			}
			snapshot.directories[name] = struct{}{}
			continue
		}
		file, err := io.ReadAll(io.LimitReader(archive, MaximumEvidenceBytes+1))
		if err != nil || int64(len(file)) != header.Size {
			return bundleSnapshot{}, fmt.Errorf("archive file %q is truncated or unreadable", name)
		}
		total += int64(len(file))
		if total > maximumBundleBytes {
			return bundleSnapshot{}, fmt.Errorf("archive exceeds %d bytes of bundle content", maximumBundleBytes)
		}
		if _, duplicate := snapshot.files[name]; duplicate {
			return bundleSnapshot{}, fmt.Errorf("archive file %q is duplicated", name)
		}
		snapshot.files[name] = file
	}
	if len(snapshot.files) == 0 || underlying.Len() != 0 {
		return bundleSnapshot{}, errors.New("archive is empty or contains trailing data")
	}
	var canonical bytes.Buffer
	if err := writeCanonicalArchive(&canonical, snapshot); err != nil {
		return bundleSnapshot{}, fmt.Errorf("rebuild canonical archive: %w", err)
	}
	if !bytes.Equal(document, canonical.Bytes()) {
		return bundleSnapshot{}, errors.New("archive bytes are not canonical")
	}
	return snapshot, nil
}
