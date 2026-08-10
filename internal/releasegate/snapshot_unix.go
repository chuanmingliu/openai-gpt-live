//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package releasegate

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/sys/unix"
)

const (
	maximumBundleEntries = 1024
	maximumBundleDepth   = 2
	maximumBundleBytes   = 64 << 20
)

// bundleSnapshot is the only filesystem view used by verification. Every
// component is opened relative to an already trusted directory descriptor,
// every leaf is fstat'd, and every regular file is read exactly once.
type bundleSnapshot struct {
	files       map[string][]byte
	directories map[string]struct{}
}

func captureBundleSnapshot(path string) (bundleSnapshot, error) {
	root, err := openDirectoryNoFollow(path)
	if err != nil {
		return bundleSnapshot{}, fmt.Errorf("open artifact root without following symbolic links: %w", err)
	}
	defer root.Close()

	snapshot := bundleSnapshot{
		files:       make(map[string][]byte),
		directories: make(map[string]struct{}),
	}
	var total int64
	var entries int
	if err := snapshotDirectory(root, "", 0, &snapshot, &total, &entries); err != nil {
		return bundleSnapshot{}, err
	}
	return snapshot, nil
}

func openDirectoryNoFollow(path string) (*os.File, error) {
	if path == "" {
		return nil, errors.New("directory path is required")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if filepath.Clean(absolute) == string(filepath.Separator) {
		return nil, errors.New("filesystem root cannot be an evidence directory")
	}
	volume := filepath.VolumeName(absolute)
	if volume != "" {
		return nil, errors.New("volume-qualified paths are unsupported on this platform")
	}

	fd, err := unix.Open(string(filepath.Separator), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	current := os.NewFile(uintptr(fd), string(filepath.Separator))
	components := strings.Split(strings.TrimPrefix(filepath.Clean(absolute), string(filepath.Separator)), string(filepath.Separator))
	for _, component := range components {
		if component == "" || component == "." || component == ".." || strings.ContainsRune(component, '\x00') {
			current.Close()
			return nil, errors.New("directory path contains an unsafe component")
		}
		nextFD, openErr := unix.Openat(int(current.Fd()), component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if openErr != nil {
			current.Close()
			return nil, fmt.Errorf("open directory component %q: %w", component, openErr)
		}
		next := os.NewFile(uintptr(nextFD), component)
		var stat unix.Stat_t
		if statErr := unix.Fstat(nextFD, &stat); statErr != nil || stat.Mode&unix.S_IFMT != unix.S_IFDIR {
			next.Close()
			current.Close()
			if statErr != nil {
				return nil, fmt.Errorf("fstat directory component %q: %w", component, statErr)
			}
			return nil, fmt.Errorf("directory component %q is not a directory", component)
		}
		current.Close()
		current = next
	}
	return current, nil
}

// ReadBoundedNoFollow reads one regular file through trusted directory
// descriptors. No pathname component is followed, the opened leaf is fstat'd,
// and the returned bytes come from the sole bounded read.
func ReadBoundedNoFollow(path string, maximum int64) ([]byte, error) {
	if path == "" || maximum <= 0 {
		return nil, errors.New("bounded input path and positive size limit are required")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve bounded input: %w", err)
	}
	base := filepath.Base(absolute)
	if base == "." || base == string(filepath.Separator) || strings.ContainsAny(base, "/\\\x00") {
		return nil, errors.New("bounded input must name a safe file")
	}
	parent, err := openDirectoryNoFollow(filepath.Dir(absolute))
	if err != nil {
		return nil, fmt.Errorf("open bounded input parent without following links: %w", err)
	}
	defer parent.Close()
	fd, err := unix.Openat(int(parent.Fd()), base, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open bounded input without following links: %w", err)
	}
	file := os.NewFile(uintptr(fd), base)
	defer file.Close()
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return nil, fmt.Errorf("fstat bounded input: %w", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Size < 0 || stat.Size > maximum {
		return nil, fmt.Errorf("bounded input has invalid type or size (maximum %d bytes)", maximum)
	}
	document, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil {
		return nil, fmt.Errorf("read bounded input: %w", err)
	}
	if int64(len(document)) > maximum || int64(len(document)) != stat.Size {
		return nil, fmt.Errorf("bounded input changed or exceeds %d-byte size limit", maximum)
	}
	return document, nil
}

type directoryEntryReader interface {
	ReadDir(int) ([]os.DirEntry, error)
}

type artifactDirectory interface {
	directoryEntryReader
	Fd() uintptr
}

func readDirectoryEntriesWithinBudget(directory directoryEntryReader, remaining int) ([]os.DirEntry, error) {
	if remaining < 0 || remaining >= maximumBundleEntries+1 {
		return nil, fmt.Errorf("artifact bundle exceeds %d total entries", maximumBundleEntries)
	}
	directoryEntries, err := directory.ReadDir(remaining + 1)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if len(directoryEntries) > remaining {
		return nil, fmt.Errorf("artifact bundle exceeds %d total entries", maximumBundleEntries)
	}
	return directoryEntries, nil
}

func snapshotDirectory(directory artifactDirectory, prefix string, depth int, snapshot *bundleSnapshot, total *int64, entries *int) error {
	remaining := maximumBundleEntries - *entries
	directoryEntries, err := readDirectoryEntriesWithinBudget(directory, remaining)
	if err != nil {
		return fmt.Errorf("read artifact directory %q: %w", prefix, err)
	}
	*entries += len(directoryEntries)
	sort.Slice(directoryEntries, func(i, j int) bool { return directoryEntries[i].Name() < directoryEntries[j].Name() })
	for _, entry := range directoryEntries {
		name := entry.Name()
		if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\\\x00") {
			return fmt.Errorf("artifact directory contains unsafe entry %q", name)
		}
		relative := name
		if prefix != "" {
			relative = prefix + "/" + name
		}
		fd, openErr := unix.Openat(int(directory.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
		if openErr != nil {
			return fmt.Errorf("open artifact entry %q without following links: %w", relative, openErr)
		}
		file := os.NewFile(uintptr(fd), relative)
		var stat unix.Stat_t
		if statErr := unix.Fstat(fd, &stat); statErr != nil {
			file.Close()
			return fmt.Errorf("fstat artifact entry %q: %w", relative, statErr)
		}
		switch stat.Mode & unix.S_IFMT {
		case unix.S_IFDIR:
			if depth >= maximumBundleDepth {
				file.Close()
				return fmt.Errorf("artifact directory %q exceeds maximum depth %d", relative, maximumBundleDepth)
			}
			snapshot.directories[relative] = struct{}{}
			if err := snapshotDirectory(file, relative, depth+1, snapshot, total, entries); err != nil {
				file.Close()
				return err
			}
			file.Close()
		case unix.S_IFREG:
			if stat.Size < 0 || stat.Size > MaximumEvidenceBytes {
				file.Close()
				return fmt.Errorf("artifact entry %q has invalid size", relative)
			}
			document, readErr := io.ReadAll(io.LimitReader(file, MaximumEvidenceBytes+1))
			file.Close()
			if readErr != nil {
				return fmt.Errorf("read artifact entry %q: %w", relative, readErr)
			}
			if int64(len(document)) > MaximumEvidenceBytes || int64(len(document)) != stat.Size {
				return fmt.Errorf("artifact entry %q changed size during its bounded read", relative)
			}
			*total += int64(len(document))
			if *total > maximumBundleBytes {
				return fmt.Errorf("artifact bundle exceeds %d bytes", maximumBundleBytes)
			}
			snapshot.files[relative] = document
		default:
			file.Close()
			return fmt.Errorf("artifact entry %q is not a regular file or directory", relative)
		}
	}
	return nil
}

func (snapshot bundleSnapshot) read(relative string, maximum int64) ([]byte, error) {
	path, err := safeArtifactPath(relative)
	if err != nil {
		return nil, err
	}
	document, ok := snapshot.files[filepath.ToSlash(path)]
	if !ok {
		return nil, os.ErrNotExist
	}
	if int64(len(document)) > maximum {
		return nil, fmt.Errorf("artifact %q size exceeds %d-byte limit", relative, maximum)
	}
	return document, nil
}
