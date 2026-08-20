package main

import (
	"embed"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"syscall"
	"text/template"

	"golang.org/x/sys/unix"
)

var (
	//go:embed templates/*
	templateFiles embed.FS
	adapterName   = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
)

type scaffoldData struct {
	Name string
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "adapter scaffold failed")
		os.Exit(1)
	}
}

func run(arguments []string) error {
	set := flag.NewFlagSet("live-adapter-scaffold", flag.ContinueOnError)
	set.SetOutput(io.Discard)
	rootFlag := set.String("root", ".", "repository root")
	nameFlag := set.String("name", "", "native adapter name")
	if err := set.Parse(arguments); err != nil || set.NArg() != 0 || !adapterName.MatchString(*nameFlag) {
		return errors.New("adapter scaffold arguments are invalid")
	}
	root, err := canonicalRoot(*rootFlag)
	if err != nil {
		return err
	}
	parent, err := secureAdapterParent(root)
	if err != nil {
		return err
	}
	parentBefore, err := os.Stat(parent)
	if err != nil {
		return errors.New("adapter scaffold parent is unavailable")
	}
	target := filepath.Join(parent, *nameFlag)
	if _, err := os.Lstat(target); err == nil || !errors.Is(err, fs.ErrNotExist) {
		return errors.New("adapter scaffold target already exists")
	}
	temporary, err := os.MkdirTemp(parent, "."+*nameFlag+"-")
	if err != nil {
		return errors.New("adapter scaffold temporary directory failed")
	}
	if err := os.Chmod(temporary, 0o700); err != nil {
		_ = os.Remove(temporary)
		return errors.New("adapter scaffold temporary mode failed")
	}
	installed := false
	defer func() {
		if !installed {
			_ = os.RemoveAll(temporary)
		}
	}()
	for source, destination := range map[string]string{
		"templates/adapter.go.tmpl":      "adapter.go",
		"templates/adapter_test.go.tmpl": "adapter_test.go",
		"templates/manifest.json.tmpl":   "manifest.json",
	} {
		if err := renderExclusive(temporary, source, destination, scaffoldData{Name: *nameFlag}); err != nil {
			return err
		}
	}
	if err := syncDirectory(temporary); err != nil {
		return err
	}
	parentAfter, err := os.Stat(parent)
	if err != nil || !os.SameFile(parentBefore, parentAfter) {
		return errors.New("adapter scaffold parent identity changed")
	}
	parentFile, err := os.Open(parent)
	if err != nil {
		return errors.New("adapter scaffold parent open failed")
	}
	defer parentFile.Close()
	if err := unix.Renameat(int(parentFile.Fd()), filepath.Base(temporary), int(parentFile.Fd()), *nameFlag); err != nil {
		return errors.New("adapter scaffold atomic install failed")
	}
	installed = true
	if err := parentFile.Sync(); err != nil {
		return errors.New("adapter scaffold parent sync failed")
	}
	return nil
}

func canonicalRoot(raw string) (string, error) {
	absolute, err := filepath.Abs(raw)
	if err != nil {
		return "", errors.New("adapter scaffold root is invalid")
	}
	absolute = filepath.Clean(absolute)
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", errors.New("adapter scaffold root must be a canonical directory")
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() {
		return "", errors.New("adapter scaffold root must be a directory")
	}
	return filepath.Clean(resolved), nil
}

func secureAdapterParent(root string) (string, error) {
	current := root
	for _, segment := range []string{"internal", "adapters"} {
		current = filepath.Join(current, segment)
		info, err := os.Lstat(current)
		if errors.Is(err, fs.ErrNotExist) {
			if err := os.Mkdir(current, 0o700); err != nil {
				return "", errors.New("adapter scaffold parent creation failed")
			}
			info, err = os.Lstat(current)
		}
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return "", errors.New("adapter scaffold parent is unsafe")
		}
	}
	return current, nil
}

func renderExclusive(directory, source, destination string, data scaffoldData) error {
	raw, err := templateFiles.ReadFile(source)
	if err != nil {
		return errors.New("adapter scaffold template is unavailable")
	}
	parsed, err := template.New(filepath.Base(source)).Option("missingkey=error").Parse(string(raw))
	if err != nil {
		return errors.New("adapter scaffold template is invalid")
	}
	path := filepath.Join(directory, destination)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return errors.New("adapter scaffold output create failed")
	}
	writeErr := parsed.Execute(file, data)
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		return errors.New("adapter scaffold output write failed")
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return errors.New("adapter scaffold directory open failed")
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return errors.New("adapter scaffold directory sync failed")
	}
	return nil
}
