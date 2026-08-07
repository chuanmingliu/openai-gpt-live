package main

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseOptionsFlagCoherence(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{name: "defaults"},
		{name: "explicit up", args: []string{"-direction=up"}},
		{name: "explicit down", args: []string{"-direction=down"}},
		{name: "up steps", args: []string{"-direction=up", "-steps=2"}},
		{name: "down steps", args: []string{"-direction=down", "-steps=2"}},
		{name: "check", args: []string{"-check"}},
		{name: "bad direction", args: []string{"-direction=sideways"}, wantErr: "direction must be up or down"},
		{name: "negative steps", args: []string{"-steps=-1"}, wantErr: "steps must be nonnegative"},
		{name: "check direction up", args: []string{"-check", "-direction=up"}, wantErr: "check cannot be combined"},
		{name: "check direction down", args: []string{"-check", "-direction=down"}, wantErr: "check cannot be combined"},
		{name: "check positive steps", args: []string{"-check", "-steps=1"}, wantErr: "check cannot be combined"},
		{name: "check explicit zero steps", args: []string{"-check", "-steps=0"}, wantErr: "check cannot be combined"},
		{name: "positional", args: []string{"extra"}, wantErr: "positional arguments"},
		{name: "invalid environment name", args: []string{"-database-url-env=BAD-NAME"}, wantErr: "environment variable name is invalid"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseOptions(tc.args, &bytes.Buffer{})
			if tc.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tc.wantErr)
			}
		})
	}
}

func TestRunParsesBeforeEnvironmentLookup(t *testing.T) {
	lookups := 0
	err := run(context.Background(), []string{"-direction=sideways"}, func(string) (string, bool) {
		lookups++
		return "", false
	}, &bytes.Buffer{}, &bytes.Buffer{})
	assert.ErrorContains(t, err, "direction must be up or down")
	assert.Zero(t, lookups)
}

func TestRunEnvironmentFailuresDoNotExposeSecrets(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		err := run(context.Background(), nil, func(string) (string, bool) { return "", false }, &bytes.Buffer{}, &bytes.Buffer{})
		assert.ErrorContains(t, err, "DATABASE_URL is not set")
	})
	t.Run("invalid", func(t *testing.T) {
		secret := "postgres://user:hunter2@%"
		var stdout, stderr bytes.Buffer
		err := run(context.Background(), []string{"-database-url-env=PRIVATE_DB_URL"}, func(name string) (string, bool) {
			assert.Equal(t, "PRIVATE_DB_URL", name)
			return secret, true
		}, &stdout, &stderr)
		assert.ErrorContains(t, err, "PRIVATE_DB_URL is invalid")
		combined := err.Error() + stdout.String() + stderr.String()
		assert.NotContains(t, combined, secret)
		assert.NotContains(t, combined, "hunter2")
	})
}

func TestRunDispatchesAllSuccessfulActions(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		wantCall  string
		wantSteps int
	}{
		{name: "up", wantCall: "up"},
		{name: "down", args: []string{"-direction=down"}, wantCall: "down"},
		{name: "up steps", args: []string{"-steps=2"}, wantCall: "steps", wantSteps: 2},
		{name: "down steps", args: []string{"-direction=down", "-steps=2"}, wantCall: "steps", wantSteps: -2},
		{name: "check", args: []string{"-check"}, wantCall: "check"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			session := &fakeMigrationSession{version: 1}
			var openedURL string
			factory := func(_ context.Context, databaseURL string) (migrationSession, error) {
				openedURL = databaseURL
				return session, nil
			}
			const databaseURL = "postgres://user:secret@db.example/live"
			var stdout, stderr bytes.Buffer
			err := runWithSessionFactory(context.Background(), tc.args,
				func(string) (string, bool) { return databaseURL, true }, &stdout, &stderr, factory)
			assert.NoError(t, err)
			assert.Equal(t, databaseURL, openedURL)
			assert.Equal(t, []string{tc.wantCall, "version", "close"}, session.calls)
			assert.Equal(t, tc.wantSteps, session.steps)
			assert.NotEmpty(t, stdout.String())
			assert.Empty(t, stderr.String())
			assert.NotContains(t, stdout.String(), databaseURL)
			assert.NotContains(t, stdout.String(), "secret")
		})
	}
}

func TestRunRedactsMigrationOperationErrors(t *testing.T) {
	const databaseURL = "postgres://user:hunter2@db.example/live"
	session := &fakeMigrationSession{upErr: fmt.Errorf("provider echoed %s and hunter2", databaseURL)}
	var stdout, stderr bytes.Buffer
	err := runWithSessionFactory(context.Background(), nil,
		func(string) (string, bool) { return databaseURL, true }, &stdout, &stderr,
		func(context.Context, string) (migrationSession, error) { return session, nil })
	assert.Error(t, err)
	combined := err.Error() + stdout.String() + stderr.String()
	assert.NotContains(t, combined, databaseURL)
	assert.NotContains(t, combined, "hunter2")
}

type fakeMigrationSession struct {
	calls   []string
	steps   int
	version uint
	upErr   error
}

func (f *fakeMigrationSession) Up(context.Context) error {
	f.calls = append(f.calls, "up")
	return f.upErr
}

func (f *fakeMigrationSession) Down(context.Context) error {
	f.calls = append(f.calls, "down")
	return nil
}

func (f *fakeMigrationSession) Steps(_ context.Context, steps int) error {
	f.calls = append(f.calls, "steps")
	f.steps = steps
	return nil
}

func (f *fakeMigrationSession) Check(context.Context) error {
	f.calls = append(f.calls, "check")
	return nil
}

func (f *fakeMigrationSession) Version(context.Context) (uint, bool, error) {
	f.calls = append(f.calls, "version")
	return f.version, false, nil
}

func (f *fakeMigrationSession) Close() {
	f.calls = append(f.calls, "close")
}
