package ws

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/chuanmingliu/openai-gpt-live/internal/profile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingProfileResolver struct {
	resolved profile.Resolved
	err      error
	calls    []profileResolveCall
}

type profileResolveCall struct {
	tenant  string
	ref     profile.VersionRef
	purpose profile.ResolvePurpose
}

func (resolver *recordingProfileResolver) Resolve(_ context.Context, tenant string, ref profile.VersionRef, purpose profile.ResolvePurpose) (profile.Resolved, error) {
	resolver.calls = append(resolver.calls, profileResolveCall{tenant: tenant, ref: ref, purpose: purpose})
	return resolver.resolved.Clone(), resolver.err
}

func TestProfileQueryResolvesExactCustomVersionForNewSession(t *testing.T) {
	resolver := &recordingProfileResolver{resolved: resolvedProfile(t, "tenant-a", "insurance", 3)}
	got, err := resolveProfile(t.Context(), "model=gpt-live&profile=insurance&profile_version=3", "tenant-a", true, resolver)
	require.NoError(t, err)
	require.Equal(t, []profileResolveCall{{tenant: "tenant-a", ref: profile.VersionRef{ID: "insurance", Version: 3}, purpose: profile.ResolveNewSession}}, resolver.calls)
	assert.Equal(t, profile.ID("insurance"), got.Snapshot.Ref.ID)
	assert.Equal(t, uint64(3), got.Snapshot.Ref.Version)
}

func TestProfileQueryDefaultsToOwnedBuiltinWithoutResolver(t *testing.T) {
	got, err := resolveProfile(t.Context(), "model=gpt-live", "tenant-a", false, nil)
	require.NoError(t, err)
	want := profile.Builtin()
	assert.Equal(t, want.Snapshot.SnapshotHash, got.Snapshot.SnapshotHash)
	got.Bundle.Templates["system.j2"] = "mutated"
	assert.NotEqual(t, got.Bundle.Templates["system.j2"], profile.Builtin().Bundle.Templates["system.j2"])
}

func TestProfileQueryRejectsMalformedAndDisabledCustomSelection(t *testing.T) {
	invalid := []string{
		"profile=insurance",
		"profile_version=3",
		"profile=insurance&profile=other&profile_version=3",
		"profile=insurance&profile_version=3&profile_version=4",
		"profile=insurance&profile_version=03",
		"profile=insurance&profile_version=0",
		"profile=insurance&profile_version=18446744073709551616",
		"profile=bad%20id&profile_version=3",
		"profile=bad%0Aid&profile_version=3",
		"profile=" + strings.Repeat("a", 257) + "&profile_version=3",
		"unknown=value",
		"model=a&model=b",
		"reconnect=1&reconnect=1",
	}
	for _, raw := range invalid {
		t.Run(raw, func(t *testing.T) {
			_, err := resolveProfile(t.Context(), raw, "tenant-a", true, &recordingProfileResolver{})
			assert.ErrorIs(t, err, errProfileQuery)
		})
	}

	_, err := resolveProfile(t.Context(), "profile=insurance&profile_version=3", "tenant-a", false, nil)
	assert.ErrorIs(t, err, errProfileForbidden)
}

func TestProfileQueryNormalizesResolverFailuresAndRejectsMismatchedOutput(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want error
	}{
		{"revoked", profile.ErrProfileRevoked, errProfileForbidden},
		{"missing", profile.ErrProfileIncompatible, errProfileUnavailable},
		{"store", profile.ErrStoreUnavailable, errProfileUnavailable},
		{"raw", errors.New("database DSN secret"), errProfileUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolver := &recordingProfileResolver{err: test.err}
			_, err := resolveProfile(t.Context(), "profile=insurance&profile_version=3", "tenant-a", true, resolver)
			assert.ErrorIs(t, err, test.want)
			assert.NotContains(t, err.Error(), "secret")
		})
	}

	mismatch := &recordingProfileResolver{resolved: resolvedProfile(t, "tenant-a", "other", 3)}
	_, err := resolveProfile(t.Context(), "profile=insurance&profile_version=3", "tenant-a", true, mismatch)
	assert.ErrorIs(t, err, errProfileUnavailable)
}

func resolvedProfile(t *testing.T, tenant string, id profile.ID, version uint64) profile.Resolved {
	t.Helper()
	resolved := profile.Builtin()
	resolved.Snapshot.TenantID = tenant
	resolved.Snapshot.Ref = profile.VersionRef{ID: id, Version: version}
	resolved.Snapshot.SnapshotHash = ""
	hash, err := resolved.Snapshot.Hash()
	require.NoError(t, err)
	resolved.Snapshot.SnapshotHash = hash
	require.NoError(t, resolved.Snapshot.Validate())
	return resolved
}
