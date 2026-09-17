package ws

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/profile"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
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

func TestProfileQueryResolvesExactCustomVersionForReconnect(t *testing.T) {
	resolver := &recordingProfileResolver{resolved: resolvedProfile(t, "tenant-a", "insurance", 3)}
	got, err := resolveProfileForPurpose(t.Context(), "model=gpt-live&reconnect=1&profile=insurance&profile_version=3", "tenant-a", true, resolver, profile.ResolveReconnect)
	require.NoError(t, err)
	require.Equal(t, []profileResolveCall{{tenant: "tenant-a", ref: profile.VersionRef{ID: "insurance", Version: 3}, purpose: profile.ResolveReconnect}}, resolver.calls)
	assert.Equal(t, profile.ID("insurance"), got.Snapshot.Ref.ID)
}

func TestReconnectCredentialRequiresExactResolvedProfile(t *testing.T) {
	manual := clock.NewManual(time.Unix(20_000, 0))
	authority, err := session.NewReconnectCredentials([]byte(strings.Repeat("p", 32)), manual)
	require.NoError(t, err)
	selected := resolvedProfile(t, "tenant-a", "insurance", 3)
	claims := session.ReconnectClaims{
		Scope:     handoff.Scope{TenantID: "tenant-a", UserID: "user-a", ConversationID: "conversation-a"},
		SessionID: "session-a", Incarnation: 1, Model: "provider", ExpiresAt: manual.Now().Add(time.Minute),
		ProfileID: string(selected.Snapshot.Ref.ID), ProfileVersion: selected.Snapshot.Ref.Version,
		ProfileSnapshotHash: selected.Snapshot.SnapshotHash, ProfileEngineVersion: selected.Snapshot.EngineVersion,
	}
	issued, err := authority.Issue(claims)
	require.NoError(t, err)
	h := &handler{opts: Options{ReconnectCredentials: authority}}
	subject := session.Subject{TenantID: "tenant-a", UserID: "user-a"}
	_, err = h.verifyReconnectCredential(issued.Credential, subject, "provider", selected)
	require.NoError(t, err)

	different := selected.Clone()
	different.Snapshot.AdapterRegistryVersion = "different-v1"
	different.Snapshot.SnapshotHash = ""
	different.Snapshot.SnapshotHash, err = different.Snapshot.Hash()
	require.NoError(t, err)
	_, err = h.verifyReconnectCredential(issued.Credential, subject, "provider", different)
	require.ErrorIs(t, err, session.ErrInvalidReconnectCredential)
}

func TestReconnectProfileRevocationFailsAtResolutionBoundary(t *testing.T) {
	resolver := &recordingProfileResolver{err: profile.ErrProfileRevoked}
	_, err := resolveProfileForPurpose(t.Context(), "reconnect=1&profile=insurance&profile_version=3", "tenant-a", true, resolver, profile.ResolveReconnect)
	require.ErrorIs(t, err, errProfileForbidden)
	require.Equal(t, []profileResolveCall{{tenant: "tenant-a", ref: profile.VersionRef{ID: "insurance", Version: 3}, purpose: profile.ResolveReconnect}}, resolver.calls)
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
