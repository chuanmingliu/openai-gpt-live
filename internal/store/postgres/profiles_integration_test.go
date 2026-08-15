//go:build integration

package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/profile"
	"github.com/chuanmingliu/openai-gpt-live/internal/prompttmpl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const profileTemplateMediaType = "text/plain; charset=utf-8"

func TestProfileDraftSaveUsesExactGenerationAndOwnedCopies(t *testing.T) {
	repo, _, _ := repositoryHarness(t)
	draft := profileDraft("tenant-a", "insurance-sales", profileManifest(t))

	first, err := repo.SaveDraft(t.Context(), draft, 0)
	require.NoError(t, err)
	assert.Equal(t, int64(1), first.Generation)

	insertRetry := first.Clone()
	insertRetry.Manifest.Description = "insert retry"
	insertRetry.UpdatedAt = profileTestTime().Add(time.Second)
	_, err = repo.SaveDraft(t.Context(), insertRetry, 0)
	assert.ErrorIs(t, err, profile.ErrStaleDraft)

	// Mutating both the caller's input and one returned value must not change
	// the persisted manifest.
	draft.Manifest.Description = "caller mutation"
	first.Manifest.Bundle.Templates["system.j2"] = profile.TemplateRef{}
	loaded, ok, err := repo.LoadDraft(t.Context(), "tenant-a", "insurance-sales")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "original", loaded.Manifest.Description)
	assert.NotEmpty(t, loaded.Manifest.Bundle.Templates["system.j2"].Hash)

	loaded.Manifest.Description = "generation two"
	loaded.UpdatedAt = profileTestTime().Add(time.Minute)
	second, err := repo.SaveDraft(t.Context(), loaded, 1)
	require.NoError(t, err)
	assert.Equal(t, int64(2), second.Generation)

	stale := second.Clone()
	stale.Generation = 1
	stale.Manifest.Description = "stale secret description"
	_, err = repo.SaveDraft(t.Context(), stale, 1)
	assert.ErrorIs(t, err, profile.ErrStaleDraft)
	assert.NotContains(t, err.Error(), "stale secret")

	ctx := t.Context()
	start := make(chan struct{})
	results := make(chan error, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	for _, description := range []string{"concurrent-a", "concurrent-b"} {
		candidate := second.Clone()
		candidate.Manifest.Description = description
		candidate.UpdatedAt = profileTestTime().Add(2 * time.Minute)
		go func() {
			ready.Done()
			<-start
			_, saveErr := repo.SaveDraft(ctx, candidate, 2)
			results <- saveErr
		}()
	}
	ready.Wait()
	close(start)
	var successes, staleConflicts int
	for range 2 {
		switch saveErr := <-results; {
		case saveErr == nil:
			successes++
		case errors.Is(saveErr, profile.ErrStaleDraft):
			staleConflicts++
		default:
			t.Fatalf("unexpected concurrent save result: %v", saveErr)
		}
	}
	assert.Equal(t, 1, successes)
	assert.Equal(t, 1, staleConflicts)

	loaded, ok, err = repo.LoadDraft(t.Context(), "tenant-a", "insurance-sales")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, int64(3), loaded.Generation)
	_, ok, err = repo.LoadDraft(t.Context(), "tenant-b", "insurance-sales")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestProfileTemplateSaveDeleteIsAtomicAndAcceptsZeroBytes(t *testing.T) {
	repo, _, _ := repositoryHarness(t)
	draft := seedProfileDraft(t, repo, "tenant-a", "insurance-sales")

	withEmpty, err := repo.SaveTemplate(t.Context(), draft.TenantID, draft.ID, "opening_message.j2", []byte{}, draft.Generation, "editor-a")
	require.NoError(t, err)
	assert.Equal(t, int64(2), withEmpty.Generation)
	emptyHash := sha256.Sum256(nil)
	emptyRef := withEmpty.Manifest.Bundle.Templates["opening_message.j2"]
	assert.Equal(t, hex.EncodeToString(emptyHash[:]), emptyRef.Hash)
	assert.Equal(t, 0, emptyRef.Bytes)
	assert.Equal(t, profileTemplateMediaType, emptyRef.MediaType)
	emptyBlob, ok, err := repo.LoadBlob(t.Context(), draft.TenantID, emptyRef.Hash)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Empty(t, emptyBlob.Content)

	_, err = repo.SaveTemplate(t.Context(), draft.TenantID, draft.ID, "../secret.j2", []byte("private prompt"), withEmpty.Generation, "editor-a")
	assert.ErrorIs(t, err, profile.ErrInvalidProfile)
	_, err = repo.SaveTemplate(t.Context(), draft.TenantID, draft.ID, "turn_instruction.j2", []byte{0xff}, withEmpty.Generation, "editor-a")
	assert.ErrorIs(t, err, profile.ErrInvalidProfile)

	rolledBackSource := []byte("rolled back private prompt")
	rolledBackHash := sha256.Sum256(rolledBackSource)
	repo.failpoint = func(step string) error {
		if step == "profile_template_draft" {
			return errors.New("template transaction failure")
		}
		return nil
	}
	_, err = repo.SaveTemplate(t.Context(), draft.TenantID, draft.ID, "turn_instruction.j2", rolledBackSource, withEmpty.Generation, "editor-a")
	require.Error(t, err)
	repo.failpoint = nil
	rolledBackDraft, ok, err := repo.LoadDraft(t.Context(), draft.TenantID, draft.ID)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, withEmpty.Generation, rolledBackDraft.Generation)
	assert.NotContains(t, rolledBackDraft.Manifest.Bundle.Templates, "turn_instruction.j2")
	_, ok, err = repo.LoadBlob(t.Context(), draft.TenantID, hex.EncodeToString(rolledBackHash[:]))
	require.NoError(t, err)
	assert.False(t, ok, "a failed template transaction must roll back its newly inserted blob")

	staleSource := []byte("private stale prompt")
	staleHash := sha256.Sum256(staleSource)
	_, err = repo.SaveTemplate(t.Context(), draft.TenantID, draft.ID, "turn_instruction.j2", staleSource, 1, "editor-a")
	assert.ErrorIs(t, err, profile.ErrStaleDraft)
	_, ok, err = repo.LoadBlob(t.Context(), draft.TenantID, hex.EncodeToString(staleHash[:]))
	require.NoError(t, err)
	assert.False(t, ok, "a stale template save must roll back its newly inserted blob")

	_, err = repo.DeleteTemplate(t.Context(), draft.TenantID, draft.ID, "opening_message.j2", 1, "editor-a")
	assert.ErrorIs(t, err, profile.ErrStaleDraft)
	unchanged, ok, err := repo.LoadDraft(t.Context(), draft.TenantID, draft.ID)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, int64(2), unchanged.Generation)
	assert.Contains(t, unchanged.Manifest.Bundle.Templates, "opening_message.j2")

	repo.failpoint = func(step string) error {
		if step == "profile_template_deleted" {
			return errors.New("delete transaction failure")
		}
		return nil
	}
	_, err = repo.DeleteTemplate(t.Context(), draft.TenantID, draft.ID, "opening_message.j2", 2, "editor-a")
	require.Error(t, err)
	repo.failpoint = nil
	unchanged, ok, err = repo.LoadDraft(t.Context(), draft.TenantID, draft.ID)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, int64(2), unchanged.Generation)
	assert.Contains(t, unchanged.Manifest.Bundle.Templates, "opening_message.j2")

	_, err = repo.DeleteTemplate(t.Context(), draft.TenantID, draft.ID, "system.j2", 2, "editor-a")
	assert.ErrorIs(t, err, profile.ErrInvalidProfile)
	unchanged, ok, err = repo.LoadDraft(t.Context(), draft.TenantID, draft.ID)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, int64(2), unchanged.Generation)

	deleted, err := repo.DeleteTemplate(t.Context(), draft.TenantID, draft.ID, "opening_message.j2", 2, "editor-a")
	require.NoError(t, err)
	assert.Equal(t, int64(3), deleted.Generation)
	assert.NotContains(t, deleted.Manifest.Bundle.Templates, "opening_message.j2")
	_, ok, err = repo.LoadBlob(t.Context(), draft.TenantID, emptyRef.Hash)
	require.NoError(t, err)
	assert.True(t, ok, "content-addressed blobs remain available to other immutable refs")
}

func TestProfileBlobCanonicalIntegrityTenantIsolationAndOwnedBytes(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	content := []byte("bounded prompt source")
	canonical, err := (profile.Blob{MediaType: profileTemplateMediaType, Content: content}).Canonical()
	require.NoError(t, err)
	require.NoError(t, repo.PutBlob(t.Context(), "tenant-a", canonical))
	require.NoError(t, repo.PutBlob(t.Context(), "tenant-a", canonical), "exact retry is idempotent")

	content[0] = 'X'
	loaded, ok, err := repo.LoadBlob(t.Context(), "tenant-a", canonical.Hash)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, []byte("bounded prompt source"), loaded.Content)
	loaded.Content[0] = 'Y'
	reloaded, ok, err := repo.LoadBlob(t.Context(), "tenant-a", canonical.Hash)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, []byte("bounded prompt source"), reloaded.Content)

	_, ok, err = repo.LoadBlob(t.Context(), "tenant-b", canonical.Hash)
	require.NoError(t, err)
	assert.False(t, ok)
	require.NoError(t, repo.PutBlob(t.Context(), "tenant-b", canonical))

	wrongHash := canonical.Clone()
	wrongHash.Hash = strings.Repeat("a", 64)
	assert.ErrorIs(t, repo.PutBlob(t.Context(), "tenant-c", wrongHash), profile.ErrInvalidProfile)
	wrongCount := canonical.Clone()
	wrongCount.ContentBytes++
	assert.ErrorIs(t, repo.PutBlob(t.Context(), "tenant-c", wrongCount), profile.ErrInvalidProfile)
	wrongMedia := canonical.Clone()
	wrongMedia.MediaType = "application/json"
	err = repo.PutBlob(t.Context(), "tenant-a", wrongMedia)
	assert.ErrorIs(t, err, profile.ErrInvalidProfile)

	corruptHash := strings.Repeat("b", 64)
	_, err = pool.Exec(t.Context(), `INSERT INTO profile_blobs
		(tenant_id,content_hash,media_type,content,content_bytes,created_at)
		VALUES ('tenant-corrupt',$1,$2,$3,$4,$5)`, corruptHash, profileTemplateMediaType, []byte("secret corrupt bytes"), len("secret corrupt bytes"), profileTestTime())
	require.NoError(t, err)
	_, _, err = repo.LoadBlob(t.Context(), "tenant-corrupt", corruptHash)
	assert.ErrorIs(t, err, profile.ErrInvalidProfile)
	assert.NotContains(t, err.Error(), "secret corrupt")
}

func TestProfileNextVersionAndPublishAllocationSerializeConcurrentCandidates(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	draft := seedProfileDraft(t, repo, "tenant-a", "insurance-sales")
	next, err := repo.NextVersion(t.Context(), draft.TenantID, draft.ID)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), next)
	record := profilePublishRecord(t, draft, next, false)

	ctx := t.Context()
	start := make(chan struct{})
	results := make(chan error, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	for range 2 {
		go func() {
			ready.Done()
			<-start
			_, publishErr := repo.Publish(ctx, record)
			results <- publishErr
		}()
	}
	ready.Wait()
	close(start)
	var successes, conflicts int
	for range 2 {
		switch publishErr := <-results; {
		case publishErr == nil:
			successes++
		case errors.Is(publishErr, profile.ErrPublicationConflict):
			conflicts++
		default:
			t.Fatalf("unexpected concurrent publish result: %v", publishErr)
		}
	}
	assert.Equal(t, 1, successes)
	assert.Equal(t, 1, conflicts)
	assert.Equal(t, int64(1), tableCount(t, pool, "profile_versions"))

	next, err = repo.NextVersion(t.Context(), draft.TenantID, draft.ID)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), next)
	duplicateDraft := profilePublishRecord(t, draft, next, false)
	_, err = repo.Publish(t.Context(), duplicateDraft)
	assert.ErrorIs(t, err, profile.ErrPublicationConflict)
}

func TestProfilePublishResolveIsImmutableTenantScopedAndSwapsActiveAtomically(t *testing.T) {
	repo, pool, manual := repositoryHarness(t)
	draft := seedProfileDraft(t, repo, "tenant-a", "insurance-sales")
	v1Record := profilePublishRecord(t, draft, 1, true)
	v1, err := repo.Publish(t.Context(), v1Record)
	require.NoError(t, err)
	assert.True(t, v1.Active)

	v1Record.Manifest.Description = "mutated after publish"
	v1Record.Manifest.Bundle.Templates["system.j2"] = profile.TemplateRef{}
	resolved, ok, err := repo.Resolve(t.Context(), "tenant-a", draft.ID, 1)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "original", resolved.Manifest.Description)
	assert.True(t, resolved.Active)
	_, ok, err = repo.Resolve(t.Context(), "tenant-b", draft.ID, 1)
	require.NoError(t, err)
	assert.False(t, ok)

	nextDraft := draft.Clone()
	nextDraft.Manifest.Description = "version two"
	nextDraft.UpdatedAt = profileTestTime().Add(time.Minute)
	nextDraft, err = repo.SaveDraft(t.Context(), nextDraft, draft.Generation)
	require.NoError(t, err)
	v2Record := profilePublishRecord(t, nextDraft, 2, true)
	v2, err := repo.Publish(t.Context(), v2Record)
	require.NoError(t, err)
	assert.True(t, v2.Active)

	v1, ok, err = repo.Resolve(t.Context(), "tenant-a", draft.ID, 1)
	require.NoError(t, err)
	require.True(t, ok)
	assert.False(t, v1.Active)
	v2, ok, err = repo.Resolve(t.Context(), "tenant-a", draft.ID, 2)
	require.NoError(t, err)
	require.True(t, ok)
	assert.True(t, v2.Active)
	assert.Equal(t, "version two", v2.Manifest.Description)

	manual.Advance(3 * time.Minute)
	require.NoError(t, repo.Activate(t.Context(), "tenant-a", draft.ID, 1, "operator-a"))
	v1, ok, err = repo.Resolve(t.Context(), "tenant-a", draft.ID, 1)
	require.NoError(t, err)
	require.True(t, ok)
	assert.True(t, v1.Active)
	v2, ok, err = repo.Resolve(t.Context(), "tenant-a", draft.ID, 2)
	require.NoError(t, err)
	require.True(t, ok)
	assert.False(t, v2.Active)
	var activatedAt time.Time
	var activatedBy string
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT activated_at,activated_by FROM profile_versions
		WHERE tenant_id='tenant-a' AND profile_id='insurance-sales' AND version=1`).Scan(&activatedAt, &activatedBy))
	assert.True(t, activatedAt.Equal(profileTestTime().Add(3*time.Minute)))
	assert.Equal(t, "operator-a", activatedBy)

	v2.Manifest.Bundle.Templates["system.j2"] = profile.TemplateRef{}
	v2.Snapshot.Knowledge = append(v2.Snapshot.Knowledge, profile.KnowledgeRef{})
	again, ok, err := repo.Resolve(t.Context(), "tenant-a", draft.ID, 2)
	require.NoError(t, err)
	require.True(t, ok)
	assert.NotEmpty(t, again.Manifest.Bundle.Templates["system.j2"].Hash)
	assert.Empty(t, again.Snapshot.Knowledge)
}

func TestProfilePublishRollsBackInsertedVersionAndActiveSwap(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	draft := seedProfileDraft(t, repo, "tenant-a", "insurance-sales")
	_, err := repo.Publish(t.Context(), profilePublishRecord(t, draft, 1, true))
	require.NoError(t, err)

	draft.Manifest.Description = "version two private prompt"
	draft.UpdatedAt = profileTestTime().Add(time.Minute)
	draft, err = repo.SaveDraft(t.Context(), draft, draft.Generation)
	require.NoError(t, err)
	repo.failpoint = func(step string) error {
		if step == "profile_publish_inserted" {
			return errors.New("SELECT secret-dsn private prompt")
		}
		return nil
	}
	_, err = repo.Publish(t.Context(), profilePublishRecord(t, draft, 2, true))
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "SELECT")
	assert.NotContains(t, err.Error(), "secret-dsn")
	assert.NotContains(t, err.Error(), "private prompt")
	repo.failpoint = nil

	assert.Equal(t, int64(1), tableCount(t, pool, "profile_versions"))
	_, ok, err := repo.Resolve(t.Context(), "tenant-a", draft.ID, 2)
	require.NoError(t, err)
	assert.False(t, ok)
	v1, ok, err := repo.Resolve(t.Context(), "tenant-a", draft.ID, 1)
	require.NoError(t, err)
	require.True(t, ok)
	assert.True(t, v1.Active)

	cancelled, cancel := context.WithCancel(t.Context())
	repo.failpoint = func(step string) error {
		if step == "profile_publish_deactivated" {
			cancel()
		}
		return nil
	}
	_, err = repo.Publish(cancelled, profilePublishRecord(t, draft, 2, true))
	assert.ErrorIs(t, err, context.Canceled)
	repo.failpoint = nil
	assert.Equal(t, int64(1), tableCount(t, pool, "profile_versions"))
	v1, ok, err = repo.Resolve(t.Context(), "tenant-a", draft.ID, 1)
	require.NoError(t, err)
	require.True(t, ok)
	assert.True(t, v1.Active)
}

func TestProfileRevokeIsTenantExactIdempotentAndBlocksActivation(t *testing.T) {
	repo, _, _ := repositoryHarness(t)
	draft := seedProfileDraft(t, repo, "tenant-a", "insurance-sales")
	_, err := repo.Publish(t.Context(), profilePublishRecord(t, draft, 1, true))
	require.NoError(t, err)

	assert.ErrorIs(t, repo.Activate(t.Context(), "tenant-b", draft.ID, 1, "intruder"), profile.ErrInvalidProfile)
	assert.ErrorIs(t, repo.Revoke(t.Context(), "tenant-b", draft.ID, 1, "intruder", profileTestTime().Add(time.Minute)), profile.ErrInvalidProfile)
	stillActive, ok, err := repo.Resolve(t.Context(), "tenant-a", draft.ID, 1)
	require.NoError(t, err)
	require.True(t, ok)
	assert.True(t, stillActive.Active)

	requestedRevokedAt := profileTestTime().Add(2*time.Minute + 123*time.Nanosecond)
	storedRevokedAt := requestedRevokedAt.Truncate(time.Microsecond)
	require.NoError(t, repo.Revoke(t.Context(), "tenant-a", draft.ID, 1, "operator-a", requestedRevokedAt))
	revoked, ok, err := repo.Resolve(t.Context(), "tenant-a", draft.ID, 1)
	require.NoError(t, err)
	require.True(t, ok)
	assert.False(t, revoked.Active)
	require.NotNil(t, revoked.RevokedAt)
	assert.Equal(t, storedRevokedAt, *revoked.RevokedAt)
	assert.ErrorIs(t, repo.Activate(t.Context(), "tenant-a", draft.ID, 1, "operator-a"), profile.ErrProfileRevoked)

	require.NoError(t, repo.Revoke(t.Context(), "tenant-a", draft.ID, 1, "operator-a", requestedRevokedAt), "an exact retry is idempotent")
	assert.ErrorIs(t, repo.Revoke(t.Context(), "tenant-a", draft.ID, 1, "operator-b", requestedRevokedAt), profile.ErrPublicationConflict)
	assert.ErrorIs(t, repo.Revoke(t.Context(), "tenant-a", draft.ID, 1, "operator-a", requestedRevokedAt.Add(time.Second)), profile.ErrPublicationConflict)
}

func TestProfileListPublishedCursorBoundaryStableOrderAndOwnedPointers(t *testing.T) {
	repo, _, _ := repositoryHarness(t)
	for _, id := range []profile.ID{"b-profile", "a-profile", "c-profile"} {
		draft := seedProfileDraft(t, repo, "tenant-a", id)
		_, err := repo.Publish(t.Context(), profilePublishRecord(t, draft, 1, false))
		require.NoError(t, err)
		if id == "a-profile" {
			draft.Manifest.Description = "a version two"
			draft.UpdatedAt = profileTestTime().Add(time.Minute)
			draft, err = repo.SaveDraft(t.Context(), draft, draft.Generation)
			require.NoError(t, err)
			_, err = repo.Publish(t.Context(), profilePublishRecord(t, draft, 2, false))
			require.NoError(t, err)
		}
	}
	draftB := profile.ID("b-profile")
	revokedAt := profileTestTime().Add(2 * time.Minute)
	require.NoError(t, repo.Revoke(t.Context(), "tenant-a", draftB, 1, "operator", revokedAt))
	other := seedProfileDraft(t, repo, "tenant-b", "private-profile")
	_, err := repo.Publish(t.Context(), profilePublishRecord(t, other, 1, false))
	require.NoError(t, err)

	page1, cursor, err := repo.ListPublished(t.Context(), "tenant-a", 2, "")
	require.NoError(t, err)
	require.Len(t, page1, 2)
	assert.Equal(t, []profile.VersionRef{{ID: "a-profile", Version: 1}, {ID: "a-profile", Version: 2}}, []profile.VersionRef{page1[0].Ref, page1[1].Ref})
	assert.NotEmpty(t, cursor)
	assert.LessOrEqual(t, len(cursor), 512)

	// A later key that sorts before the cursor cannot duplicate or shift the
	// second keyset page.
	inserted := seedProfileDraft(t, repo, "tenant-a", "0-profile")
	_, err = repo.Publish(t.Context(), profilePublishRecord(t, inserted, 1, false))
	require.NoError(t, err)
	page2, next, err := repo.ListPublished(t.Context(), "tenant-a", 2, cursor)
	require.NoError(t, err)
	require.Len(t, page2, 2)
	assert.Equal(t, []profile.VersionRef{{ID: "b-profile", Version: 1}, {ID: "c-profile", Version: 1}}, []profile.VersionRef{page2[0].Ref, page2[1].Ref})
	assert.Empty(t, next)
	require.NotNil(t, page2[0].RevokedAt)
	*page2[0].RevokedAt = time.Time{}

	again, _, err := repo.ListPublished(t.Context(), "tenant-a", 10, "")
	require.NoError(t, err)
	var foundB *profile.PublishedSummary
	for index := range again {
		if again[index].Ref == (profile.VersionRef{ID: "b-profile", Version: 1}) {
			foundB = &again[index]
		}
	}
	require.NotNil(t, foundB)
	require.NotNil(t, foundB.RevokedAt)
	assert.Equal(t, revokedAt, *foundB.RevokedAt)
	for _, summary := range again {
		assert.NotEqual(t, profile.ID("private-profile"), summary.Ref.ID)
	}

	for _, tc := range []struct {
		name   string
		size   int
		cursor string
	}{
		{name: "zero page", size: 0},
		{name: "oversized page", size: 101},
		{name: "invalid cursor", size: 10, cursor: "not-base64!"},
		{name: "oversized cursor", size: 10, cursor: strings.Repeat("x", 513)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, listErr := repo.ListPublished(t.Context(), "tenant-a", tc.size, tc.cursor)
			assert.ErrorIs(t, listErr, profile.ErrInvalidProfile)
			if listErr != nil && tc.cursor != "" {
				assert.NotContains(t, listErr.Error(), tc.cursor)
			}
		})
	}
}

func TestProfilePublishedNestedMapsSlicesAndRawJSONAreOwned(t *testing.T) {
	repo, _, _ := repositoryHarness(t)
	manifest, workerBlob := richProfileManifest(t)
	require.NoError(t, repo.PutBlob(t.Context(), "tenant-a", mustProfileBlob(t, []byte("system"))))
	require.NoError(t, repo.PutBlob(t.Context(), "tenant-a", workerBlob))
	draft := profileDraft("tenant-a", "rich-profile", manifest)
	var err error
	draft, err = repo.SaveDraft(t.Context(), draft, 0)
	require.NoError(t, err)
	record := profilePublishRecord(t, draft, 1, false)
	_, err = repo.Publish(t.Context(), record)
	require.NoError(t, err)

	record.Manifest.Bundle.Schema.Fields["customer"] = prompttmpl.Field{}
	record.Manifest.Recipes[0].BindingIDs[0] = "mutated"
	record.Manifest.Recipes[0].ResultSchema[0] = '['
	record.Manifest.Bindings[0].ToolIDs[0] = "mutated"
	record.Manifest.Knowledge[0].ID = "mutated"
	record.Snapshot.Recipes[0].RequiredEvidence[0] = "mutated"

	published, ok, err := repo.Resolve(t.Context(), "tenant-a", "rich-profile", 1)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, prompttmpl.TypeString, published.Manifest.Bundle.Schema.Fields["customer"].Type)
	assert.Equal(t, []string{"lookup"}, published.Manifest.Recipes[0].BindingIDs)
	assert.JSONEq(t, `{"type":"object"}`, string(published.Manifest.Recipes[0].ResultSchema))
	assert.Equal(t, []string{"read"}, published.Manifest.Bindings[0].ToolIDs)
	assert.Equal(t, "knowledge", published.Manifest.Knowledge[0].ID)
	assert.Equal(t, []string{"customer_request"}, published.Snapshot.Recipes[0].RequiredEvidence)

	published.Manifest.Bundle.Schema.Fields["customer"] = prompttmpl.Field{}
	published.Manifest.Recipes[0].ResultSchema[0] = '['
	published.Snapshot.Bindings[0].ToolIDs[0] = "mutated"
	again, ok, err := repo.Resolve(t.Context(), "tenant-a", "rich-profile", 1)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, prompttmpl.TypeString, again.Manifest.Bundle.Schema.Fields["customer"].Type)
	assert.JSONEq(t, `{"type":"object"}`, string(again.Manifest.Recipes[0].ResultSchema))
	assert.Equal(t, []string{"read"}, again.Snapshot.Bindings[0].ToolIDs)
}

func TestProfileResolvePreservesStoredCanonicalRawJSONAndHash(t *testing.T) {
	repo, _, _ := repositoryHarness(t)
	manifest, workerBlob := richProfileManifest(t)
	manifest.Recipes[0].ResultSchema = []byte(`{"minimum":1.2300e+2,"type":"number"}`)
	require.NoError(t, manifest.Validate())
	require.NoError(t, repo.PutBlob(t.Context(), "tenant-a", mustProfileBlob(t, []byte("system"))))
	require.NoError(t, repo.PutBlob(t.Context(), "tenant-a", workerBlob))
	draft, err := repo.SaveDraft(t.Context(), profileDraft("tenant-a", "numeric-profile", manifest), 0)
	require.NoError(t, err)
	storedSchema := append([]byte(nil), draft.Manifest.Recipes[0].ResultSchema...)
	assert.JSONEq(t, `{"minimum":123.00,"type":"number"}`, string(storedSchema))
	record := profilePublishRecord(t, draft, 1, false)
	_, err = repo.Publish(t.Context(), record)
	require.NoError(t, err)

	published, ok, err := repo.Resolve(t.Context(), "tenant-a", "numeric-profile", 1)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, storedSchema, []byte(published.Manifest.Recipes[0].ResultSchema))
	assert.Equal(t, record.Snapshot.SnapshotHash, published.Snapshot.SnapshotHash)
}

func TestProfileErrorsAreStableContentFreeAndValidateBeforeSQL(t *testing.T) {
	repo, pool, _ := repositoryHarness(t)
	secret := "private-prompt SELECT postgres://credential"
	_, err := repo.SaveDraft(t.Context(), profileDraft("bad\ntenant"+secret, "profile", profileManifest(t)), 0)
	assert.ErrorIs(t, err, profile.ErrInvalidProfile)
	assert.NotContains(t, err.Error(), secret)

	_, err = repo.SaveTemplate(nil, "tenant-a", "profile", "system.j2", []byte(secret), 0, "actor")
	assert.ErrorIs(t, err, profile.ErrInvalidProfile)
	assert.NotContains(t, err.Error(), secret)
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err = repo.LoadDraft(cancelled, "tenant-a", "profile")
	assert.ErrorIs(t, err, context.Canceled)

	draft := seedProfileDraft(t, repo, "tenant-a", "corrupt-profile")
	_, err = repo.Publish(t.Context(), profilePublishRecord(t, draft, 1, false))
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `UPDATE profile_versions SET manifest='{"description":"private persisted prompt"}'::jsonb
		WHERE tenant_id='tenant-a' AND profile_id='corrupt-profile' AND version=1`)
	require.NoError(t, err)
	_, _, err = repo.Resolve(t.Context(), "tenant-a", "corrupt-profile", 1)
	assert.ErrorIs(t, err, profile.ErrInvalidProfile)
	assert.NotContains(t, err.Error(), "private persisted prompt")
	assert.NotContains(t, err.Error(), "UPDATE")
}

func seedProfileDraft(t *testing.T, repo *Repository, tenant string, id profile.ID) profile.Draft {
	t.Helper()
	manifest := profileManifest(t)
	systemBlob := mustProfileBlob(t, []byte("system"))
	require.NoError(t, repo.PutBlob(t.Context(), tenant, systemBlob))
	draft, err := repo.SaveDraft(t.Context(), profileDraft(tenant, id, manifest), 0)
	require.NoError(t, err)
	return draft
}

func profileDraft(tenant string, id profile.ID, manifest profile.Manifest) profile.Draft {
	return profile.Draft{
		TenantID: tenant, ID: id, Generation: 0, Manifest: manifest,
		UpdatedAt: profileTestTime(), UpdatedBy: "editor-a",
	}
}

func profileManifest(t *testing.T) profile.Manifest {
	t.Helper()
	ref := profileRef(mustProfileBlob(t, []byte("system")))
	manifest := profile.Manifest{
		Description: "original",
		Bundle: profile.PromptBundle{
			Templates: map[string]profile.TemplateRef{"system.j2": ref},
			Schema:    prompttmpl.ContextSchema{Fields: map[string]prompttmpl.Field{}},
			Fixtures:  map[string]profile.TemplateRef{},
		},
	}
	require.NoError(t, manifest.Validate())
	return manifest
}

func richProfileManifest(t *testing.T) (profile.Manifest, profile.Blob) {
	t.Helper()
	systemBlob := mustProfileBlob(t, []byte("system"))
	workerBlob := mustProfileBlob(t, []byte("worker objective"))
	systemRef, workerRef := profileRef(systemBlob), profileRef(workerBlob)
	manifest := profile.Manifest{
		Description: "rich",
		Bundle: profile.PromptBundle{
			Templates: map[string]profile.TemplateRef{
				"system.j2":        systemRef,
				"workers/quote.j2": workerRef,
			},
			Schema: prompttmpl.ContextSchema{Fields: map[string]prompttmpl.Field{
				"customer": {Type: prompttmpl.TypeString, Required: true, MaxBytes: 1024, Templates: []string{"system.j2"}, Sensitivity: "customer", Source: "session"},
			}},
			Fixtures: map[string]profile.TemplateRef{},
		},
		Recipes: []profile.WorkerRecipe{{
			ID: "quote", Version: 1, Objective: workerRef, BindingIDs: []string{"lookup"},
			RequiredEvidence: []string{"customer_request"}, OptionalEvidence: []string{"region"},
			MaxObjectiveBytes: 1024, MaxEvidenceBytes: 1024, MaxToolResultBytes: 1024, MaxFinalResultBytes: 1024,
			ExecutionTimeout: time.Minute, ToolTimeout: time.Minute, OverallTimeout: 2 * time.Minute,
			ResultSchema: []byte(`{"type":"object"}`),
		}},
		Bindings:  []profile.AdapterBinding{{AdapterID: "lookup", Version: "1", ToolIDs: []string{"read"}, MaxResultBytes: 1024}},
		Knowledge: []profile.KnowledgeRef{{ID: "knowledge", Version: 1, Hash: strings.Repeat("c", 64)}},
	}
	require.NoError(t, manifest.Validate())
	return manifest, workerBlob
}

func profilePublishRecord(t *testing.T, draft profile.Draft, version uint64, activate bool) profile.PublishRecord {
	t.Helper()
	bundleHash, err := draft.Manifest.Bundle.Hash()
	require.NoError(t, err)
	snapshot := profile.Snapshot{
		TenantID: draft.TenantID, Ref: profile.VersionRef{ID: draft.ID, Version: version},
		EngineVersion: "jinja-go-v1", BundleHash: bundleHash,
		System:  draft.Manifest.Bundle.Templates["system.j2"],
		Opening: draft.Manifest.Bundle.Templates["opening_message.j2"],
		Turn:    draft.Manifest.Bundle.Templates["turn_instruction.j2"],
		Recipes: cloneProfileRecipes(draft.Manifest.Recipes), Bindings: cloneProfileBindings(draft.Manifest.Bindings),
		Knowledge:              append([]profile.KnowledgeRef(nil), draft.Manifest.Knowledge...),
		AdapterRegistryVersion: "registry-v1",
	}
	snapshot.SnapshotHash, err = snapshot.Hash()
	require.NoError(t, err)
	record := profile.PublishRecord{
		TenantID: draft.TenantID, ProfileID: draft.ID,
		ExpectedGeneration: draft.Generation, DraftGeneration: draft.Generation,
		Manifest: draft.Manifest.Clone(), Snapshot: snapshot,
		EngineVersion: snapshot.EngineVersion, Actor: "publisher-a", Activate: activate,
		PublishedAt: profileTestTime().Add(time.Duration(version) * time.Minute),
	}
	require.NoError(t, record.Validate())
	return record
}

func mustProfileBlob(t *testing.T, content []byte) profile.Blob {
	t.Helper()
	blob, err := (profile.Blob{MediaType: profileTemplateMediaType, Content: append([]byte(nil), content...)}).Canonical()
	require.NoError(t, err)
	return blob
}

func profileRef(blob profile.Blob) profile.TemplateRef {
	return profile.TemplateRef{Hash: blob.Hash, MediaType: blob.MediaType, Bytes: blob.ContentBytes}
}

func cloneProfileRecipes(source []profile.WorkerRecipe) []profile.WorkerRecipe {
	result := make([]profile.WorkerRecipe, len(source))
	for index := range source {
		result[index] = source[index].Clone()
	}
	return result
}

func cloneProfileBindings(source []profile.AdapterBinding) []profile.AdapterBinding {
	result := make([]profile.AdapterBinding, len(source))
	for index := range source {
		result[index] = source[index].Clone()
	}
	return result
}

func profileTestTime() time.Time {
	return repositoryTestNow
}
