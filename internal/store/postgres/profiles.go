package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/profile"
	"github.com/chuanmingliu/openai-gpt-live/internal/prompttmpl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	storedTemplateMediaType = "text/plain; charset=utf-8"
	maxProfilePageSize      = 100
	maxProfileCursorBytes   = 512
	maxProfileDocumentBytes = 4 * 1024 * 1024
)

var errProfileStoreUnavailable = errors.New("profile_store_unavailable")

var _ profile.Store = (*Repository)(nil)

// PutBlob stores one immutable tenant-scoped content tuple. Hash and byte
// count are always recomputed before SQL; zero values are accepted as omitted
// declarations, while non-zero declarations must be exact.
func (r *Repository) PutBlob(ctx context.Context, tenantID string, blob profile.Blob) error {
	if err := validateProfileRepository(r, ctx); err != nil {
		return err
	}
	if !validProfileIdentifier(tenantID) {
		return profile.ErrInvalidProfile
	}
	input := blob.Clone()
	canonical, err := input.Canonical()
	if err != nil || input.Hash != "" && input.Hash != canonical.Hash || input.ContentBytes != 0 && input.ContentBytes != canonical.ContentBytes {
		return profile.ErrInvalidProfile
	}
	createdAt := canonicalProfileTime(r.clock.Now())
	if !validProfileTime(createdAt) {
		return profile.ErrInvalidProfile
	}
	tag, err := r.pool.Exec(ctx, `INSERT INTO profile_blobs
		(tenant_id,content_hash,media_type,content,content_bytes,created_at)
		VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (tenant_id,content_hash) DO NOTHING`,
		tenantID, canonical.Hash, canonical.MediaType, nonNilProfileBytes(canonical.Content), canonical.ContentBytes, createdAt)
	if err != nil {
		return mapProfileStoreError(ctx, err, profile.ErrInvalidProfile)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	stored, ok, err := r.loadBlob(ctx, r.pool, tenantID, canonical.Hash)
	if err != nil {
		return err
	}
	if !ok || stored.MediaType != canonical.MediaType || stored.ContentBytes != canonical.ContentBytes || !bytes.Equal(stored.Content, canonical.Content) {
		return profile.ErrInvalidProfile
	}
	return nil
}

// LoadBlob returns a newly allocated byte slice after rechecking persisted
// count and content hash integrity.
func (r *Repository) LoadBlob(ctx context.Context, tenantID, contentHash string) (profile.Blob, bool, error) {
	if err := validateProfileRepository(r, ctx); err != nil {
		return profile.Blob{}, false, err
	}
	if !validProfileIdentifier(tenantID) || !validProfileHash(contentHash) {
		return profile.Blob{}, false, profile.ErrInvalidProfile
	}
	return r.loadBlob(ctx, r.pool, tenantID, contentHash)
}

func (r *Repository) loadBlob(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, tenantID, contentHash string) (profile.Blob, bool, error) {
	var mediaType string
	var content []byte
	var contentBytes int
	err := queryer.QueryRow(ctx, `SELECT media_type,content,content_bytes
		FROM profile_blobs WHERE tenant_id=$1 AND content_hash=$2`, tenantID, contentHash).
		Scan(&mediaType, &content, &contentBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		return profile.Blob{}, false, nil
	}
	if err != nil {
		return profile.Blob{}, false, mapProfileStoreError(ctx, err, nil)
	}
	stored := profile.Blob{MediaType: mediaType, Content: append([]byte(nil), content...), ContentBytes: contentBytes, Hash: contentHash}
	canonical, canonicalErr := stored.Canonical()
	if canonicalErr != nil || canonical.Hash != contentHash || canonical.ContentBytes != contentBytes || canonical.MediaType != mediaType {
		return profile.Blob{}, false, profile.ErrInvalidProfile
	}
	return canonical.Clone(), true, nil
}

func (r *Repository) LoadDraft(ctx context.Context, tenantID string, profileID profile.ID) (profile.Draft, bool, error) {
	if err := validateProfileRepository(r, ctx); err != nil {
		return profile.Draft{}, false, err
	}
	if !validProfileIdentifier(tenantID) || profileID.Validate() != nil {
		return profile.Draft{}, false, profile.ErrInvalidProfile
	}
	draft, err := scanProfileDraft(r.pool.QueryRow(ctx, `SELECT tenant_id,profile_id,generation,manifest::text,updated_at,updated_by
		FROM profile_drafts WHERE tenant_id=$1 AND profile_id=$2`, tenantID, profileID))
	if errors.Is(err, pgx.ErrNoRows) {
		return profile.Draft{}, false, nil
	}
	if err != nil {
		return profile.Draft{}, false, mapProfileStoreError(ctx, err, nil)
	}
	if draft.TenantID != tenantID || draft.ID != profileID {
		return profile.Draft{}, false, profile.ErrInvalidProfile
	}
	return draft.Clone(), true, nil
}

func (r *Repository) SaveDraft(ctx context.Context, draft profile.Draft, expectedGeneration int64) (profile.Draft, error) {
	if err := validateProfileRepository(r, ctx); err != nil {
		return profile.Draft{}, err
	}
	owned := draft.Clone()
	if expectedGeneration < 0 || expectedGeneration > 0 && owned.Generation != expectedGeneration || owned.Validate() != nil || !validProfileTime(owned.UpdatedAt) {
		return profile.Draft{}, profile.ErrInvalidProfile
	}
	manifestJSON, err := encodeProfileDocument(owned.Manifest)
	if err != nil {
		return profile.Draft{}, err
	}
	owned.UpdatedAt = canonicalProfileTime(owned.UpdatedAt)
	var saved profile.Draft
	if expectedGeneration == 0 {
		saved, err = scanProfileDraft(r.pool.QueryRow(ctx, `INSERT INTO profile_drafts
			(tenant_id,profile_id,generation,manifest,updated_at,updated_by)
			VALUES ($1,$2,1,$3::jsonb,$4,$5)
			RETURNING tenant_id,profile_id,generation,manifest::text,updated_at,updated_by`,
			owned.TenantID, owned.ID, manifestJSON, owned.UpdatedAt, owned.UpdatedBy))
		if err != nil {
			return profile.Draft{}, mapProfileStoreError(ctx, err, profile.ErrStaleDraft)
		}
	} else {
		saved, err = scanProfileDraft(r.pool.QueryRow(ctx, `UPDATE profile_drafts SET
			generation=generation+1,manifest=$4::jsonb,updated_at=$5,updated_by=$6
			WHERE tenant_id=$1 AND profile_id=$2 AND generation=$3
			RETURNING tenant_id,profile_id,generation,manifest::text,updated_at,updated_by`,
			owned.TenantID, owned.ID, expectedGeneration, manifestJSON, owned.UpdatedAt, owned.UpdatedBy))
		if errors.Is(err, pgx.ErrNoRows) {
			return profile.Draft{}, profile.ErrStaleDraft
		}
		if err != nil {
			return profile.Draft{}, mapProfileStoreError(ctx, err, profile.ErrStaleDraft)
		}
	}
	return saved.Clone(), nil
}

func (r *Repository) SaveTemplate(
	ctx context.Context,
	tenantID string,
	profileID profile.ID,
	logicalName string,
	source []byte,
	expectedGeneration int64,
	actor string,
) (profile.Draft, error) {
	if err := validateProfileRepository(r, ctx); err != nil {
		return profile.Draft{}, err
	}
	ownedSource := append([]byte(nil), source...)
	if !validProfileIdentifier(tenantID) || profileID.Validate() != nil || profile.ValidateTemplateName(logicalName) != nil ||
		!validProfileIdentifier(actor) || expectedGeneration <= 0 || len(ownedSource) > prompttmpl.MaxTemplateBytes || !utf8.Valid(ownedSource) {
		return profile.Draft{}, profile.ErrInvalidProfile
	}
	blob, err := (profile.Blob{MediaType: storedTemplateMediaType, Content: ownedSource}).Canonical()
	if err != nil {
		return profile.Draft{}, profile.ErrInvalidProfile
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return profile.Draft{}, mapProfileStoreError(ctx, err, nil)
	}
	defer rollback(tx)
	draft, err := loadProfileDraftForUpdate(ctx, tx, tenantID, profileID, expectedGeneration)
	if err != nil {
		return profile.Draft{}, err
	}
	if err := putProfileBlobTx(ctx, tx, tenantID, blob, canonicalProfileTime(r.clock.Now())); err != nil {
		return profile.Draft{}, err
	}
	if err := r.profileAfter(ctx, "profile_template_blob"); err != nil {
		return profile.Draft{}, err
	}
	draft.Manifest = draft.Manifest.Clone()
	if draft.Manifest.Bundle.Templates == nil {
		draft.Manifest.Bundle.Templates = make(map[string]profile.TemplateRef)
	}
	draft.Manifest.Bundle.Templates[logicalName] = profile.TemplateRef{Hash: blob.Hash, MediaType: blob.MediaType, Bytes: blob.ContentBytes}
	if draft.Manifest.Validate() != nil {
		return profile.Draft{}, profile.ErrInvalidProfile
	}
	draft.Generation = expectedGeneration + 1
	draft.UpdatedAt = canonicalProfileTime(r.clock.Now())
	draft.UpdatedBy = actor
	if !validProfileTime(draft.UpdatedAt) {
		return profile.Draft{}, profile.ErrInvalidProfile
	}
	manifestJSON, err := encodeProfileDocument(draft.Manifest)
	if err != nil {
		return profile.Draft{}, err
	}
	storedDraft, err := scanProfileDraft(tx.QueryRow(ctx, `UPDATE profile_drafts SET
		generation=generation+1,manifest=$4::jsonb,updated_at=$5,updated_by=$6
		WHERE tenant_id=$1 AND profile_id=$2 AND generation=$3
		RETURNING tenant_id,profile_id,generation,manifest::text,updated_at,updated_by`,
		tenantID, profileID, expectedGeneration, manifestJSON, draft.UpdatedAt, actor))
	if errors.Is(err, pgx.ErrNoRows) {
		return profile.Draft{}, profile.ErrStaleDraft
	}
	if err != nil {
		return profile.Draft{}, mapProfileStoreError(ctx, err, profile.ErrStaleDraft)
	}
	if err := r.profileAfter(ctx, "profile_template_draft"); err != nil {
		return profile.Draft{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return profile.Draft{}, mapProfileStoreError(ctx, err, nil)
	}
	return storedDraft.Clone(), nil
}

func (r *Repository) DeleteTemplate(
	ctx context.Context,
	tenantID string,
	profileID profile.ID,
	logicalName string,
	expectedGeneration int64,
	actor string,
) (profile.Draft, error) {
	if err := validateProfileRepository(r, ctx); err != nil {
		return profile.Draft{}, err
	}
	if !validProfileIdentifier(tenantID) || profileID.Validate() != nil || profile.ValidateTemplateName(logicalName) != nil ||
		!validProfileIdentifier(actor) || expectedGeneration <= 0 {
		return profile.Draft{}, profile.ErrInvalidProfile
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return profile.Draft{}, mapProfileStoreError(ctx, err, nil)
	}
	defer rollback(tx)
	draft, err := loadProfileDraftForUpdate(ctx, tx, tenantID, profileID, expectedGeneration)
	if err != nil {
		return profile.Draft{}, err
	}
	draft.Manifest = draft.Manifest.Clone()
	if _, exists := draft.Manifest.Bundle.Templates[logicalName]; !exists {
		return profile.Draft{}, profile.ErrInvalidProfile
	}
	delete(draft.Manifest.Bundle.Templates, logicalName)
	if draft.Manifest.Validate() != nil {
		return profile.Draft{}, profile.ErrInvalidProfile
	}
	draft.Generation = expectedGeneration + 1
	draft.UpdatedAt = canonicalProfileTime(r.clock.Now())
	draft.UpdatedBy = actor
	if !validProfileTime(draft.UpdatedAt) {
		return profile.Draft{}, profile.ErrInvalidProfile
	}
	manifestJSON, err := encodeProfileDocument(draft.Manifest)
	if err != nil {
		return profile.Draft{}, err
	}
	storedDraft, err := scanProfileDraft(tx.QueryRow(ctx, `UPDATE profile_drafts SET
		generation=generation+1,manifest=$4::jsonb,updated_at=$5,updated_by=$6
		WHERE tenant_id=$1 AND profile_id=$2 AND generation=$3
		RETURNING tenant_id,profile_id,generation,manifest::text,updated_at,updated_by`,
		tenantID, profileID, expectedGeneration, manifestJSON, draft.UpdatedAt, actor))
	if errors.Is(err, pgx.ErrNoRows) {
		return profile.Draft{}, profile.ErrStaleDraft
	}
	if err != nil {
		return profile.Draft{}, mapProfileStoreError(ctx, err, profile.ErrStaleDraft)
	}
	if err := r.profileAfter(ctx, "profile_template_deleted"); err != nil {
		return profile.Draft{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return profile.Draft{}, mapProfileStoreError(ctx, err, nil)
	}
	return storedDraft.Clone(), nil
}

func (r *Repository) NextVersion(ctx context.Context, tenantID string, profileID profile.ID) (uint64, error) {
	if err := validateProfileRepository(r, ctx); err != nil {
		return 0, err
	}
	if !validProfileIdentifier(tenantID) || profileID.Validate() != nil {
		return 0, profile.ErrInvalidProfile
	}
	var maximum int64
	if err := r.pool.QueryRow(ctx, `SELECT COALESCE(MAX(version),0)
		FROM profile_versions WHERE tenant_id=$1 AND profile_id=$2`, tenantID, profileID).Scan(&maximum); err != nil {
		return 0, mapProfileStoreError(ctx, err, nil)
	}
	if maximum < 0 || maximum == math.MaxInt64 {
		return 0, profile.ErrPublicationConflict
	}
	return uint64(maximum + 1), nil
}

func (r *Repository) Publish(ctx context.Context, record profile.PublishRecord) (profile.Published, error) {
	if err := validateProfileRepository(r, ctx); err != nil {
		return profile.Published{}, err
	}
	owned := record
	owned.Manifest = record.Manifest.Clone()
	owned.Snapshot = record.Snapshot.Clone()
	if owned.Validate() != nil || owned.ExpectedGeneration <= 0 || owned.DraftGeneration != owned.ExpectedGeneration ||
		owned.Snapshot.Ref.Version > math.MaxInt64 || !validProfileTime(owned.PublishedAt) {
		return profile.Published{}, profile.ErrInvalidProfile
	}
	owned.PublishedAt = canonicalProfileTime(owned.PublishedAt)
	manifestJSON, err := encodeProfileDocument(owned.Manifest)
	if err != nil {
		return profile.Published{}, err
	}
	snapshotJSON, err := encodeProfileDocument(owned.Snapshot)
	if err != nil {
		return profile.Published{}, err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return profile.Published{}, mapProfileStoreError(ctx, err, nil)
	}
	defer rollback(tx)
	draft, err := loadProfileDraftForUpdate(ctx, tx, owned.TenantID, owned.ProfileID, owned.ExpectedGeneration)
	if err != nil {
		return profile.Published{}, err
	}
	draftJSON, err := encodeProfileDocument(draft.Manifest)
	if err != nil {
		return profile.Published{}, err
	}
	if !bytes.Equal(draftJSON, manifestJSON) {
		return profile.Published{}, profile.ErrStaleDraft
	}
	if err := validatePublishedBlobRefs(ctx, tx, owned.TenantID, owned.Manifest); err != nil {
		return profile.Published{}, err
	}
	var maximum int64
	if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(version),0)
		FROM profile_versions WHERE tenant_id=$1 AND profile_id=$2`, owned.TenantID, owned.ProfileID).Scan(&maximum); err != nil {
		return profile.Published{}, mapProfileStoreError(ctx, err, nil)
	}
	if maximum < 0 || maximum == math.MaxInt64 || owned.Snapshot.Ref.Version != uint64(maximum+1) {
		return profile.Published{}, profile.ErrPublicationConflict
	}
	if owned.Activate {
		if _, err := tx.Exec(ctx, `UPDATE profile_versions SET active=false
			WHERE tenant_id=$1 AND profile_id=$2 AND active`, owned.TenantID, owned.ProfileID); err != nil {
			return profile.Published{}, mapProfileStoreError(ctx, err, nil)
		}
		if err := r.profileAfter(ctx, "profile_publish_deactivated"); err != nil {
			return profile.Published{}, err
		}
	}
	var activatedAt any
	var activatedBy any
	if owned.Activate {
		activatedAt = owned.PublishedAt
		activatedBy = owned.Actor
	}
	_, err = tx.Exec(ctx, `INSERT INTO profile_versions (
		tenant_id,profile_id,version,draft_generation,engine_version,snapshot_hash,manifest,snapshot,
		active,published_at,published_by,activated_at,activated_by)
		VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,$9,$10,$11,$12,$13)`,
		owned.TenantID, owned.ProfileID, int64(owned.Snapshot.Ref.Version), owned.DraftGeneration,
		owned.EngineVersion, owned.Snapshot.SnapshotHash, manifestJSON, snapshotJSON, owned.Activate,
		owned.PublishedAt, owned.Actor, activatedAt, activatedBy)
	if err != nil {
		return profile.Published{}, mapProfileStoreError(ctx, err, profile.ErrPublicationConflict)
	}
	if err := r.profileAfter(ctx, "profile_publish_inserted"); err != nil {
		return profile.Published{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return profile.Published{}, mapProfileStoreError(ctx, err, nil)
	}
	published := profile.Published{
		TenantID: owned.TenantID, Ref: owned.Snapshot.Ref,
		DraftGeneration: owned.DraftGeneration, EngineVersion: owned.EngineVersion,
		Snapshot: owned.Snapshot.Clone(), Manifest: owned.Manifest.Clone(), Active: owned.Activate,
		PublishedAt: owned.PublishedAt, PublishedBy: owned.Actor,
	}
	return published.Clone(), nil
}

func (r *Repository) Resolve(ctx context.Context, tenantID string, profileID profile.ID, version uint64) (profile.Published, bool, error) {
	if err := validateProfileRepository(r, ctx); err != nil {
		return profile.Published{}, false, err
	}
	if !validProfileIdentifier(tenantID) || profileID.Validate() != nil || version == 0 || version > math.MaxInt64 {
		return profile.Published{}, false, profile.ErrInvalidProfile
	}
	published, err := scanProfilePublished(r.pool.QueryRow(ctx, `SELECT
		tenant_id,profile_id,version,draft_generation,engine_version,snapshot_hash,
		manifest::text,snapshot::text,active,revoked_at,revoked_by,published_at,published_by,activated_at,activated_by
		FROM profile_versions WHERE tenant_id=$1 AND profile_id=$2 AND version=$3`, tenantID, profileID, int64(version)))
	if errors.Is(err, pgx.ErrNoRows) {
		return profile.Published{}, false, nil
	}
	if err != nil {
		if errors.Is(err, profile.ErrInvalidProfile) {
			return profile.Published{}, false, err
		}
		return profile.Published{}, false, mapProfileStoreError(ctx, err, nil)
	}
	if published.TenantID != tenantID || published.Ref != (profile.VersionRef{ID: profileID, Version: version}) {
		return profile.Published{}, false, profile.ErrInvalidProfile
	}
	return published.Clone(), true, nil
}

type publishedCursor struct {
	ID      profile.ID `json:"i"`
	Version uint64     `json:"v"`
}

func (r *Repository) ListPublished(ctx context.Context, tenantID string, pageSize int, cursor string) ([]profile.PublishedSummary, string, error) {
	if err := validateProfileRepository(r, ctx); err != nil {
		return nil, "", err
	}
	if !validProfileIdentifier(tenantID) || pageSize <= 0 || pageSize > maxProfilePageSize || len(cursor) > maxProfileCursorBytes {
		return nil, "", profile.ErrInvalidProfile
	}
	after, err := decodePublishedCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	rows, err := r.pool.Query(ctx, `SELECT profile_id,version,active,revoked_at,published_at,engine_version,snapshot_hash
		FROM profile_versions
		WHERE tenant_id=$1 AND ($2='' OR profile_id>$2 OR (profile_id=$2 AND version>$3))
		ORDER BY profile_id,version LIMIT $4`, tenantID, after.ID, int64(after.Version), pageSize+1)
	if err != nil {
		return nil, "", mapProfileStoreError(ctx, err, nil)
	}
	defer rows.Close()
	summaries := make([]profile.PublishedSummary, 0, pageSize+1)
	for rows.Next() {
		var id string
		var version int64
		var active bool
		var revoked sql.NullTime
		var publishedAt time.Time
		var engineVersion, snapshotHash string
		if err := rows.Scan(&id, &version, &active, &revoked, &publishedAt, &engineVersion, &snapshotHash); err != nil {
			return nil, "", mapProfileStoreError(ctx, err, nil)
		}
		ref := profile.VersionRef{ID: profile.ID(id), Version: uint64(version)}
		if version <= 0 || ref.Validate() != nil || !validProfileIdentifier(engineVersion) || !validProfileHash(snapshotHash) ||
			!validProfileTime(publishedAt) || active && revoked.Valid || revoked.Valid && !validProfileTime(revoked.Time) {
			return nil, "", profile.ErrInvalidProfile
		}
		summary := profile.PublishedSummary{
			Ref: ref, Active: active, PublishedAt: publishedAt.UTC(), EngineVersion: engineVersion, SnapshotHash: snapshotHash,
		}
		if revoked.Valid {
			value := revoked.Time.UTC()
			summary.RevokedAt = &value
		}
		summaries = append(summaries, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, "", mapProfileStoreError(ctx, err, nil)
	}
	next := ""
	if len(summaries) > pageSize {
		last := summaries[pageSize-1].Ref
		next = encodePublishedCursor(publishedCursor{ID: last.ID, Version: last.Version})
		summaries = summaries[:pageSize]
	}
	return clonePublishedSummaries(summaries), next, nil
}

func (r *Repository) Activate(ctx context.Context, tenantID string, profileID profile.ID, version uint64, actor string) error {
	if err := validateProfileRepository(r, ctx); err != nil {
		return err
	}
	if !validProfileIdentifier(tenantID) || profileID.Validate() != nil || version == 0 || version > math.MaxInt64 || !validProfileIdentifier(actor) {
		return profile.ErrInvalidProfile
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return mapProfileStoreError(ctx, err, nil)
	}
	defer rollback(tx)
	if err := lockProfileDraft(ctx, tx, tenantID, profileID); err != nil {
		return err
	}
	var active bool
	var revokedAt sql.NullTime
	var publishedAt time.Time
	err = tx.QueryRow(ctx, `SELECT active,revoked_at,published_at FROM profile_versions
		WHERE tenant_id=$1 AND profile_id=$2 AND version=$3 FOR UPDATE`, tenantID, profileID, int64(version)).
		Scan(&active, &revokedAt, &publishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return profile.ErrInvalidProfile
	}
	if err != nil {
		return mapProfileStoreError(ctx, err, nil)
	}
	if revokedAt.Valid {
		return profile.ErrProfileRevoked
	}
	if active {
		return nil
	}
	activatedAt := canonicalProfileTime(r.clock.Now())
	if !validProfileTime(activatedAt) || activatedAt.Before(publishedAt) {
		return profile.ErrInvalidProfile
	}
	if _, err := tx.Exec(ctx, `UPDATE profile_versions SET active=false
		WHERE tenant_id=$1 AND profile_id=$2 AND active`, tenantID, profileID); err != nil {
		return mapProfileStoreError(ctx, err, nil)
	}
	if err := r.profileAfter(ctx, "profile_activate_deactivated"); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE profile_versions SET active=true,activated_at=$4,activated_by=$5
		WHERE tenant_id=$1 AND profile_id=$2 AND version=$3 AND revoked_at IS NULL`,
		tenantID, profileID, int64(version), activatedAt, actor)
	if err != nil {
		return mapProfileStoreError(ctx, err, profile.ErrPublicationConflict)
	}
	if tag.RowsAffected() != 1 {
		return profile.ErrProfileRevoked
	}
	if err := r.profileAfter(ctx, "profile_activated"); err != nil {
		return err
	}
	return mapProfileStoreError(ctx, tx.Commit(ctx), nil)
}

func (r *Repository) Revoke(ctx context.Context, tenantID string, profileID profile.ID, version uint64, actor string, revokedAt time.Time) error {
	if err := validateProfileRepository(r, ctx); err != nil {
		return err
	}
	if !validProfileIdentifier(tenantID) || profileID.Validate() != nil || version == 0 || version > math.MaxInt64 ||
		!validProfileIdentifier(actor) || !validProfileTime(revokedAt) {
		return profile.ErrInvalidProfile
	}
	revokedAt = canonicalProfileTime(revokedAt)
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return mapProfileStoreError(ctx, err, nil)
	}
	defer rollback(tx)
	if err := lockProfileDraft(ctx, tx, tenantID, profileID); err != nil {
		return err
	}
	var storedAt sql.NullTime
	var storedBy sql.NullString
	var publishedAt time.Time
	err = tx.QueryRow(ctx, `SELECT revoked_at,revoked_by,published_at FROM profile_versions
		WHERE tenant_id=$1 AND profile_id=$2 AND version=$3 FOR UPDATE`, tenantID, profileID, int64(version)).
		Scan(&storedAt, &storedBy, &publishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return profile.ErrInvalidProfile
	}
	if err != nil {
		return mapProfileStoreError(ctx, err, nil)
	}
	if storedAt.Valid || storedBy.Valid {
		if storedAt.Valid && storedBy.Valid && storedAt.Time.Equal(revokedAt) && storedBy.String == actor {
			return nil
		}
		return profile.ErrPublicationConflict
	}
	if revokedAt.Before(publishedAt) {
		return profile.ErrInvalidProfile
	}
	tag, err := tx.Exec(ctx, `UPDATE profile_versions SET revoked_at=$4,revoked_by=$5,active=false
		WHERE tenant_id=$1 AND profile_id=$2 AND version=$3 AND revoked_at IS NULL`,
		tenantID, profileID, int64(version), revokedAt, actor)
	if err != nil {
		return mapProfileStoreError(ctx, err, profile.ErrPublicationConflict)
	}
	if tag.RowsAffected() != 1 {
		return profile.ErrPublicationConflict
	}
	if err := r.profileAfter(ctx, "profile_revoked"); err != nil {
		return err
	}
	return mapProfileStoreError(ctx, tx.Commit(ctx), nil)
}

func loadProfileDraftForUpdate(ctx context.Context, tx pgx.Tx, tenantID string, profileID profile.ID, expectedGeneration int64) (profile.Draft, error) {
	draft, err := scanProfileDraft(tx.QueryRow(ctx, `SELECT tenant_id,profile_id,generation,manifest::text,updated_at,updated_by
		FROM profile_drafts WHERE tenant_id=$1 AND profile_id=$2 FOR UPDATE`, tenantID, profileID))
	if errors.Is(err, pgx.ErrNoRows) {
		return profile.Draft{}, profile.ErrStaleDraft
	}
	if err != nil {
		return profile.Draft{}, mapProfileStoreError(ctx, err, nil)
	}
	if draft.TenantID != tenantID || draft.ID != profileID || draft.Generation != expectedGeneration {
		return profile.Draft{}, profile.ErrStaleDraft
	}
	return draft, nil
}

func lockProfileDraft(ctx context.Context, tx pgx.Tx, tenantID string, profileID profile.ID) error {
	var present int
	err := tx.QueryRow(ctx, `SELECT 1 FROM profile_drafts
		WHERE tenant_id=$1 AND profile_id=$2 FOR UPDATE`, tenantID, profileID).Scan(&present)
	if errors.Is(err, pgx.ErrNoRows) {
		return profile.ErrInvalidProfile
	}
	if err != nil {
		return mapProfileStoreError(ctx, err, nil)
	}
	return nil
}

func scanProfileDraft(row pgx.Row) (profile.Draft, error) {
	var tenantID, profileID, updatedBy string
	var generation int64
	var manifestJSON []byte
	var updatedAt time.Time
	if err := row.Scan(&tenantID, &profileID, &generation, &manifestJSON, &updatedAt, &updatedBy); err != nil {
		return profile.Draft{}, err
	}
	var manifest profile.Manifest
	if decodeProfileDocument(manifestJSON, &manifest) != nil {
		return profile.Draft{}, profile.ErrInvalidProfile
	}
	draft := profile.Draft{
		TenantID: tenantID, ID: profile.ID(profileID), Generation: generation,
		Manifest: manifest, UpdatedAt: updatedAt.UTC(), UpdatedBy: updatedBy,
	}
	if generation <= 0 || !validProfileTime(updatedAt) || draft.Validate() != nil {
		return profile.Draft{}, profile.ErrInvalidProfile
	}
	return draft.Clone(), nil
}

func putProfileBlobTx(ctx context.Context, tx pgx.Tx, tenantID string, blob profile.Blob, createdAt time.Time) error {
	if !validProfileTime(createdAt) {
		return profile.ErrInvalidProfile
	}
	tag, err := tx.Exec(ctx, `INSERT INTO profile_blobs
		(tenant_id,content_hash,media_type,content,content_bytes,created_at)
		VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (tenant_id,content_hash) DO NOTHING`,
		tenantID, blob.Hash, blob.MediaType, nonNilProfileBytes(blob.Content), blob.ContentBytes, createdAt)
	if err != nil {
		return mapProfileStoreError(ctx, err, profile.ErrInvalidProfile)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var mediaType string
	var content []byte
	var contentBytes int
	err = tx.QueryRow(ctx, `SELECT media_type,content,content_bytes FROM profile_blobs
		WHERE tenant_id=$1 AND content_hash=$2`, tenantID, blob.Hash).Scan(&mediaType, &content, &contentBytes)
	if err != nil {
		return mapProfileStoreError(ctx, err, nil)
	}
	if mediaType != blob.MediaType || contentBytes != blob.ContentBytes || !bytes.Equal(content, blob.Content) {
		return profile.ErrInvalidProfile
	}
	return nil
}

func validatePublishedBlobRefs(ctx context.Context, tx pgx.Tx, tenantID string, manifest profile.Manifest) error {
	refs := make(map[string]profile.TemplateRef, len(manifest.Bundle.Templates)+len(manifest.Bundle.Fixtures))
	for _, collection := range []map[string]profile.TemplateRef{manifest.Bundle.Templates, manifest.Bundle.Fixtures} {
		for _, ref := range collection {
			if existing, ok := refs[ref.Hash]; ok && existing != ref {
				return profile.ErrInvalidProfile
			}
			refs[ref.Hash] = ref
		}
	}
	for hash, ref := range refs {
		var mediaType string
		var content []byte
		var contentBytes int
		err := tx.QueryRow(ctx, `SELECT media_type,content,content_bytes FROM profile_blobs
			WHERE tenant_id=$1 AND content_hash=$2`, tenantID, hash).Scan(&mediaType, &content, &contentBytes)
		if errors.Is(err, pgx.ErrNoRows) {
			return profile.ErrInvalidProfile
		}
		if err != nil {
			return mapProfileStoreError(ctx, err, nil)
		}
		sum := sha256.Sum256(content)
		if hex.EncodeToString(sum[:]) != hash || mediaType != ref.MediaType || contentBytes != ref.Bytes || contentBytes != len(content) {
			return profile.ErrInvalidProfile
		}
	}
	return nil
}

func scanProfilePublished(row pgx.Row) (profile.Published, error) {
	var tenantID, profileID, engineVersion, snapshotHash, publishedBy string
	var version, draftGeneration int64
	var manifestJSON, snapshotJSON []byte
	var active bool
	var revokedAt, activatedAt sql.NullTime
	var revokedBy, activatedBy sql.NullString
	var publishedAt time.Time
	if err := row.Scan(
		&tenantID, &profileID, &version, &draftGeneration, &engineVersion, &snapshotHash,
		&manifestJSON, &snapshotJSON, &active, &revokedAt, &revokedBy, &publishedAt, &publishedBy, &activatedAt, &activatedBy,
	); err != nil {
		return profile.Published{}, err
	}
	var manifest profile.Manifest
	var snapshot profile.Snapshot
	if decodeProfileDocument(manifestJSON, &manifest) != nil || decodeProfileDocument(snapshotJSON, &snapshot) != nil {
		return profile.Published{}, profile.ErrInvalidProfile
	}
	if version <= 0 || draftGeneration <= 0 || !validProfileTime(publishedAt) ||
		revokedAt.Valid != revokedBy.Valid || activatedAt.Valid != activatedBy.Valid ||
		revokedAt.Valid && (!validProfileTime(revokedAt.Time) || !validProfileIdentifier(revokedBy.String) || revokedAt.Time.Before(publishedAt)) ||
		activatedAt.Valid && (!validProfileTime(activatedAt.Time) || !validProfileIdentifier(activatedBy.String) || activatedAt.Time.Before(publishedAt)) ||
		active && (revokedAt.Valid || !activatedAt.Valid) {
		return profile.Published{}, profile.ErrInvalidProfile
	}
	published := profile.Published{
		TenantID: tenantID, Ref: profile.VersionRef{ID: profile.ID(profileID), Version: uint64(version)},
		DraftGeneration: draftGeneration, EngineVersion: engineVersion,
		Snapshot: snapshot, Manifest: manifest, Active: active,
		PublishedAt: publishedAt.UTC(), PublishedBy: publishedBy,
	}
	if revokedAt.Valid {
		value := revokedAt.Time.UTC()
		published.RevokedAt = &value
	}
	if snapshotHash != snapshot.SnapshotHash || published.Validate() != nil {
		return profile.Published{}, profile.ErrInvalidProfile
	}
	return published.Clone(), nil
}

func encodeProfileDocument(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, profile.ErrInvalidProfile
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var generic any
	if err := decoder.Decode(&generic); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return nil, profile.ErrInvalidProfile
	}
	canonical, err := json.Marshal(generic)
	if err != nil || len(canonical) < 2 || len(canonical) > maxProfileDocumentBytes {
		return nil, profile.ErrInvalidProfile
	}
	return canonical, nil
}

func decodeProfileDocument(raw []byte, target any) error {
	if len(raw) < 2 || len(raw) > maxProfileDocumentBytes || !utf8.Valid(raw) {
		return profile.ErrInvalidProfile
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return profile.ErrInvalidProfile
	}
	return nil
}

func decodePublishedCursor(encoded string) (publishedCursor, error) {
	if encoded == "" {
		return publishedCursor{}, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(raw) == 0 || len(raw) > maxProfileCursorBytes {
		return publishedCursor{}, profile.ErrInvalidProfile
	}
	var cursor publishedCursor
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cursor); err != nil || decoder.Decode(&struct{}{}) != io.EOF || cursor.ID.Validate() != nil || cursor.Version == 0 || cursor.Version > math.MaxInt64 {
		return publishedCursor{}, profile.ErrInvalidProfile
	}
	return cursor, nil
}

func encodePublishedCursor(cursor publishedCursor) string {
	raw, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func clonePublishedSummaries(source []profile.PublishedSummary) []profile.PublishedSummary {
	result := make([]profile.PublishedSummary, len(source))
	copy(result, source)
	for index := range result {
		if result[index].RevokedAt != nil {
			value := *result[index].RevokedAt
			result[index].RevokedAt = &value
		}
	}
	return result
}

func nonNilProfileBytes(source []byte) []byte {
	if source == nil {
		return []byte{}
	}
	return source
}

func validateProfileRepository(r *Repository, ctx context.Context) error {
	if ctx == nil || r == nil || r.pool == nil || nilInterface(r.clock) {
		return profile.ErrInvalidProfile
	}
	return ctx.Err()
}

func validProfileIdentifier(value string) bool {
	if value == "" || len(value) > maxIdentityBytes || !utf8.ValidString(value) {
		return false
	}
	for _, runeValue := range value {
		if unicode.IsControl(runeValue) {
			return false
		}
	}
	return true
}

func validProfileHash(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, character := range []byte(value) {
		if !(character >= '0' && character <= '9' || character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

func validProfileTime(value time.Time) bool {
	return !value.IsZero() && value.Year() >= 1 && value.Year() <= 9999
}

func canonicalProfileTime(value time.Time) time.Time {
	return value.UTC().Truncate(time.Microsecond)
}

func mapProfileStoreError(ctx context.Context, err error, conflict error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, profile.ErrInvalidProfile) || errors.Is(err, profile.ErrStaleDraft) ||
		errors.Is(err, profile.ErrPublicationConflict) || errors.Is(err, profile.ErrProfileRevoked) {
		return err
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505":
			if conflict != nil {
				return conflict
			}
			return profile.ErrInvalidProfile
		case "23503", "23514", "22001", "22003", "22P02":
			return profile.ErrInvalidProfile
		}
	}
	return errProfileStoreUnavailable
}

func (r *Repository) profileAfter(ctx context.Context, step string) error {
	if err := r.after(step); err != nil {
		return mapProfileStoreError(ctx, err, nil)
	}
	return nil
}
