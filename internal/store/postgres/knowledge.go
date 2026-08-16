package postgres

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/knowledge"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const maxKnowledgePageSize = 100

var _ knowledge.Store = knowledgeStore{}

// KnowledgeStore returns this repository's knowledge.Store view. Profiles and
// knowledge share one pool and one Repository, but their interfaces both name
// Publish and Resolve, so knowledge is reached through an explicit view rather
// than by renaming either domain's vocabulary.
func (r *Repository) KnowledgeStore() knowledge.Store { return knowledgeStore{repository: r} }

type knowledgeStore struct{ repository *Repository }

func (store knowledgeStore) CreateDraft(ctx context.Context, draft knowledge.Draft) (knowledge.Draft, error) {
	return store.repository.createKnowledgeDraft(ctx, draft)
}

func (store knowledgeStore) Publish(ctx context.Context, record knowledge.PublishRecord) (knowledge.Version, error) {
	return store.repository.publishKnowledge(ctx, record)
}

func (store knowledgeStore) Resolve(ctx context.Context, tenantID string, ref knowledge.VersionRef) (knowledge.Version, bool, error) {
	return store.repository.resolveKnowledge(ctx, tenantID, ref)
}

func (store knowledgeStore) List(ctx context.Context, tenantID, packID string, limit int) ([]knowledge.Version, error) {
	return store.repository.listKnowledge(ctx, tenantID, packID, limit)
}

func (store knowledgeStore) Tombstone(ctx context.Context, tenantID string, ref knowledge.VersionRef, actor string, at time.Time) error {
	return store.repository.tombstoneKnowledge(ctx, tenantID, ref, actor, at)
}

func (store knowledgeStore) Search(ctx context.Context, request knowledge.SearchRequest) ([]knowledge.Excerpt, error) {
	return store.repository.searchKnowledge(ctx, request)
}

// CreateDraft validates and returns normalized draft material without
// persisting it. Drafts are deliberately not durable: the operator previews the
// exact content hash, then publishes that hash, so an unreviewed draft can
// never become a published version by sitting in a table.
func (r *Repository) createKnowledgeDraft(ctx context.Context, draft knowledge.Draft) (knowledge.Draft, error) {
	if err := validateKnowledgeRepository(r, ctx); err != nil {
		return knowledge.Draft{}, err
	}
	candidate := knowledge.Version{
		TenantID:  draft.TenantID,
		Ref:       knowledge.VersionRef{PackID: draft.PackID, Version: 1},
		Title:     draft.Title,
		MediaType: draft.MediaType,
		Excerpts:  draft.Excerpts,
	}
	candidate.Ref.Hash = knowledge.ContentHash(candidate)
	if err := candidate.Validate(); err != nil {
		return knowledge.Draft{}, err
	}
	clone := draft.Clone()
	clone.Hash = contentHashAtVersion(candidate, 1)
	return clone, nil
}

// Publish allocates the next version number for the pack and writes the version
// and all of its excerpts in one transaction. The caller's previewed hash is
// recomputed against the allocated version number and must match, so material
// that was reviewed at one version cannot silently publish at another.
func (r *Repository) publishKnowledge(ctx context.Context, record knowledge.PublishRecord) (knowledge.Version, error) {
	if err := validateKnowledgeRepository(r, ctx); err != nil {
		return knowledge.Version{}, err
	}
	if !validProfileIdentifier(record.TenantID) || !validProfileIdentifier(record.PackID) {
		return knowledge.Version{}, knowledge.ErrInvalidKnowledge
	}
	if len(record.Excerpts) == 0 || len(record.Excerpts) > knowledge.MaxExcerpts {
		return knowledge.Version{}, knowledge.ErrContentLimit
	}

	transaction, err := r.pool.Begin(ctx)
	if err != nil {
		return knowledge.Version{}, mapKnowledgeStoreError(ctx, err)
	}
	defer func() { _ = transaction.Rollback(context.WithoutCancel(ctx)) }()

	var next int64
	if err := transaction.QueryRow(ctx, `
		SELECT COALESCE(MAX(version), 0) + 1
		FROM knowledge_versions
		WHERE tenant_id = $1 AND pack_id = $2`,
		record.TenantID, record.PackID).Scan(&next); err != nil {
		return knowledge.Version{}, mapKnowledgeStoreError(ctx, err)
	}
	if next <= 0 || uint64(next) > math.MaxInt64 {
		return knowledge.Version{}, knowledge.ErrInvalidKnowledge
	}

	version := knowledge.Version{
		TenantID:    record.TenantID,
		Ref:         knowledge.VersionRef{PackID: record.PackID, Version: uint64(next)},
		Title:       record.Title,
		MediaType:   record.MediaType,
		Excerpts:    record.Excerpts,
		PublishedAt: record.PublishedAt.UTC(),
		PublishedBy: record.PublishedBy,
	}
	version.Ref.Hash = knowledge.ContentHash(version)
	if err := version.Validate(); err != nil {
		return knowledge.Version{}, err
	}
	if record.Hash != "" && record.Hash != version.Ref.Hash {
		return knowledge.Version{}, knowledge.ErrInvalidKnowledge
	}

	contentBytes := 0
	for _, excerpt := range version.Excerpts {
		contentBytes += len(excerpt.Text)
	}
	if _, err := transaction.Exec(ctx, `
		INSERT INTO knowledge_versions (
			tenant_id, pack_id, version, content_hash, title, media_type,
			excerpt_count, content_bytes, published_at, published_by
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		version.TenantID, version.Ref.PackID, next, version.Ref.Hash, version.Title, version.MediaType,
		len(version.Excerpts), contentBytes, version.PublishedAt, version.PublishedBy); err != nil {
		return knowledge.Version{}, mapKnowledgeStoreError(ctx, err)
	}

	rows := make([][]any, 0, len(version.Excerpts))
	for _, excerpt := range version.Excerpts {
		rows = append(rows, []any{
			version.TenantID, version.Ref.PackID, next, excerpt.Ordinal,
			excerpt.Text, len(excerpt.Text), excerpt.Citation.ExcerptHash, excerpt.Tokens,
			excerpt.Citation.SourceTitle, excerpt.Citation.Section, excerpt.Citation.Page,
		})
	}
	if _, err := transaction.CopyFrom(ctx,
		pgx.Identifier{"knowledge_excerpts"},
		[]string{
			"tenant_id", "pack_id", "version", "ordinal",
			"body", "body_bytes", "excerpt_hash", "tokens",
			"source_title", "section", "page",
		},
		pgx.CopyFromRows(rows)); err != nil {
		return knowledge.Version{}, mapKnowledgeStoreError(ctx, err)
	}
	if err := transaction.Commit(ctx); err != nil {
		return knowledge.Version{}, mapKnowledgeStoreError(ctx, err)
	}
	return version.Clone(), nil
}

// Resolve returns one exact version. A reference that does not exist for this
// tenant reports ok=false with no error, so a caller cannot use error class to
// tell "wrong tenant" from "no such pack".
func (r *Repository) resolveKnowledge(ctx context.Context, tenantID string, ref knowledge.VersionRef) (knowledge.Version, bool, error) {
	if err := validateKnowledgeRepository(r, ctx); err != nil {
		return knowledge.Version{}, false, err
	}
	if !validProfileIdentifier(tenantID) || ref.Validate() != nil {
		return knowledge.Version{}, false, knowledge.ErrInvalidKnowledge
	}
	if ref.Version > math.MaxInt64 {
		return knowledge.Version{}, false, knowledge.ErrInvalidKnowledge
	}

	var version knowledge.Version
	var tombstonedAt *time.Time
	var tombstonedBy *string
	err := r.pool.QueryRow(ctx, `
		SELECT title, media_type, published_at, published_by, tombstoned_at, tombstoned_by
		FROM knowledge_versions
		WHERE tenant_id = $1 AND pack_id = $2 AND version = $3 AND content_hash = $4`,
		tenantID, ref.PackID, int64(ref.Version), ref.Hash).
		Scan(&version.Title, &version.MediaType, &version.PublishedAt, &version.PublishedBy, &tombstonedAt, &tombstonedBy)
	if errors.Is(err, pgx.ErrNoRows) {
		return knowledge.Version{}, false, nil
	}
	if err != nil {
		return knowledge.Version{}, false, mapKnowledgeStoreError(ctx, err)
	}
	version.TenantID = tenantID
	version.Ref = ref
	version.TombstonedAt = tombstonedAt
	if tombstonedBy != nil {
		version.TombstonedBy = *tombstonedBy
	}

	excerpts, err := r.loadKnowledgeExcerpts(ctx, tenantID, ref)
	if err != nil {
		return knowledge.Version{}, false, err
	}
	version.Excerpts = excerpts
	if version.Ref.Hash != knowledge.ContentHash(version) {
		// Stored content no longer hashes to the reference it is filed under.
		// Refuse rather than serve material that was not the reviewed bytes.
		return knowledge.Version{}, false, knowledge.ErrInvalidKnowledge
	}
	return version, true, nil
}

// List returns bounded version metadata for one pack, newest first. Excerpt
// bodies are deliberately excluded: listing is an operator overview, not a
// content read.
func (r *Repository) listKnowledge(ctx context.Context, tenantID, packID string, limit int) ([]knowledge.Version, error) {
	if err := validateKnowledgeRepository(r, ctx); err != nil {
		return nil, err
	}
	if !validProfileIdentifier(tenantID) || !validProfileIdentifier(packID) {
		return nil, knowledge.ErrInvalidKnowledge
	}
	if limit <= 0 || limit > maxKnowledgePageSize {
		limit = maxKnowledgePageSize
	}
	rows, err := r.pool.Query(ctx, `
		SELECT version, content_hash, title, media_type, published_at, published_by, tombstoned_at, tombstoned_by
		FROM knowledge_versions
		WHERE tenant_id = $1 AND pack_id = $2
		ORDER BY version DESC
		LIMIT $3`, tenantID, packID, limit)
	if err != nil {
		return nil, mapKnowledgeStoreError(ctx, err)
	}
	defer rows.Close()

	versions := make([]knowledge.Version, 0, limit)
	for rows.Next() {
		var version knowledge.Version
		var number int64
		var tombstonedBy *string
		if err := rows.Scan(&number, &version.Ref.Hash, &version.Title, &version.MediaType,
			&version.PublishedAt, &version.PublishedBy, &version.TombstonedAt, &tombstonedBy); err != nil {
			return nil, mapKnowledgeStoreError(ctx, err)
		}
		if number <= 0 {
			return nil, knowledge.ErrInvalidKnowledge
		}
		version.TenantID = tenantID
		version.Ref.PackID = packID
		version.Ref.Version = uint64(number)
		if tombstonedBy != nil {
			version.TombstonedBy = *tombstonedBy
		}
		versions = append(versions, version)
	}
	if err := rows.Err(); err != nil {
		return nil, mapKnowledgeStoreError(ctx, err)
	}
	return versions, nil
}

// Tombstone marks one version unbindable. Excerpts are left intact so work that
// already pinned this version keeps resolving until retention deletes it.
func (r *Repository) tombstoneKnowledge(ctx context.Context, tenantID string, ref knowledge.VersionRef, actor string, at time.Time) error {
	if err := validateKnowledgeRepository(r, ctx); err != nil {
		return err
	}
	if !validProfileIdentifier(tenantID) || ref.Validate() != nil || !validProfileIdentifier(actor) {
		return knowledge.ErrInvalidKnowledge
	}
	if ref.Version > math.MaxInt64 {
		return knowledge.ErrInvalidKnowledge
	}
	tag, err := r.pool.Exec(ctx, `
		UPDATE knowledge_versions
		SET tombstoned_at = $5, tombstoned_by = $4
		WHERE tenant_id = $1 AND pack_id = $2 AND version = $3 AND tombstoned_at IS NULL`,
		tenantID, ref.PackID, int64(ref.Version), actor, at.UTC())
	if err != nil {
		return mapKnowledgeStoreError(ctx, err)
	}
	if tag.RowsAffected() == 0 {
		return knowledge.ErrKnowledgeNotFound
	}
	return nil
}

// Search returns excerpts from the exact pinned versions only. Ranking is not
// performed here: the caller ranks in Go so ordering is identical regardless of
// database collation or planner choices.
func (r *Repository) searchKnowledge(ctx context.Context, request knowledge.SearchRequest) ([]knowledge.Excerpt, error) {
	if err := validateKnowledgeRepository(r, ctx); err != nil {
		return nil, err
	}
	if !validProfileIdentifier(request.TenantID) || len(request.Refs) == 0 {
		return nil, knowledge.ErrInvalidKnowledge
	}
	excerpts := make([]knowledge.Excerpt, 0, len(request.Refs)*8)
	for _, ref := range request.Refs {
		if ref.Validate() != nil {
			return nil, knowledge.ErrInvalidKnowledge
		}
		version, ok, err := r.resolveKnowledge(ctx, request.TenantID, ref)
		if err != nil {
			return nil, err
		}
		if !ok {
			// A pinned version that no longer resolves is a hard failure: the
			// alternative is silently answering from less material than the
			// publication promised.
			return nil, knowledge.ErrKnowledgeNotFound
		}
		excerpts = append(excerpts, version.Excerpts...)
	}
	return excerpts, nil
}

func (r *Repository) loadKnowledgeExcerpts(ctx context.Context, tenantID string, ref knowledge.VersionRef) ([]knowledge.Excerpt, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT ordinal, body, excerpt_hash, tokens, source_title, section, page
		FROM knowledge_excerpts
		WHERE tenant_id = $1 AND pack_id = $2 AND version = $3
		ORDER BY ordinal ASC`, tenantID, ref.PackID, int64(ref.Version))
	if err != nil {
		return nil, mapKnowledgeStoreError(ctx, err)
	}
	defer rows.Close()

	excerpts := make([]knowledge.Excerpt, 0, 16)
	for rows.Next() {
		var excerpt knowledge.Excerpt
		if err := rows.Scan(&excerpt.Ordinal, &excerpt.Text, &excerpt.Citation.ExcerptHash,
			&excerpt.Tokens, &excerpt.Citation.SourceTitle, &excerpt.Citation.Section, &excerpt.Citation.Page); err != nil {
			return nil, mapKnowledgeStoreError(ctx, err)
		}
		excerpts = append(excerpts, excerpt)
	}
	if err := rows.Err(); err != nil {
		return nil, mapKnowledgeStoreError(ctx, err)
	}
	if len(excerpts) > knowledge.MaxExcerpts {
		return nil, knowledge.ErrContentLimit
	}
	return excerpts, nil
}

// contentHashAtVersion recomputes a version's identity for a specific version
// number without mutating the caller's value.
func contentHashAtVersion(version knowledge.Version, number uint64) string {
	clone := version.Clone()
	clone.Ref.Version = number
	return knowledge.ContentHash(clone)
}

func validateKnowledgeRepository(r *Repository, ctx context.Context) error {
	if ctx == nil || r == nil || r.pool == nil || nilInterface(r.clock) {
		return knowledge.ErrInvalidKnowledge
	}
	return ctx.Err()
}

// mapKnowledgeStoreError keeps constraint violations as content faults and
// everything else as an availability fault, so no dependency error text ever
// reaches a caller.
func mapKnowledgeStoreError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, knowledge.ErrInvalidKnowledge) || errors.Is(err, knowledge.ErrContentLimit) ||
		errors.Is(err, knowledge.ErrKnowledgeNotFound) || errors.Is(err, knowledge.ErrKnowledgeTombstoned) {
		return err
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505", "23503", "23514", "22001", "22003", "22P02":
			return knowledge.ErrInvalidKnowledge
		}
	}
	return knowledge.ErrStoreUnavailable
}
