CREATE TABLE profile_blobs (
    tenant_id TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    media_type TEXT NOT NULL,
    content BYTEA NOT NULL,
    content_bytes INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT profile_blobs_pkey PRIMARY KEY (tenant_id, content_hash),
    CONSTRAINT profile_blobs_identity_check CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 256
        AND tenant_id !~ '[[:cntrl:]]'
        AND content_hash ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT profile_blobs_media_type_check CHECK (
        octet_length(media_type) BETWEEN 1 AND 256
        AND media_type !~ '[[:cntrl:]]'
    ),
    CONSTRAINT profile_blobs_content_check CHECK (
        content_bytes BETWEEN 0 AND 524288
        AND octet_length(content) = content_bytes
    ),
    CONSTRAINT profile_blobs_created_at_check CHECK (isfinite(created_at))
);

CREATE TABLE profile_drafts (
    tenant_id TEXT NOT NULL,
    profile_id TEXT NOT NULL,
    generation BIGINT NOT NULL,
    manifest JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    updated_by TEXT NOT NULL,
    CONSTRAINT profile_drafts_pkey PRIMARY KEY (tenant_id, profile_id),
    CONSTRAINT profile_drafts_identity_check CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 256
        AND tenant_id !~ '[[:cntrl:]]'
        AND octet_length(profile_id) BETWEEN 1 AND 256
        AND profile_id !~ '[[:cntrl:]]'
    ),
    CONSTRAINT profile_drafts_generation_check CHECK (generation > 0),
    CONSTRAINT profile_drafts_manifest_check CHECK (
        jsonb_typeof(manifest) = 'object'
        AND octet_length(manifest::text) BETWEEN 2 AND 4194304
        AND canonical_json_depth_valid(manifest::json)
    ),
    CONSTRAINT profile_drafts_updated_by_check CHECK (
        octet_length(updated_by) BETWEEN 1 AND 256
        AND updated_by !~ '[[:cntrl:]]'
    ),
    CONSTRAINT profile_drafts_updated_at_check CHECK (isfinite(updated_at))
);

CREATE TABLE profile_versions (
    tenant_id TEXT NOT NULL,
    profile_id TEXT NOT NULL,
    version BIGINT NOT NULL,
    draft_generation BIGINT NOT NULL,
    engine_version TEXT NOT NULL,
    snapshot_hash TEXT NOT NULL,
    manifest JSONB NOT NULL,
    snapshot JSONB NOT NULL,
    active BOOLEAN NOT NULL DEFAULT false,
    revoked_at TIMESTAMPTZ,
    revoked_by TEXT,
    published_at TIMESTAMPTZ NOT NULL,
    published_by TEXT NOT NULL,
    activated_at TIMESTAMPTZ,
    activated_by TEXT,
    CONSTRAINT profile_versions_pkey PRIMARY KEY (tenant_id, profile_id, version),
    CONSTRAINT profile_versions_draft_generation_uniq UNIQUE (tenant_id, profile_id, draft_generation),
    CONSTRAINT profile_versions_identity_check CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 256
        AND tenant_id !~ '[[:cntrl:]]'
        AND octet_length(profile_id) BETWEEN 1 AND 256
        AND profile_id !~ '[[:cntrl:]]'
    ),
    CONSTRAINT profile_versions_numbers_check CHECK (version > 0 AND draft_generation > 0),
    CONSTRAINT profile_versions_engine_hash_check CHECK (
        octet_length(engine_version) BETWEEN 1 AND 256
        AND engine_version !~ '[[:cntrl:]]'
        AND snapshot_hash ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT profile_versions_documents_check CHECK (
        jsonb_typeof(manifest) = 'object'
        AND octet_length(manifest::text) BETWEEN 2 AND 4194304
        AND canonical_json_depth_valid(manifest::json)
        AND jsonb_typeof(snapshot) = 'object'
        AND octet_length(snapshot::text) BETWEEN 2 AND 4194304
        AND canonical_json_depth_valid(snapshot::json)
    ),
    CONSTRAINT profile_versions_publication_check CHECK (
        octet_length(published_by) BETWEEN 1 AND 256
        AND published_by !~ '[[:cntrl:]]'
        AND isfinite(published_at)
    ),
    CONSTRAINT profile_versions_activation_check CHECK (
        (activated_at IS NULL AND activated_by IS NULL)
        OR (
            activated_at IS NOT NULL AND activated_by IS NOT NULL
            AND isfinite(activated_at) AND activated_at >= published_at
            AND octet_length(activated_by) BETWEEN 1 AND 256
            AND activated_by !~ '[[:cntrl:]]'
        )
    ),
    CONSTRAINT profile_versions_revocation_check CHECK (
        (revoked_at IS NULL AND revoked_by IS NULL)
        OR (
            revoked_at IS NOT NULL AND revoked_by IS NOT NULL
            AND isfinite(revoked_at) AND revoked_at >= published_at
            AND octet_length(revoked_by) BETWEEN 1 AND 256
            AND revoked_by !~ '[[:cntrl:]]'
        )
    ),
    CONSTRAINT profile_versions_state_check CHECK (
        (NOT active OR (activated_at IS NOT NULL AND revoked_at IS NULL))
        AND (revoked_at IS NULL OR NOT active)
    )
);

CREATE UNIQUE INDEX profile_one_active_version
    ON profile_versions (tenant_id, profile_id)
    WHERE active;
