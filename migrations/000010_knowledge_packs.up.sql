CREATE TABLE knowledge_versions (
    tenant_id TEXT NOT NULL,
    pack_id TEXT NOT NULL,
    version BIGINT NOT NULL,
    content_hash TEXT NOT NULL,
    title TEXT NOT NULL,
    media_type TEXT NOT NULL,
    excerpt_count INTEGER NOT NULL,
    content_bytes BIGINT NOT NULL,
    published_at TIMESTAMPTZ NOT NULL,
    published_by TEXT NOT NULL,
    tombstoned_at TIMESTAMPTZ,
    tombstoned_by TEXT,
    CONSTRAINT knowledge_versions_pkey PRIMARY KEY (tenant_id, pack_id, version),
    CONSTRAINT knowledge_versions_content_uniq UNIQUE (tenant_id, pack_id, content_hash),
    CONSTRAINT knowledge_versions_identity_check CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 256
        AND tenant_id !~ '[[:cntrl:][:space:]]'
        AND octet_length(pack_id) BETWEEN 1 AND 256
        AND pack_id !~ '[[:cntrl:][:space:]]'
    ),
    CONSTRAINT knowledge_versions_version_check CHECK (version > 0),
    CONSTRAINT knowledge_versions_hash_check CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT knowledge_versions_title_check CHECK (
        octet_length(title) BETWEEN 1 AND 1024
        AND title !~ '[[:cntrl:]]'
        AND octet_length(media_type) BETWEEN 0 AND 256
        AND media_type !~ '[[:cntrl:][:space:]]'
    ),
    -- Aggregate bounds are enforced here as well as in Go so a direct SQL
    -- writer cannot install a version the domain would reject.
    CONSTRAINT knowledge_versions_aggregate_check CHECK (
        excerpt_count BETWEEN 1 AND 2048
        AND content_bytes BETWEEN 1 AND 8388608
    ),
    CONSTRAINT knowledge_versions_publication_check CHECK (
        octet_length(published_by) BETWEEN 1 AND 256
        AND published_by !~ '[[:cntrl:]]'
        AND isfinite(published_at)
    ),
    CONSTRAINT knowledge_versions_tombstone_check CHECK (
        (tombstoned_at IS NULL AND tombstoned_by IS NULL)
        OR (
            tombstoned_at IS NOT NULL AND tombstoned_by IS NOT NULL
            AND isfinite(tombstoned_at) AND tombstoned_at >= published_at
            AND octet_length(tombstoned_by) BETWEEN 1 AND 256
            AND tombstoned_by !~ '[[:cntrl:]]'
        )
    )
);

CREATE TABLE knowledge_excerpts (
    tenant_id TEXT NOT NULL,
    pack_id TEXT NOT NULL,
    version BIGINT NOT NULL,
    ordinal INTEGER NOT NULL,
    body TEXT NOT NULL,
    body_bytes INTEGER NOT NULL,
    excerpt_hash TEXT NOT NULL,
    tokens TEXT[] NOT NULL,
    source_title TEXT NOT NULL,
    section TEXT NOT NULL,
    page INTEGER NOT NULL,
    CONSTRAINT knowledge_excerpts_pkey PRIMARY KEY (tenant_id, pack_id, version, ordinal),
    CONSTRAINT knowledge_excerpts_version_fkey FOREIGN KEY (tenant_id, pack_id, version)
        REFERENCES knowledge_versions (tenant_id, pack_id, version) ON DELETE CASCADE,
    CONSTRAINT knowledge_excerpts_ordinal_check CHECK (ordinal BETWEEN 0 AND 2047),
    CONSTRAINT knowledge_excerpts_body_check CHECK (
        body_bytes BETWEEN 1 AND 4096
        AND octet_length(body) = body_bytes
    ),
    CONSTRAINT knowledge_excerpts_hash_check CHECK (excerpt_hash ~ '^[0-9a-f]{64}$'),
    -- array_length and array_ndims return NULL for an empty array, and a NULL
    -- CHECK is satisfied, so both are coalesced. Zero tokens is legal: a
    -- paragraph of only punctuation has no lexical index but is still content.
    CONSTRAINT knowledge_excerpts_tokens_check CHECK (
        COALESCE(array_ndims(tokens), 1) = 1
        AND COALESCE(array_length(tokens, 1), 0) <= 1024
        AND array_position(tokens, NULL) IS NULL
    ),
    CONSTRAINT knowledge_excerpts_citation_check CHECK (
        octet_length(source_title) BETWEEN 1 AND 1024
        AND source_title !~ '[[:cntrl:]]'
        AND octet_length(section) BETWEEN 0 AND 1024
        AND section !~ '[[:cntrl:]]'
        AND page BETWEEN 0 AND 1048576
    )
);

CREATE INDEX knowledge_excerpts_tokens_idx ON knowledge_excerpts USING GIN (tokens);
