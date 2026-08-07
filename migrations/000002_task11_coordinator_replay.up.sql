CREATE TABLE coordinator_work_journal (
    work_key TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    delivery_affinity TEXT NOT NULL,
    incarnation BIGINT NOT NULL,
    local_sequence BIGINT NOT NULL,
    payload JSON NOT NULL,
    payload_hash TEXT NOT NULL,
    storage_sequence BIGINT,
    delivered BOOLEAN NOT NULL DEFAULT FALSE,
    claim_owner TEXT,
    claim_until TIMESTAMPTZ,
    claim_generation BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    delivered_at TIMESTAMPTZ,
    CONSTRAINT coordinator_work_journal_kind_check CHECK (kind IN ('transcript', 'handoff')),
    CONSTRAINT coordinator_work_journal_scope_fkey FOREIGN KEY (tenant_id, conversation_id)
        REFERENCES conversations (tenant_id, id) ON DELETE CASCADE,
    CONSTRAINT coordinator_work_journal_work_key_size CHECK (octet_length(work_key) BETWEEN 1 AND 256),
    CONSTRAINT coordinator_work_journal_identity_size CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 256 AND
        octet_length(user_id) BETWEEN 1 AND 256 AND
        octet_length(conversation_id) BETWEEN 1 AND 256 AND
        octet_length(session_id) BETWEEN 1 AND 256 AND
        octet_length(delivery_affinity) BETWEEN 1 AND 256
    ),
    CONSTRAINT coordinator_work_journal_incarnation_check CHECK (incarnation > 0),
    CONSTRAINT coordinator_work_journal_local_sequence_check CHECK (local_sequence >= 0),
    CONSTRAINT coordinator_work_journal_payload_size CHECK (octet_length(payload::text) BETWEEN 2 AND 262144),
    CONSTRAINT coordinator_work_journal_payload_depth_check CHECK (canonical_json_depth_valid(payload)),
    CONSTRAINT coordinator_work_journal_payload_hash_size CHECK (
        octet_length(payload_hash) = 64 AND payload_hash ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT coordinator_work_journal_storage_sequence_check CHECK (
        (kind = 'transcript' AND storage_sequence IS NOT NULL AND storage_sequence > 0) OR
        (kind = 'handoff' AND storage_sequence IS NULL)
    ),
    CONSTRAINT coordinator_work_journal_delivery_check CHECK (
        (delivered = FALSE AND delivered_at IS NULL) OR
        (delivered = TRUE AND delivered_at IS NOT NULL)
    ),
    CONSTRAINT coordinator_work_journal_claim_check CHECK (
        (claim_owner IS NULL AND claim_until IS NULL) OR
        (claim_owner IS NOT NULL AND claim_until IS NOT NULL)
    ),
    CONSTRAINT coordinator_work_journal_claim_owner_size CHECK (
        claim_owner IS NULL OR octet_length(claim_owner) BETWEEN 1 AND 256
    ),
    CONSTRAINT coordinator_work_journal_claim_generation_check CHECK (
        claim_generation >= 0
    )
);

CREATE UNIQUE INDEX coordinator_work_journal_storage_sequence_uniq
    ON coordinator_work_journal (tenant_id, conversation_id, storage_sequence)
    WHERE storage_sequence IS NOT NULL;

CREATE INDEX coordinator_work_journal_pending_idx
    ON coordinator_work_journal (kind, delivery_affinity, delivered, created_at, work_key)
    WHERE delivered = FALSE;
