CREATE TABLE coordinator_delivery_shards (
    delivery_affinity TEXT PRIMARY KEY,
    lease_owner TEXT NOT NULL,
    lease_until TIMESTAMPTZ NOT NULL,
    lease_generation BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT coordinator_delivery_shards_affinity_size_check CHECK (
        octet_length(delivery_affinity) BETWEEN 1 AND 256
    ),
    CONSTRAINT coordinator_delivery_shards_owner_size_check CHECK (
        octet_length(lease_owner) BETWEEN 1 AND 256
    ),
    CONSTRAINT coordinator_delivery_shards_generation_check CHECK (
        lease_generation > 0
    ),
    CONSTRAINT coordinator_delivery_shards_timestamps_check CHECK (
        updated_at >= created_at AND lease_until > updated_at
    )
);

ALTER TABLE coordinator_work_journal
    DROP CONSTRAINT coordinator_work_journal_claim_check,
    ADD COLUMN claim_affinity_generation BIGINT;

-- Claims created by a pre-v7 gateway have no shard generation and therefore
-- cannot be trusted after the new fencing authority is installed. Preserve
-- their work and generation history while making them immediately replayable.
UPDATE coordinator_work_journal
SET claim_owner=NULL, claim_until=NULL
WHERE claim_owner IS NOT NULL;

ALTER TABLE coordinator_work_journal
    ADD CONSTRAINT coordinator_work_journal_claim_check CHECK (
        (claim_owner IS NULL AND claim_until IS NULL AND claim_affinity_generation IS NULL) OR
        (claim_owner IS NOT NULL AND claim_until IS NOT NULL AND claim_affinity_generation IS NOT NULL)
    ),
    ADD CONSTRAINT coordinator_work_journal_claim_affinity_generation_check CHECK (
        claim_affinity_generation IS NULL OR claim_affinity_generation > 0
    );
