LOCK TABLE coordinator_work_journal IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM coordinator_work_journal WHERE delivered = FALSE) THEN
        RAISE EXCEPTION 'coordinator journal contains undelivered replay work' USING ERRCODE = '55000';
    END IF;
END;
$$;

ALTER TABLE coordinator_work_journal
    DROP CONSTRAINT coordinator_work_journal_claim_check,
    DROP CONSTRAINT coordinator_work_journal_claim_affinity_generation_check,
    DROP COLUMN claim_affinity_generation,
    ADD CONSTRAINT coordinator_work_journal_claim_check CHECK (
        (claim_owner IS NULL AND claim_until IS NULL) OR
        (claim_owner IS NOT NULL AND claim_until IS NOT NULL)
    );

DROP TABLE coordinator_delivery_shards;
