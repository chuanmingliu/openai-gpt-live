LOCK TABLE handoffs IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM handoffs
        WHERE profile_id <> 'builtin'
           OR profile_version <> 1
           OR profile_snapshot_hash <> '0000000000000000000000000000000000000000000000000000000000000000'
           OR profile_engine_version <> 'jinja-go-v1/legacy'
           OR worker_recipe_id <> 'builtin'
           OR worker_recipe_version <> 1
           OR worker_recipe_hash <> '0000000000000000000000000000000000000000000000000000000000000000'
           OR adapter_registry_version <> 'builtin-v1'
           OR profile_knowledge::text <> '[]'
           OR profile_evidence::text <> '{}'
    ) THEN
        RAISE EXCEPTION 'handoff profile snapshot downgrade would lose non-default data' USING ERRCODE = '55000';
    END IF;
END;
$$;

ALTER TABLE handoffs
    DROP CONSTRAINT handoffs_profile_snapshot_check,
    DROP COLUMN profile_evidence,
    DROP COLUMN profile_knowledge,
    DROP COLUMN adapter_registry_version,
    DROP COLUMN worker_recipe_hash,
    DROP COLUMN worker_recipe_version,
    DROP COLUMN worker_recipe_id,
    DROP COLUMN profile_engine_version,
    DROP COLUMN profile_snapshot_hash,
    DROP COLUMN profile_version,
    DROP COLUMN profile_id;
