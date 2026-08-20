ALTER TABLE handoffs
    ADD COLUMN profile_id TEXT NOT NULL DEFAULT 'builtin',
    ADD COLUMN profile_version BIGINT NOT NULL DEFAULT 1,
    ADD COLUMN profile_snapshot_hash TEXT NOT NULL DEFAULT '0000000000000000000000000000000000000000000000000000000000000000',
    ADD COLUMN profile_engine_version TEXT NOT NULL DEFAULT 'jinja-go-v1/legacy',
    ADD COLUMN worker_recipe_id TEXT NOT NULL DEFAULT 'builtin',
    ADD COLUMN worker_recipe_version BIGINT NOT NULL DEFAULT 1,
    ADD COLUMN worker_recipe_hash TEXT NOT NULL DEFAULT '0000000000000000000000000000000000000000000000000000000000000000',
    ADD COLUMN adapter_registry_version TEXT NOT NULL DEFAULT 'builtin-v1',
    ADD COLUMN profile_knowledge JSON NOT NULL DEFAULT '[]'::JSON,
    ADD COLUMN profile_evidence JSON NOT NULL DEFAULT '{}'::JSON,
    ADD CONSTRAINT handoffs_profile_snapshot_check CHECK (
        octet_length(profile_id) BETWEEN 1 AND 256
        AND profile_id !~ '[[:cntrl:][:space:]]'
        AND profile_version > 0
        AND profile_snapshot_hash ~ '^[0-9a-f]{64}$'
        AND octet_length(profile_engine_version) BETWEEN 3 AND 256
        AND profile_engine_version !~ '[[:cntrl:][:space:]]'
        AND octet_length(worker_recipe_id) BETWEEN 1 AND 256
        AND worker_recipe_id !~ '[[:cntrl:][:space:]]'
        AND worker_recipe_version > 0
        AND worker_recipe_hash ~ '^[0-9a-f]{64}$'
        AND octet_length(adapter_registry_version) BETWEEN 1 AND 256
        AND adapter_registry_version !~ '[[:cntrl:][:space:]]'
        AND json_typeof(profile_knowledge) = 'array'
        AND json_array_length(profile_knowledge) <= 32
        AND octet_length(profile_knowledge::text) <= 32768
        AND canonical_json_depth_valid(profile_knowledge)
        AND json_typeof(profile_evidence) = 'object'
        AND octet_length(profile_evidence::text) <= 262144
        AND canonical_json_depth_valid(profile_evidence)
    );
