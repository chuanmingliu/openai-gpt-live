LOCK TABLE tool_invocations IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM tool_invocations
        GROUP BY
            tenant_id,
            handoff_id,
            objective_revision,
            tool_ordinal,
            tool_name,
            canonical_arguments_hash
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'tool invocation rows conflict under legacy scope' USING ERRCODE = '55000';
    END IF;
END;
$$;

ALTER TABLE tool_invocations
    DROP CONSTRAINT tool_invocations_stable_uniq;

ALTER TABLE tool_invocations
    ADD CONSTRAINT tool_invocations_stable_uniq UNIQUE (
        tenant_id,
        handoff_id,
        objective_revision,
        tool_ordinal,
        tool_name,
        canonical_arguments_hash
    );
