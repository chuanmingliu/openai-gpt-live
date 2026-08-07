LOCK TABLE tool_invocations IN ACCESS EXCLUSIVE MODE;

ALTER TABLE tool_invocations
    DROP CONSTRAINT tool_invocations_stable_uniq;

ALTER TABLE tool_invocations
    ADD CONSTRAINT tool_invocations_stable_uniq UNIQUE (
        tenant_id,
        conversation_id,
        handoff_id,
        objective_revision,
        tool_ordinal,
        tool_name,
        canonical_arguments_hash
    );
