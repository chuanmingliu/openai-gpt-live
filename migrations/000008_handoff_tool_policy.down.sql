LOCK TABLE handoffs IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM handoffs
        WHERE tool_policy <> '{"web_search":{"enabled":false,"safe_search":"moderate","region":"wt-wt","max_results":5,"timeout_ms":8000}}'::JSONB
    ) THEN
        RAISE EXCEPTION 'handoff tool policy downgrade would lose non-default data' USING ERRCODE = '55000';
    END IF;
END;
$$;

ALTER TABLE handoffs DROP COLUMN tool_policy;
