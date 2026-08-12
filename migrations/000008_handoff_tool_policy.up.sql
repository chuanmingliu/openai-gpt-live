ALTER TABLE handoffs
    ADD COLUMN tool_policy JSONB NOT NULL
    DEFAULT '{"web_search":{"enabled":false,"safe_search":"moderate","region":"wt-wt","max_results":5,"timeout_ms":8000}}'::JSONB;
