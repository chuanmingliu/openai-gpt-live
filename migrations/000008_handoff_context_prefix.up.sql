ALTER TABLE handoffs
    ADD COLUMN context_prefix JSON,
    ADD CONSTRAINT handoffs_context_prefix_check CHECK (
        context_prefix IS NULL OR (
            json_typeof(context_prefix) = 'array'
            AND octet_length(context_prefix::text) <= 65536
            AND canonical_json_depth_valid(context_prefix)
        )
    );
