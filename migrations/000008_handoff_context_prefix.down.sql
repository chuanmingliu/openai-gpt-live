ALTER TABLE handoffs
    DROP CONSTRAINT handoffs_context_prefix_check,
    DROP COLUMN context_prefix;
