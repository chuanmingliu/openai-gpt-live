ALTER TABLE conversations
    ADD COLUMN deletion_actor TEXT,
    ADD COLUMN deletion_reason_code TEXT,
    ADD COLUMN deletion_requested_at TIMESTAMPTZ,
    ADD CONSTRAINT conversations_deletion_actor_size_check CHECK (
        deletion_actor IS NULL OR octet_length(deletion_actor) BETWEEN 1 AND 256
    ),
    ADD CONSTRAINT conversations_deletion_reason_size_check CHECK (
        deletion_reason_code IS NULL OR octet_length(deletion_reason_code) BETWEEN 1 AND 256
    ),
    ADD CONSTRAINT conversations_deletion_tuple_check CHECK (
        (deletion_actor IS NULL AND deletion_reason_code IS NULL AND deletion_requested_at IS NULL)
        OR
        (deletion_actor IS NOT NULL AND deletion_reason_code IS NOT NULL
         AND deletion_requested_at IS NOT NULL AND deletion_requested_at >= created_at)
    );
