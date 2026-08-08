ALTER TABLE conversations
    DROP CONSTRAINT conversations_deletion_tuple_check,
    DROP CONSTRAINT conversations_deletion_reason_size_check,
    DROP CONSTRAINT conversations_deletion_actor_size_check,
    DROP COLUMN deletion_requested_at,
    DROP COLUMN deletion_reason_code,
    DROP COLUMN deletion_actor;
