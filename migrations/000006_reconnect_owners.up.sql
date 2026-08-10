CREATE TABLE session_reconnect_owners (
    session_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    incarnation NUMERIC(20,0) NOT NULL,
    event_cursor BIGINT NOT NULL,
    provider_model TEXT NOT NULL,
    response_epoch NUMERIC(20,0) NOT NULL,
    credential_id TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    owner_state TEXT NOT NULL,
    turn_boundary BOOLEAN NOT NULL DEFAULT FALSE,
    expires_at TIMESTAMPTZ NOT NULL,
    subject_expires_at TIMESTAMPTZ,
    previous_credential_id TEXT,
    previous_incarnation NUMERIC(20,0),
    previous_event_cursor BIGINT,
    previous_expires_at TIMESTAMPTZ,
    previous_accept_until TIMESTAMPTZ,
    owner_lease_expires_at TIMESTAMPTZ NOT NULL,
    ephemeral BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT session_reconnect_owners_pkey PRIMARY KEY (session_id),
    CONSTRAINT session_reconnect_owners_identifiers_check CHECK (
        octet_length(session_id) BETWEEN 1 AND 256 AND
        octet_length(tenant_id) BETWEEN 1 AND 256 AND
        octet_length(user_id) BETWEEN 1 AND 256 AND
        octet_length(conversation_id) BETWEEN 1 AND 256 AND
        octet_length(provider_model) BETWEEN 1 AND 256 AND
        octet_length(credential_id) BETWEEN 1 AND 256 AND
        octet_length(owner_id) BETWEEN 1 AND 256 AND
        (previous_credential_id IS NULL OR octet_length(previous_credential_id) BETWEEN 1 AND 256)
    ),
    CONSTRAINT session_reconnect_owners_counters_check CHECK (
        incarnation BETWEEN 1 AND 18446744073709551615 AND
        response_epoch BETWEEN 0 AND 18446744073709551615 AND event_cursor >= 0 AND
        (previous_incarnation IS NULL OR previous_incarnation BETWEEN 1 AND 18446744073709551615) AND
        (previous_event_cursor IS NULL OR previous_event_cursor >= 0)
    ),
    CONSTRAINT session_reconnect_owners_state_check CHECK (
        owner_state IN ('establishing','active','tombstoned','claiming')
    ),
    CONSTRAINT session_reconnect_owners_times_check CHECK (
        expires_at > created_at AND owner_lease_expires_at > created_at AND
        (subject_expires_at IS NULL OR subject_expires_at > created_at)
    ),
    CONSTRAINT session_reconnect_owners_previous_check CHECK (
        (previous_credential_id IS NULL AND previous_incarnation IS NULL AND previous_event_cursor IS NULL AND previous_expires_at IS NULL AND previous_accept_until IS NULL) OR
        (previous_credential_id IS NOT NULL AND previous_incarnation IS NOT NULL AND previous_event_cursor IS NOT NULL AND previous_expires_at IS NOT NULL AND
         previous_accept_until IS NOT NULL AND previous_expires_at > created_at AND
         previous_accept_until > created_at AND previous_accept_until <= previous_expires_at)
    )
);

CREATE INDEX session_reconnect_owners_expiry_idx ON session_reconnect_owners (expires_at);
CREATE INDEX session_reconnect_owners_cleanup_idx ON session_reconnect_owners (owner_lease_expires_at, expires_at);
