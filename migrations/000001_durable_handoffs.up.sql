-- Task 8 supplies compact canonical JSON. PostgreSQL's textual JSON type
-- preserves its complete lexeme domain, including escaped NUL and numbers
-- beyond numeric's range. This helper validates a canonical JSON string
-- without decoding it into PostgreSQL TEXT, which cannot contain U+0000.
CREATE FUNCTION canonical_tool_name_bytes(value TEXT)
RETURNS BYTEA
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
DECLARE
	position INTEGER := 2;
	closing_quote INTEGER := char_length(value);
	character TEXT;
	escape TEXT;
	codepoint INTEGER;
	low_surrogate INTEGER;
	decoded BYTEA := ''::bytea;
	nonblank BOOLEAN := FALSE;
BEGIN
	IF closing_quote < 2 OR substr(value, 1, 1) <> '"' OR substr(value, closing_quote, 1) <> '"' THEN
		RETURN NULL;
	END IF;
	WHILE position < closing_quote LOOP
		character := substr(value, position, 1);
		IF character <> chr(92) THEN
			codepoint := ascii(character);
			decoded := decoded || convert_to(character, 'UTF8');
			position := position + 1;
		ELSE
			escape := substr(value, position + 1, 1);
			CASE escape
				WHEN '"' THEN codepoint := 34;
				WHEN '/' THEN codepoint := 47;
				WHEN 'b' THEN codepoint := 8;
				WHEN 'f' THEN codepoint := 12;
				WHEN 'n' THEN codepoint := 10;
				WHEN 'r' THEN codepoint := 13;
				WHEN 't' THEN codepoint := 9;
				WHEN 'u' THEN
					codepoint := get_byte(decode(substr(value, position + 2, 4), 'hex'), 0) * 256
						+ get_byte(decode(substr(value, position + 2, 4), 'hex'), 1);
					IF codepoint BETWEEN 55296 AND 56319 THEN
						IF substr(value, position + 6, 2) <> chr(92) || 'u' THEN
							RETURN NULL;
						END IF;
						low_surrogate := get_byte(decode(substr(value, position + 8, 4), 'hex'), 0) * 256
							+ get_byte(decode(substr(value, position + 8, 4), 'hex'), 1);
						IF low_surrogate NOT BETWEEN 56320 AND 57343 THEN
							RETURN NULL;
						END IF;
						codepoint := 65536 + (codepoint - 55296) * 1024 + (low_surrogate - 56320);
						position := position + 6;
					ELSIF codepoint BETWEEN 56320 AND 57343 THEN
						RETURN NULL;
					END IF;
				ELSE
					IF escape = chr(92) THEN
						codepoint := 92;
					ELSE
						RETURN NULL;
					END IF;
			END CASE;
			decoded := decoded || decode(CASE
				WHEN codepoint <= 127 THEN
					lpad(to_hex(codepoint), 2, '0')
				WHEN codepoint <= 2047 THEN
					lpad(to_hex(192 | (codepoint >> 6)), 2, '0') ||
					lpad(to_hex(128 | (codepoint & 63)), 2, '0')
				WHEN codepoint <= 65535 THEN
					lpad(to_hex(224 | (codepoint >> 12)), 2, '0') ||
					lpad(to_hex(128 | ((codepoint >> 6) & 63)), 2, '0') ||
					lpad(to_hex(128 | (codepoint & 63)), 2, '0')
				ELSE
					lpad(to_hex(240 | (codepoint >> 18)), 2, '0') ||
					lpad(to_hex(128 | ((codepoint >> 12) & 63)), 2, '0') ||
					lpad(to_hex(128 | ((codepoint >> 6) & 63)), 2, '0') ||
					lpad(to_hex(128 | (codepoint & 63)), 2, '0')
			END, 'hex');
			IF escape = 'u' THEN
				position := position + 6;
			ELSE
				position := position + 2;
			END IF;
		END IF;
		IF codepoint NOT IN (
			9, 10, 11, 12, 13, 32, 133, 160, 5760,
			8192, 8193, 8194, 8195, 8196, 8197, 8198, 8199, 8200, 8201, 8202,
			8232, 8233, 8239, 8287, 12288
		) THEN
			nonblank := TRUE;
		END IF;
		IF octet_length(decoded) > 256 THEN
			RETURN NULL;
		END IF;
	END LOOP;
	IF octet_length(decoded) BETWEEN 1 AND 256 AND nonblank THEN
		RETURN decoded;
	END IF;
	RETURN NULL;
END;
$$;

-- Scan validated JSON as UTF-8 bytes so depth enforcement stays linear and
-- never decodes string values (notably escaped U+0000) into PostgreSQL TEXT.
CREATE FUNCTION canonical_json_depth_valid(value JSON)
RETURNS BOOLEAN
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
DECLARE
	raw_value BYTEA := convert_to(value::text, 'UTF8');
	position INTEGER;
	character INTEGER;
	depth INTEGER := 0;
	in_string BOOLEAN := FALSE;
	escaped BOOLEAN := FALSE;
BEGIN
	FOR position IN 0..octet_length(raw_value) - 1 LOOP
		character := get_byte(raw_value, position);
		IF in_string THEN
			IF escaped THEN
				escaped := FALSE;
			ELSIF character = 92 THEN
				escaped := TRUE;
			ELSIF character = 34 THEN
				in_string := FALSE;
			END IF;
			CONTINUE;
		END IF;
		IF character = 34 THEN
			IF depth >= 64 THEN
				RETURN FALSE;
			END IF;
			in_string := TRUE;
		ELSIF character IN (91, 123) THEN
			depth := depth + 1;
			IF depth > 64 THEN
				RETURN FALSE;
			END IF;
		ELSIF character IN (93, 125) THEN
			depth := depth - 1;
		ELSIF character NOT IN (9, 10, 13, 32, 44, 58) AND depth >= 64 THEN
			RETURN FALSE;
		END IF;
	END LOOP;
	RETURN TRUE;
END;
$$;

-- Application validation remains authoritative for canonicalization. At the
-- database boundary, decoded BYTEA provides exact equality and byte/blank
-- checks without materializing U+0000 as PostgreSQL TEXT.
CREATE FUNCTION tool_allowlist_valid(value JSON)
RETURNS BOOLEAN
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
DECLARE
	item JSON;
	raw_item TEXT;
	decoded_name BYTEA;
	names BYTEA[] := ARRAY[]::BYTEA[];
BEGIN
	IF json_typeof(value) <> 'array' OR json_array_length(value) > 64
		OR octet_length(value::text) > 98497 OR NOT canonical_json_depth_valid(value) THEN
		RETURN FALSE;
	END IF;
	FOR item IN SELECT array_item FROM json_array_elements(value) AS elements(array_item) LOOP
		IF json_typeof(item) <> 'string' THEN
			RETURN FALSE;
		END IF;
		raw_item := item::text;
		decoded_name := canonical_tool_name_bytes(raw_item);
		IF decoded_name IS NULL OR decoded_name = ANY(names) THEN
			RETURN FALSE;
		END IF;
		names := array_append(names, decoded_name);
	END LOOP;
	RETURN TRUE;
END;
$$;

CREATE FUNCTION outbox_payload_keys_valid(value JSON)
RETURNS BOOLEAN
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
DECLARE
	raw_value TEXT := value::text;
	position INTEGER := 2;
	value_length INTEGER := char_length(value::text);
	key_start INTEGER;
	raw_key TEXT;
	raw_scalar TEXT;
	value_start INTEGER;
	character TEXT;
	escaped BOOLEAN;
BEGIN
	IF json_typeof(value) <> 'object' THEN
		RETURN FALSE;
	END IF;
	LOOP
		WHILE position <= value_length AND substr(raw_value, position, 1) IN (' ', chr(9), chr(10), chr(13)) LOOP
			position := position + 1;
		END LOOP;
		IF position > value_length OR substr(raw_value, position, 1) = '}' THEN
			RETURN TRUE;
		END IF;
		IF substr(raw_value, position, 1) <> '"' THEN
			RETURN FALSE;
		END IF;
		key_start := position;
		position := position + 1;
		escaped := FALSE;
		LOOP
			IF position > value_length THEN
				RETURN FALSE;
			END IF;
			character := substr(raw_value, position, 1);
			IF escaped THEN
				escaped := FALSE;
			ELSIF character = chr(92) THEN
				escaped := TRUE;
			ELSIF character = '"' THEN
				EXIT;
			END IF;
			position := position + 1;
		END LOOP;
		raw_key := substr(raw_value, key_start, position - key_start + 1);
		IF raw_key <> ALL(ARRAY['"tenant_id"', '"conversation_id"', '"handoff_id"', '"event_seq"', '"highest_seq"']) THEN
			RETURN FALSE;
		END IF;
		position := position + 1;
		WHILE position <= value_length AND substr(raw_value, position, 1) IN (' ', chr(9), chr(10), chr(13)) LOOP
			position := position + 1;
		END LOOP;
		IF substr(raw_value, position, 1) <> ':' THEN
			RETURN FALSE;
		END IF;
		position := position + 1;
		WHILE position <= value_length AND substr(raw_value, position, 1) IN (' ', chr(9), chr(10), chr(13)) LOOP
			position := position + 1;
		END LOOP;
		IF position > value_length OR substr(raw_value, position, 1) IN ('[', '{') THEN
			RETURN FALSE;
		END IF;
		value_start := position;
		IF substr(raw_value, position, 1) = '"' THEN
			position := position + 1;
			escaped := FALSE;
			LOOP
				IF position > value_length THEN
					RETURN FALSE;
				END IF;
				character := substr(raw_value, position, 1);
				IF escaped THEN
					escaped := FALSE;
				ELSIF character = chr(92) THEN
					escaped := TRUE;
				ELSIF character = '"' THEN
					EXIT;
				END IF;
				position := position + 1;
			END LOOP;
			raw_scalar := substr(raw_value, value_start, position - value_start + 1);
			IF raw_key IN ('"event_seq"', '"highest_seq"')
				OR canonical_tool_name_bytes(raw_scalar) IS NULL THEN
				RETURN FALSE;
			END IF;
			position := position + 1;
		ELSE
			WHILE position <= value_length AND substr(raw_value, position, 1) NOT IN (',', '}') LOOP
				position := position + 1;
			END LOOP;
			raw_scalar := btrim(substr(raw_value, value_start, position - value_start));
			IF raw_key NOT IN ('"event_seq"', '"highest_seq"')
				OR raw_scalar !~ '^(0|[1-9][0-9]*)$'
				OR char_length(raw_scalar) > 19
				OR (char_length(raw_scalar) = 19 AND raw_scalar > '9223372036854775807') THEN
				RETURN FALSE;
			END IF;
		END IF;
		WHILE position <= value_length AND substr(raw_value, position, 1) IN (' ', chr(9), chr(10), chr(13)) LOOP
			position := position + 1;
		END LOOP;
		IF substr(raw_value, position, 1) = '}' THEN
			RETURN TRUE;
		ELSIF substr(raw_value, position, 1) <> ',' THEN
			RETURN FALSE;
		END IF;
		position := position + 1;
	END LOOP;
END;
$$;

CREATE TABLE conversations (
    tenant_id TEXT NOT NULL,
    id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    retention_until TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT conversations_pkey PRIMARY KEY (tenant_id, id),
    CONSTRAINT conversations_tenant_id_size_check CHECK (octet_length(tenant_id) BETWEEN 1 AND 256),
    CONSTRAINT conversations_id_size_check CHECK (octet_length(id) BETWEEN 1 AND 256),
    CONSTRAINT conversations_user_id_size_check CHECK (octet_length(user_id) BETWEEN 1 AND 256),
    CONSTRAINT conversations_status_check CHECK (status IN ('active', 'closed', 'deleted')),
    CONSTRAINT conversations_retention_check CHECK (retention_until >= created_at),
    CONSTRAINT conversations_timestamps_check CHECK (updated_at >= created_at)
);

CREATE TABLE conversation_items (
    tenant_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    turn_revision BIGINT NOT NULL,
    sequence BIGINT NOT NULL,
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT conversation_items_pkey PRIMARY KEY (tenant_id, conversation_id, session_id, sequence),
    CONSTRAINT conversation_items_conversation_fkey FOREIGN KEY (tenant_id, conversation_id)
        REFERENCES conversations (tenant_id, id) ON DELETE CASCADE,
    CONSTRAINT conversation_items_sequence_uniq UNIQUE (tenant_id, conversation_id, sequence),
    CONSTRAINT conversation_items_turn_role_uniq UNIQUE (tenant_id, conversation_id, turn_id, turn_revision, role),
    CONSTRAINT conversation_items_tenant_id_size_check CHECK (octet_length(tenant_id) BETWEEN 1 AND 256),
    CONSTRAINT conversation_items_conversation_id_size_check CHECK (octet_length(conversation_id) BETWEEN 1 AND 256),
    CONSTRAINT conversation_items_session_id_size_check CHECK (octet_length(session_id) BETWEEN 1 AND 256),
    CONSTRAINT conversation_items_turn_id_size_check CHECK (octet_length(turn_id) BETWEEN 1 AND 256),
    CONSTRAINT conversation_items_turn_revision_check CHECK (turn_revision >= 0),
    CONSTRAINT conversation_items_sequence_check CHECK (sequence >= 0),
    CONSTRAINT conversation_items_role_check CHECK (role IN ('system', 'user', 'assistant', 'tool')),
    CONSTRAINT conversation_items_content_size_check CHECK (octet_length(content) BETWEEN 1 AND 65536),
    CONSTRAINT conversation_items_content_hash_size_check CHECK (octet_length(content_hash) BETWEEN 1 AND 512),
    CONSTRAINT conversation_items_timestamps_check CHECK (updated_at >= created_at)
);

CREATE TABLE handoffs (
    tenant_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    source_turn_id TEXT NOT NULL,
    source_turn_revision BIGINT NOT NULL,
    user_request TEXT NOT NULL,
    objective TEXT NOT NULL,
    context_ref TEXT NOT NULL,
    context_hash TEXT NOT NULL,
    deadline TIMESTAMPTZ NOT NULL,
    token_budget INTEGER NOT NULL,
    priority SMALLINT NOT NULL DEFAULT 0,
    tool_allowlist JSON NOT NULL DEFAULT '[]'::json,
    approval_policy JSON NOT NULL,
    model_version TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    tool_registry_version TEXT NOT NULL,
    config_version TEXT NOT NULL,
    presentation_hint TEXT NOT NULL DEFAULT '',
    idempotency_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    objective_revision BIGINT NOT NULL DEFAULT 0,
    input_revision BIGINT NOT NULL DEFAULT 0,
    consumed_input_revision BIGINT NOT NULL DEFAULT 0,
    input JSON,
    ready_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    attempt INTEGER NOT NULL DEFAULT 0,
    retry_count INTEGER NOT NULL DEFAULT 0,
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    lease_version BIGINT NOT NULL DEFAULT 0,
    cancel_requested_at TIMESTAMPTZ,
    cancel_actor TEXT,
    cancel_reason TEXT,
    terminal_mutation_id TEXT,
    result_summary TEXT,
    structured_result JSON,
    failure_code TEXT,
    failure_retryable BOOLEAN,
    failure_message TEXT,
    safety_decision JSON,
    usage JSON,
    terminal_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT handoffs_pkey PRIMARY KEY (tenant_id, conversation_id, id),
    CONSTRAINT handoffs_conversation_fkey FOREIGN KEY (tenant_id, conversation_id)
        REFERENCES conversations (tenant_id, id) ON DELETE CASCADE,
    CONSTRAINT handoffs_tenant_idempotency_uniq UNIQUE (tenant_id, idempotency_key),
    CONSTRAINT handoffs_tenant_id_size_check CHECK (octet_length(tenant_id) BETWEEN 1 AND 256),
    CONSTRAINT handoffs_conversation_id_size_check CHECK (octet_length(conversation_id) BETWEEN 1 AND 256),
    CONSTRAINT handoffs_id_size_check CHECK (octet_length(id) BETWEEN 1 AND 256),
    CONSTRAINT handoffs_user_id_size_check CHECK (octet_length(user_id) BETWEEN 1 AND 256),
    CONSTRAINT handoffs_session_id_size_check CHECK (octet_length(session_id) BETWEEN 1 AND 256),
    CONSTRAINT handoffs_source_turn_id_size_check CHECK (octet_length(source_turn_id) BETWEEN 1 AND 256),
    CONSTRAINT handoffs_source_turn_revision_check CHECK (source_turn_revision > 0),
    CONSTRAINT handoffs_user_request_size_check CHECK (octet_length(user_request) BETWEEN 1 AND 16384),
    CONSTRAINT handoffs_objective_size_check CHECK (octet_length(objective) BETWEEN 1 AND 8192),
    CONSTRAINT handoffs_context_ref_size_check CHECK (octet_length(context_ref) BETWEEN 1 AND 2048),
    CONSTRAINT handoffs_context_hash_size_check CHECK (octet_length(context_hash) BETWEEN 1 AND 512),
    CONSTRAINT handoffs_deadline_check CHECK (deadline > created_at),
    CONSTRAINT handoffs_token_budget_check CHECK (token_budget > 0),
    CONSTRAINT handoffs_priority_check CHECK (priority BETWEEN -1000 AND 1000),
    CONSTRAINT handoffs_tool_allowlist_check CHECK (
		tool_allowlist_valid(tool_allowlist)
    ),
    CONSTRAINT handoffs_approval_policy_check CHECK (
		octet_length(approval_policy::text) <= 65536 AND canonical_json_depth_valid(approval_policy)
    ),
    CONSTRAINT handoffs_model_version_size_check CHECK (octet_length(model_version) BETWEEN 1 AND 256),
    CONSTRAINT handoffs_prompt_version_size_check CHECK (octet_length(prompt_version) BETWEEN 1 AND 256),
    CONSTRAINT handoffs_tool_registry_version_size_check CHECK (octet_length(tool_registry_version) BETWEEN 1 AND 256),
    CONSTRAINT handoffs_config_version_size_check CHECK (octet_length(config_version) BETWEEN 1 AND 256),
    CONSTRAINT handoffs_presentation_hint_size_check CHECK (octet_length(presentation_hint) <= 2048),
    CONSTRAINT handoffs_idempotency_key_size_check CHECK (octet_length(idempotency_key) BETWEEN 1 AND 256),
    CONSTRAINT handoffs_request_hash_size_check CHECK (octet_length(request_hash) BETWEEN 1 AND 512),
    CONSTRAINT handoffs_state_check CHECK (state IN (
        'pending', 'leased', 'running', 'needs_input',
        'succeeded', 'failed', 'cancelled', 'expired'
    )),
    CONSTRAINT handoffs_revision_check CHECK (
        objective_revision >= 0 AND input_revision >= 0
        AND consumed_input_revision >= 0 AND consumed_input_revision <= input_revision
    ),
    CONSTRAINT handoffs_input_check CHECK (
        (input_revision = 0 AND input IS NULL)
		OR (input_revision > 0 AND input IS NOT NULL
			AND octet_length(input::text) <= 65536 AND canonical_json_depth_valid(input))
    ),
    CONSTRAINT handoffs_ready_at_check CHECK (ready_at >= created_at),
    CONSTRAINT handoffs_attempt_check CHECK (attempt >= 0 AND retry_count >= 0),
    CONSTRAINT handoffs_lease_tuple_check CHECK (
        lease_version >= 0
        AND ((lease_owner IS NULL AND lease_expires_at IS NULL)
          OR (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL))
    ),
    CONSTRAINT handoffs_lease_owner_size_check CHECK (lease_owner IS NULL OR octet_length(lease_owner) BETWEEN 1 AND 256),
    CONSTRAINT handoffs_cancel_tuple_check CHECK (
        (cancel_requested_at IS NULL AND cancel_actor IS NULL AND cancel_reason IS NULL)
        OR (cancel_requested_at IS NOT NULL AND cancel_actor IS NOT NULL)
    ),
    CONSTRAINT handoffs_cancel_actor_size_check CHECK (cancel_actor IS NULL OR octet_length(cancel_actor) BETWEEN 1 AND 256),
    CONSTRAINT handoffs_cancel_reason_size_check CHECK (cancel_reason IS NULL OR octet_length(cancel_reason) <= 2048),
    CONSTRAINT handoffs_terminal_mutation_id_size_check CHECK (
        terminal_mutation_id IS NULL OR octet_length(terminal_mutation_id) BETWEEN 1 AND 256
    ),
    CONSTRAINT handoffs_result_summary_size_check CHECK (result_summary IS NULL OR octet_length(result_summary) BETWEEN 1 AND 16384),
    CONSTRAINT handoffs_structured_result_check CHECK (
		structured_result IS NULL OR (
			octet_length(structured_result::text) <= 65536 AND canonical_json_depth_valid(structured_result)
		)
    ),
    CONSTRAINT handoffs_failure_code_size_check CHECK (failure_code IS NULL OR octet_length(failure_code) BETWEEN 1 AND 256),
    CONSTRAINT handoffs_failure_message_size_check CHECK (failure_message IS NULL OR octet_length(failure_message) BETWEEN 1 AND 16384),
    CONSTRAINT handoffs_safety_decision_check CHECK (
		safety_decision IS NULL OR (
			json_typeof(safety_decision) = 'object' AND octet_length(safety_decision::text) <= 65536
			AND canonical_json_depth_valid(safety_decision)
		)
    ),
    CONSTRAINT handoffs_usage_check CHECK (
		usage IS NULL OR (octet_length(usage::text) <= 65536 AND canonical_json_depth_valid(usage))
    ),
    CONSTRAINT handoffs_terminal_fields_check CHECK (
        (state NOT IN ('succeeded', 'failed', 'cancelled', 'expired')
          AND terminal_at IS NULL AND terminal_mutation_id IS NULL
          AND result_summary IS NULL AND structured_result IS NULL AND usage IS NULL
          AND failure_code IS NULL AND failure_retryable IS NULL AND failure_message IS NULL)
        OR
        (state = 'succeeded' AND terminal_at IS NOT NULL AND terminal_mutation_id IS NOT NULL
          AND result_summary IS NOT NULL AND structured_result IS NOT NULL AND usage IS NOT NULL
          AND failure_code IS NULL AND failure_retryable IS NULL AND failure_message IS NULL)
        OR
        (state = 'failed' AND terminal_at IS NOT NULL AND terminal_mutation_id IS NOT NULL
          AND result_summary IS NULL AND structured_result IS NULL AND usage IS NULL
          AND failure_code IS NOT NULL AND failure_retryable IS NOT NULL AND failure_message IS NOT NULL)
        OR
        (state IN ('cancelled', 'expired') AND terminal_at IS NOT NULL AND terminal_mutation_id IS NOT NULL
          AND result_summary IS NULL AND structured_result IS NULL AND usage IS NULL
          AND failure_code IS NULL AND failure_retryable IS NULL AND failure_message IS NULL)
    ),
    CONSTRAINT handoffs_timestamps_check CHECK (
        updated_at >= created_at AND (terminal_at IS NULL OR terminal_at >= created_at)
    )
);

CREATE INDEX handoffs_runnable_idx
    ON handoffs (priority DESC, ready_at, created_at)
    WHERE state IN ('pending', 'running') AND lease_owner IS NULL;

CREATE INDEX handoffs_expired_lease_idx
    ON handoffs (lease_expires_at)
    WHERE lease_owner IS NOT NULL;

CREATE INDEX handoffs_supplied_input_idx
    ON handoffs (updated_at)
    WHERE state = 'needs_input' AND input_revision > consumed_input_revision;

CREATE TABLE handoff_events (
    seq BIGSERIAL NOT NULL,
    event_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    handoff_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    state TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    payload JSON NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT handoff_events_pkey PRIMARY KEY (seq),
    CONSTRAINT handoff_events_event_id_uniq UNIQUE (event_id),
    CONSTRAINT handoff_events_handoff_fkey FOREIGN KEY (tenant_id, conversation_id, handoff_id)
        REFERENCES handoffs (tenant_id, conversation_id, id) ON DELETE CASCADE,
    CONSTRAINT handoff_events_event_id_size_check CHECK (octet_length(event_id) BETWEEN 1 AND 256),
    CONSTRAINT handoff_events_kind_size_check CHECK (octet_length(kind) BETWEEN 1 AND 256),
    CONSTRAINT handoff_events_state_check CHECK (state IN (
        'pending', 'leased', 'running', 'needs_input',
        'succeeded', 'failed', 'cancelled', 'expired'
    )),
    CONSTRAINT handoff_events_attempt_check CHECK (attempt >= 0),
    CONSTRAINT handoff_events_payload_check CHECK (
		json_typeof(payload) = 'object' AND octet_length(payload::text) <= 65536
		AND canonical_json_depth_valid(payload)
    )
);

CREATE INDEX handoff_events_cursor_idx
    ON handoff_events (tenant_id, conversation_id, seq);

CREATE TABLE outbox (
    id BIGSERIAL NOT NULL,
    topic TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    event_seq BIGINT NOT NULL,
    payload JSON NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    CONSTRAINT outbox_pkey PRIMARY KEY (id),
    CONSTRAINT outbox_topic_event_seq_uniq UNIQUE (topic, event_seq),
    CONSTRAINT outbox_event_fkey FOREIGN KEY (event_seq) REFERENCES handoff_events (seq) ON DELETE CASCADE,
    CONSTRAINT outbox_topic_size_check CHECK (octet_length(topic) BETWEEN 1 AND 256),
    CONSTRAINT outbox_aggregate_id_size_check CHECK (octet_length(aggregate_id) BETWEEN 1 AND 256),
    CONSTRAINT outbox_event_seq_check CHECK (event_seq >= 0),
    CONSTRAINT outbox_payload_check CHECK (
		json_typeof(payload) = 'object' AND octet_length(payload::text) <= 8192
		AND canonical_json_depth_valid(payload)
    ),
    CONSTRAINT outbox_payload_keys_check CHECK (
		outbox_payload_keys_valid(payload)
    ),
    CONSTRAINT outbox_attempts_check CHECK (attempts >= 0),
    CONSTRAINT outbox_last_error_size_check CHECK (last_error IS NULL OR octet_length(last_error) <= 2048),
    CONSTRAINT outbox_timestamps_check CHECK (published_at IS NULL OR published_at >= created_at)
);

CREATE INDEX outbox_unpublished_idx
    ON outbox (created_at)
    WHERE published_at IS NULL;

CREATE INDEX outbox_event_seq_idx
    ON outbox (event_seq);

CREATE TABLE tool_invocations (
    invocation_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    handoff_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    objective_revision BIGINT NOT NULL,
    tool_ordinal INTEGER NOT NULL,
    tool_name TEXT NOT NULL,
    approval_state TEXT NOT NULL,
    approval_expires_at TIMESTAMPTZ,
    idempotency_key TEXT NOT NULL,
    canonical_arguments_hash TEXT NOT NULL,
    arguments_ciphertext BYTEA NOT NULL,
    arguments_nonce BYTEA NOT NULL,
    arguments_key_id TEXT NOT NULL,
    result_metadata JSON,
    result JSON,
    mutation_id TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT tool_invocations_pkey PRIMARY KEY (invocation_id),
    CONSTRAINT tool_invocations_handoff_fkey FOREIGN KEY (tenant_id, conversation_id, handoff_id)
        REFERENCES handoffs (tenant_id, conversation_id, id) ON DELETE CASCADE,
    CONSTRAINT tool_invocations_stable_uniq UNIQUE (
        tenant_id, handoff_id, objective_revision, tool_ordinal, tool_name, canonical_arguments_hash
    ),
    CONSTRAINT tool_invocations_idempotency_uniq UNIQUE (tenant_id, handoff_id, idempotency_key),
    CONSTRAINT tool_invocations_invocation_id_size_check CHECK (octet_length(invocation_id) BETWEEN 1 AND 256),
    CONSTRAINT tool_invocations_attempt_check CHECK (attempt >= 0),
    CONSTRAINT tool_invocations_objective_revision_check CHECK (objective_revision >= 0),
    CONSTRAINT tool_invocations_tool_ordinal_check CHECK (tool_ordinal >= 0),
    CONSTRAINT tool_invocations_tool_name_size_check CHECK (octet_length(tool_name) BETWEEN 1 AND 256),
    CONSTRAINT tool_invocations_approval_state_check CHECK (
        approval_state IN ('not_required', 'pending', 'approved', 'denied', 'expired')
    ),
    CONSTRAINT tool_invocations_idempotency_key_size_check CHECK (octet_length(idempotency_key) BETWEEN 1 AND 256),
    CONSTRAINT tool_invocations_arguments_hash_size_check CHECK (octet_length(canonical_arguments_hash) BETWEEN 1 AND 512),
    CONSTRAINT tool_invocations_ciphertext_check CHECK (octet_length(arguments_ciphertext) BETWEEN 16 AND 65552),
    CONSTRAINT tool_invocations_nonce_check CHECK (octet_length(arguments_nonce) = 12),
    CONSTRAINT tool_invocations_key_id_size_check CHECK (octet_length(arguments_key_id) BETWEEN 1 AND 256),
    CONSTRAINT tool_invocations_result_metadata_check CHECK (
		result_metadata IS NULL OR (
			json_typeof(result_metadata) = 'object' AND octet_length(result_metadata::text) <= 65536
			AND canonical_json_depth_valid(result_metadata)
		)
    ),
	CONSTRAINT tool_invocations_result_check CHECK (
		result IS NULL OR (octet_length(result::text) <= 65536 AND canonical_json_depth_valid(result))
	),
    CONSTRAINT tool_invocations_mutation_id_size_check CHECK (mutation_id IS NULL OR octet_length(mutation_id) BETWEEN 1 AND 256),
    CONSTRAINT tool_invocations_status_check CHECK (status IN ('pending', 'running', 'succeeded', 'failed', 'cancelled')),
    CONSTRAINT tool_invocations_timestamps_check CHECK (updated_at >= created_at)
);

CREATE TABLE delivery_cursors (
    consumer_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    last_sequence BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT delivery_cursors_pkey PRIMARY KEY (consumer_id, tenant_id, conversation_id),
    CONSTRAINT delivery_cursors_conversation_fkey FOREIGN KEY (tenant_id, conversation_id)
        REFERENCES conversations (tenant_id, id) ON DELETE CASCADE,
    CONSTRAINT delivery_cursors_consumer_id_size_check CHECK (octet_length(consumer_id) BETWEEN 1 AND 256),
    CONSTRAINT delivery_cursors_sequence_check CHECK (last_sequence >= 0),
    CONSTRAINT delivery_cursors_timestamps_check CHECK (updated_at >= created_at)
);

CREATE TABLE session_admission_leases (
    instance_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    heartbeat_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT session_admission_leases_pkey PRIMARY KEY (instance_id, session_id),
    CONSTRAINT session_admission_leases_instance_id_size_check CHECK (octet_length(instance_id) BETWEEN 1 AND 256),
    CONSTRAINT session_admission_leases_session_id_size_check CHECK (octet_length(session_id) BETWEEN 1 AND 256),
    CONSTRAINT session_admission_leases_tenant_id_size_check CHECK (octet_length(tenant_id) BETWEEN 1 AND 256),
    CONSTRAINT session_admission_leases_user_id_size_check CHECK (octet_length(user_id) BETWEEN 1 AND 256),
    CONSTRAINT session_admission_leases_timestamps_check CHECK (
        heartbeat_at >= created_at AND expires_at >= heartbeat_at
    )
);

CREATE INDEX session_admission_leases_active_tenant_user_idx
    ON session_admission_leases (tenant_id, user_id, expires_at);

CREATE TABLE deletion_audits (
    audit_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    conversation_hash TEXT NOT NULL,
    actor TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    deleted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT deletion_audits_pkey PRIMARY KEY (audit_id),
    CONSTRAINT deletion_audits_audit_id_size_check CHECK (octet_length(audit_id) BETWEEN 1 AND 256),
    CONSTRAINT deletion_audits_tenant_id_size_check CHECK (octet_length(tenant_id) BETWEEN 1 AND 256),
    CONSTRAINT deletion_audits_conversation_hash_size_check CHECK (octet_length(conversation_hash) BETWEEN 1 AND 512),
    CONSTRAINT deletion_audits_actor_size_check CHECK (octet_length(actor) BETWEEN 1 AND 256),
    CONSTRAINT deletion_audits_reason_code_size_check CHECK (octet_length(reason_code) BETWEEN 1 AND 256)
);
