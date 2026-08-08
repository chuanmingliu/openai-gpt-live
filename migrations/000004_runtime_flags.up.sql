CREATE TABLE runtime_feature_authority (
    singleton boolean PRIMARY KEY DEFAULT true,
    version bigint NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT runtime_feature_authority_singleton_check CHECK (singleton),
    CONSTRAINT runtime_feature_authority_version_check CHECK (version > 0)
);

CREATE TABLE runtime_feature_flags (
    name text PRIMARY KEY,
    enabled boolean NOT NULL,
    version bigint NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT runtime_feature_flags_name_check CHECK (name IN ('handoffs_enabled', 'shadow_mode')),
    CONSTRAINT runtime_feature_flags_version_check CHECK (version > 0)
);

CREATE TABLE runtime_feature_audit (
    version bigint PRIMARY KEY,
    name text NOT NULL,
    enabled boolean NOT NULL,
    actor text NOT NULL,
    reason text NOT NULL,
    changed_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT runtime_feature_audit_name_check CHECK (name IN ('handoffs_enabled', 'shadow_mode')),
    CONSTRAINT runtime_feature_audit_actor_size_check CHECK (octet_length(actor) BETWEEN 1 AND 256),
    CONSTRAINT runtime_feature_audit_reason_size_check CHECK (octet_length(reason) BETWEEN 1 AND 256),
    CONSTRAINT runtime_feature_audit_version_check CHECK (version > 1)
);

CREATE FUNCTION runtime_feature_audit_reject_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'runtime feature audit is append-only' USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER runtime_feature_audit_append_only
BEFORE UPDATE OR DELETE ON runtime_feature_audit
FOR EACH ROW EXECUTE FUNCTION runtime_feature_audit_reject_mutation();

CREATE TRIGGER runtime_feature_audit_reject_truncate
BEFORE TRUNCATE ON runtime_feature_audit
FOR EACH STATEMENT EXECUTE FUNCTION runtime_feature_audit_reject_mutation();

INSERT INTO runtime_feature_authority (singleton, version) VALUES (true, 1);
INSERT INTO runtime_feature_flags (name, enabled, version) VALUES
    ('handoffs_enabled', true, 1),
    ('shadow_mode', false, 1);
