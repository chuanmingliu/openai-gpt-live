package migrations

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTask11MigrationEmbeddedChecksums(t *testing.T) {
	want := map[string]string{
		"000002_task11_coordinator_replay.up.sql":   "902012b3c544dc10df451187e4dc675b7f0ead46573b0b8c3a790b75d88a087a",
		"000002_task11_coordinator_replay.down.sql": "97f6f8a37969269e4edfcc67c0060a9f7e966aa7fe83612164d137672a423283",
	}
	for name, checksum := range want {
		contents, err := files.ReadFile(name)
		require.NoError(t, err)
		digest := sha256.Sum256(contents)
		assert.Equal(t, checksum, hex.EncodeToString(digest[:]), name)
	}
}

func TestTask12MigrationEmbeddedChecksums(t *testing.T) {
	want := map[string]string{
		"000003_task12_tool_invocation_scope.up.sql":   "1cf28c19130362441c73a937967fe33e0bc2dc1f0052dae3c9ad9f7d5ead9711",
		"000003_task12_tool_invocation_scope.down.sql": "8ca1c778456c6ceada9655865c3a9ff576ec9b8b5f75e86935edca60e393fd2c",
	}
	for name, checksum := range want {
		contents, err := files.ReadFile(name)
		require.NoError(t, err)
		digest := sha256.Sum256(contents)
		assert.Equal(t, checksum, hex.EncodeToString(digest[:]), name)
	}
}

func TestTask20MigrationEmbeddedChecksums(t *testing.T) {
	want := map[string]string{
		"000004_runtime_flags.up.sql":   "c6b3736ecdf0a8be66c99430383f75b8ef7236e2e345a7dd5bf48be49cb0a7ff",
		"000004_runtime_flags.down.sql": "7434b93a436203cb4a9713df54b46bea71b47a2ca96027ac8e8b39cf446d5af7",
	}
	for name, checksum := range want {
		contents, err := files.ReadFile(name)
		require.NoError(t, err)
		digest := sha256.Sum256(contents)
		assert.Equal(t, checksum, hex.EncodeToString(digest[:]), name)
	}
}

func TestTask21MigrationEmbeddedChecksums(t *testing.T) {
	want := map[string]string{
		"000005_deletion_provenance.up.sql":   "9ecbfaf6a25da3f84f077764351a0d4aba51713353af7fd7cf7d8cd1b70dbf55",
		"000005_deletion_provenance.down.sql": "d3b5110ffcac169889b0bb838f23c9d47f94407bffe51635d44959712209a94c",
	}
	for name, checksum := range want {
		contents, err := files.ReadFile(name)
		require.NoError(t, err)
		digest := sha256.Sum256(contents)
		assert.Equal(t, checksum, hex.EncodeToString(digest[:]), name)
	}
}

func TestReconnectOwnerMigrationEmbeddedChecksums(t *testing.T) {
	want := map[string]string{
		"000006_reconnect_owners.up.sql":   "981f22d9f314f8074d65115b3d47ae51addf1aa26ceb953d1bf38ef6ec786cc6",
		"000006_reconnect_owners.down.sql": "65398f2e482b2fb26ac91522adf499deb2cbb07f5e1b535c26b655ffb16ba64f",
	}
	for name, checksum := range want {
		contents, err := files.ReadFile(name)
		require.NoError(t, err)
		digest := sha256.Sum256(contents)
		assert.Equal(t, checksum, hex.EncodeToString(digest[:]), name)
	}
}

func TestCoordinatorDeliveryShardMigrationEmbeddedChecksums(t *testing.T) {
	want := map[string]string{
		"000007_coordinator_delivery_shards.up.sql":   "260e46deeaa6aa6e5044e28177d08b9c1fc2ae29189d121c51c335243fb4d116",
		"000007_coordinator_delivery_shards.down.sql": "d6e67928b51ddceb4363706a187302e057ca32ee2baef17d8282b68f9382cf7d",
	}
	for name, checksum := range want {
		contents, err := files.ReadFile(name)
		require.NoError(t, err)
		digest := sha256.Sum256(contents)
		assert.Equal(t, checksum, hex.EncodeToString(digest[:]), name)
	}
}

func TestHandoffToolPolicyMigrationCatalogIsExact(t *testing.T) {
	wantChecksums := map[string]string{
		"000008_handoff_tool_policy.up.sql":   "1d8c8b02aeb778c6f7dd773e202af3385c7898f1ec597109b35452377adc2af7",
		"000008_handoff_tool_policy.down.sql": "a58d92d73037821a1358840f786f81ac006c04dc17675bde52ac2765607e1697",
	}
	for name, checksum := range wantChecksums {
		contents, err := files.ReadFile(name)
		require.NoError(t, err)
		digest := sha256.Sum256(contents)
		assert.Equal(t, checksum, hex.EncodeToString(digest[:]), name)
	}
	up, err := files.ReadFile("000008_handoff_tool_policy.up.sql")
	require.NoError(t, err)
	down, err := files.ReadFile("000008_handoff_tool_policy.down.sql")
	require.NoError(t, err)
	assert.Equal(t, `ALTER TABLE handoffs
    ADD COLUMN tool_policy JSONB NOT NULL
    DEFAULT '{"web_search":{"enabled":false,"safe_search":"moderate","region":"wt-wt","max_results":5,"timeout_ms":8000}}'::JSONB;
`, string(up))
	assert.Equal(t, `LOCK TABLE handoffs IN ACCESS EXCLUSIVE MODE;

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
`, string(down))
}

func TestEmbeddedVerticalProfileMigrationCatalogIsExact(t *testing.T) {
	assert.Equal(t, uint(11), latestVersion)
	wantNames := []string{
		"000001_durable_handoffs.down.sql", "000001_durable_handoffs.up.sql",
		"000002_task11_coordinator_replay.down.sql", "000002_task11_coordinator_replay.up.sql",
		"000003_task12_tool_invocation_scope.down.sql", "000003_task12_tool_invocation_scope.up.sql",
		"000004_runtime_flags.down.sql", "000004_runtime_flags.up.sql",
		"000005_deletion_provenance.down.sql", "000005_deletion_provenance.up.sql",
		"000006_reconnect_owners.down.sql", "000006_reconnect_owners.up.sql",
		"000007_coordinator_delivery_shards.down.sql", "000007_coordinator_delivery_shards.up.sql",
		"000008_handoff_tool_policy.down.sql", "000008_handoff_tool_policy.up.sql",
		"000009_vertical_profiles.down.sql", "000009_vertical_profiles.up.sql",
		"000010_knowledge_packs.down.sql", "000010_knowledge_packs.up.sql",
		"000011_handoff_profile_snapshot.down.sql", "000011_handoff_profile_snapshot.up.sql",
	}
	entries, err := files.ReadDir(".")
	require.NoError(t, err)
	gotNames := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			gotNames = append(gotNames, entry.Name())
		}
	}
	assert.Equal(t, wantNames, gotNames)
	up, err := files.ReadFile("000011_handoff_profile_snapshot.up.sql")
	require.NoError(t, err)
	down, err := files.ReadFile("000011_handoff_profile_snapshot.down.sql")
	require.NoError(t, err)
	assert.Contains(t, string(up), "profile_snapshot_hash")
	assert.Contains(t, string(down), "handoff profile snapshot downgrade would lose non-default data")
	wantChecksums := map[string]string{
		"000009_vertical_profiles.down.sql":        "14ddeb515492515b2f23a0afe754c1ab31cfa534ec93bed974f46b87798dd39e",
		"000009_vertical_profiles.up.sql":          "363d71258d0fecd30538eaf3bbeb323a1e9c87ac7004226a39ea32f9ca8d7729",
		"000011_handoff_profile_snapshot.up.sql":   "faab92097fa7a1583274f671859bf9da2ae08f03e04e772c3c4d465e482f41dd",
		"000011_handoff_profile_snapshot.down.sql": "6edf23d03428848d18df269d4f653ac7929bfaf2c35ff13d2ba69e010b2cdfb7",
	}
	for name, checksum := range wantChecksums {
		contents, readErr := files.ReadFile(name)
		require.NoError(t, readErr)
		digest := sha256.Sum256(contents)
		assert.Equal(t, checksum, hex.EncodeToString(digest[:]), name)
	}
}

func TestHandoffToolPolicyDataLossRefusalClassificationIsExact(t *testing.T) {
	assert.True(t, isHandoffToolPolicyDataLossRefusal(&pgconn.PgError{Code: "55000", Message: ErrHandoffToolPolicyDataLoss.Error()}))
	assert.False(t, isHandoffToolPolicyDataLossRefusal(&pgconn.PgError{Code: "57014", Message: ErrHandoffToolPolicyDataLoss.Error()}))
	assert.False(t, isHandoffToolPolicyDataLossRefusal(&pgconn.PgError{Code: "55000", Message: "arbitrary prerequisite failure"}))
}

func TestHandoffProfileSnapshotDataLossRefusalClassificationIsExact(t *testing.T) {
	assert.True(t, isHandoffProfileSnapshotDataLossRefusal(&pgconn.PgError{Code: "55000", Message: ErrHandoffProfileSnapshotDataLoss.Error()}))
	assert.False(t, isHandoffProfileSnapshotDataLossRefusal(&pgconn.PgError{Code: "57014", Message: ErrHandoffProfileSnapshotDataLoss.Error()}))
	assert.False(t, isHandoffProfileSnapshotDataLossRefusal(&pgconn.PgError{Code: "55000", Message: "arbitrary prerequisite failure"}))
}

func TestCoordinatorPendingRefusalClassificationIsExact(t *testing.T) {
	assert.True(t, isCoordinatorPendingRefusal(&pgconn.PgError{Code: "55000", Message: ErrCoordinatorPendingWork.Error()}))
	assert.False(t, isCoordinatorPendingRefusal(&pgconn.PgError{Code: "57014", Message: ErrCoordinatorPendingWork.Error()}))
	assert.False(t, isCoordinatorPendingRefusal(&pgconn.PgError{Code: "55000", Message: "arbitrary prerequisite failure"}))
}

func TestToolInvocationScopeRefusalClassificationIsExact(t *testing.T) {
	assert.True(t, isToolInvocationScopeRefusal(&pgconn.PgError{Code: "55000", Message: ErrToolInvocationScopeConflict.Error()}))
	assert.False(t, isToolInvocationScopeRefusal(&pgconn.PgError{Code: "57014", Message: ErrToolInvocationScopeConflict.Error()}))
	assert.False(t, isToolInvocationScopeRefusal(&pgconn.PgError{Code: "55000", Message: "arbitrary prerequisite failure"}))
}

func TestKnowledgePackMigrationEmbeddedChecksums(t *testing.T) {
	want := map[string]string{
		"000010_knowledge_packs.up.sql":   "451b5ae4e890249b5341feeb7bf48d62db00c6a331c90da1958c4a7dba279e36",
		"000010_knowledge_packs.down.sql": "4be578f7067f55842e24c41df4b05e165de3ad4a5c470c977a06e4b529cd40ec",
	}
	for name, checksum := range want {
		contents, err := files.ReadFile(name)
		require.NoError(t, err)
		digest := sha256.Sum256(contents)
		assert.Equal(t, checksum, hex.EncodeToString(digest[:]), name)
	}
}
