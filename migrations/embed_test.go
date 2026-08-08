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
