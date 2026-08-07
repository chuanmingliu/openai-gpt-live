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

func TestCoordinatorPendingRefusalClassificationIsExact(t *testing.T) {
	assert.True(t, isCoordinatorPendingRefusal(&pgconn.PgError{Code: "55000", Message: ErrCoordinatorPendingWork.Error()}))
	assert.False(t, isCoordinatorPendingRefusal(&pgconn.PgError{Code: "57014", Message: ErrCoordinatorPendingWork.Error()}))
	assert.False(t, isCoordinatorPendingRefusal(&pgconn.PgError{Code: "55000", Message: "arbitrary prerequisite failure"}))
}
