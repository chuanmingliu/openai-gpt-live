package postgres

import (
	"errors"
	"strings"
	"testing"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

func TestDeletionConversationHashMatchesDomainSeparatedKeyedVector(t *testing.T) {
	repo := &Repository{
		retentionHMACKeyID: "retention-key-v1",
		retentionHMACKey:   []byte("0123456789abcdef0123456789abcdef"),
	}

	got := repo.deletionConversationHash("tenant-a", "conversation-42")
	assert.Equal(t, "e274b448444d502ae1a87183f848105506006b4f386b22ac66f955d63f17c757", got)
	assert.NotContains(t, got, "conversation-42")
	assert.NotEqual(t, got, repo.deletionConversationHash("tenant-b", "conversation-42"))
}

func TestNewRepositoryFailureZeroizesAppliedRetentionOptionCopy(t *testing.T) {
	source := []byte(strings.Repeat("s", 32))
	retention := WithRetentionHMAC("retention-key-v1", source)
	var applied []byte
	fault := errors.New("later option failed")
	_, err := NewRepository(&pgxpool.Pool{}, clock.Real{}, retention, func(repo *Repository) error {
		applied = repo.retentionHMACKey
		return fault
	})
	assert.ErrorIs(t, err, fault)
	assert.NotEmpty(t, applied)
	for _, value := range applied {
		assert.Zero(t, value)
	}
	assert.Equal(t, []byte(strings.Repeat("s", 32)), source, "caller retains and must clear its source ownership")
	clear(source)
	for _, value := range source {
		assert.Zero(t, value)
	}
}

func TestRepositoryCloseSensitiveZeroizesOwnedRetentionKeyIdempotently(t *testing.T) {
	repo := &Repository{retentionHMACKeyID: "retention-key-v1", retentionHMACKey: []byte(strings.Repeat("k", 32))}
	owned := repo.retentionHMACKey
	assert.True(t, repo.RetentionConfigured())

	repo.CloseSensitive()
	assert.False(t, repo.RetentionConfigured())
	assert.Empty(t, repo.retentionHMACKeyID)
	for _, value := range owned {
		assert.Zero(t, value)
	}
	repo.CloseSensitive()
	assert.False(t, repo.RetentionConfigured())
}
