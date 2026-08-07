package safety_test

import (
	"encoding/base64"
	"testing"

	"github.com/chuanmingliu/openai-gpt-live/internal/safety"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProviderSafetyIDGoldenVectorAndAmbiguityResistance(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	one, err := safety.ProviderSafetyID(key, "ab", "c")
	require.NoError(t, err)
	assert.Equal(t, "W8aXDzoNtda2ePEiCiTqv8IIZUf2PWQF5m24VuTu7eU", one)
	two, err := safety.ProviderSafetyID(key, "a", "bc")
	require.NoError(t, err)
	assert.NotEqual(t, one, two)
	decoded, err := base64.RawURLEncoding.DecodeString(one)
	require.NoError(t, err)
	assert.Len(t, decoded, 32)
}

func TestProviderSafetyIDCopiesInputsAndChecksBoundaries(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	tenant := []byte("tenant")
	user := []byte("user")
	want, err := safety.ProviderSafetyID(key, string(tenant), string(user))
	require.NoError(t, err)
	key[0], tenant[0], user[0] = 'x', 'x', 'x'
	assert.Equal(t, "OMH3aalkmmcIAdp-yyYV1cfTHd0zmnGdxtfaXvkYrPY", want)
	mutated, err := safety.ProviderSafetyID([]byte("1123456789abcdef0123456789abcdef"), "tenant", "user")
	require.NoError(t, err)
	assert.NotEqual(t, want, mutated)
	boundary, err := safety.ProviderSafetyID(make([]byte, 32), string(make([]byte, 512)), "u")
	require.NoError(t, err)
	assert.Len(t, boundary, 43)

	_, err = safety.ProviderSafetyID(make([]byte, 31), "tenant", "user")
	assert.Error(t, err)
	_, err = safety.ProviderSafetyID(make([]byte, 32), "", "user")
	assert.Error(t, err)
	_, err = safety.ProviderSafetyID(make([]byte, 32), "tenant", string([]byte{0xff}))
	assert.Error(t, err)
	_, err = safety.ProviderSafetyID(make([]byte, 32), string(make([]byte, 513)), "user")
	assert.Error(t, err)
}
