package reasoner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildPrefixIsStableAndBounded(t *testing.T) {
	first, err := BuildPrefix("conv", "", []PrefixLine{
		{Role: "user", Text: "look this up"},
		{Role: "assistant", Text: "working"},
		{Role: "tool", Text: "ignored"},
		{Role: "user", Text: "   "},
	})
	require.NoError(t, err)
	assert.Equal(t, "conversation:conv", first.Ref)
	assert.Len(t, first.Lines, 2)
	second, err := BuildPrefix("conv", "", []PrefixLine{
		{Role: "user", Text: "look this up"},
		{Role: "assistant", Text: "working"},
	})
	require.NoError(t, err)
	assert.Equal(t, first.Hash, second.Hash)
	assert.JSONEq(t, string(first.Raw), string(second.Raw))
}

func TestVerifyPrefixRejectsHashMismatch(t *testing.T) {
	prefix, err := BuildPrefix("conv", "", []PrefixLine{{Role: "user", Text: "look this up"}})
	require.NoError(t, err)
	_, err = VerifyPrefix(prefix.Raw, "deadbeef")
	assert.Error(t, err)
	lines, err := VerifyPrefix(prefix.Raw, prefix.Hash)
	require.NoError(t, err)
	assert.Equal(t, prefix.Lines, lines)
}

func TestIsBackchannelRecognizesAcknowledgementsOnly(t *testing.T) {
	assert.True(t, IsBackchannel("mm-hmm"))
	assert.True(t, IsBackchannel("  Yeah!  "))
	assert.True(t, IsBackchannel("got it."))
	assert.False(t, IsBackchannel("look this up"))
	assert.False(t, IsBackchannel("okay, search the launch brief"))
}
