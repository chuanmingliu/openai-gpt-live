package cliopt

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOptionalPathRejectsEmptyInvalidAndDuplicateConfigFlags(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "empty", value: ""},
		{name: "invalid UTF-8", value: string([]byte{0xff})},
		{name: "control", value: "config\n.yaml"},
		{name: "too long", value: strings.Repeat("x", 4097)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var path OptionalPath
			require.EqualError(t, path.Set(test.value), "configuration path is invalid")
			require.False(t, path.Present())
			require.Empty(t, path.String())
		})
	}

	var path OptionalPath
	require.NoError(t, path.Set("gateway.yaml"))
	require.True(t, path.Present())
	require.Equal(t, "gateway.yaml", path.Path())
	require.Empty(t, path.String())
	require.EqualError(t, path.Set("other.yaml"), "configuration path was provided more than once")
}
