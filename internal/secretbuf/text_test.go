package secretbuf

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLookupTextRejectsUntrustedValuesWithoutReturningContent(t *testing.T) {
	tests := []struct {
		name  string
		value string
		ok    bool
	}{
		{name: "missing"},
		{name: "empty", ok: true},
		{name: "whitespace", value: "  value", ok: true},
		{name: "invalid UTF-8", value: string([]byte{0xff}), ok: true},
		{name: "control", value: "value\n", ok: true},
		{name: "too long", value: strings.Repeat("x", 17), ok: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value, err := LookupText(func(string) (string, bool) { return test.value, test.ok }, "SECRET_NAME", 16)
			require.EqualError(t, err, "secret value is unavailable or invalid")
			assert.Empty(t, value)
			if test.value != "" {
				assert.NotContains(t, err.Error(), test.value)
			}
		})
	}
}

func TestLookupTextReturnsBoundedText(t *testing.T) {
	value, err := LookupText(func(string) (string, bool) { return "gateway-secret", true }, "SECRET_NAME", 16)
	require.NoError(t, err)
	assert.Equal(t, "gateway-secret", value)
}
