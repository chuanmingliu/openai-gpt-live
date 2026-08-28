package secretbuf

import (
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"
)

var errTextUnavailableOrInvalid = errors.New("secret value is unavailable or invalid")

// LookupText resolves one bounded textual secret without ever including its
// value in an error.
func LookupText(lookup func(string) (string, bool), name string, maximumBytes int) (string, error) {
	if lookup == nil || maximumBytes <= 0 {
		return "", errTextUnavailableOrInvalid
	}
	value, ok := lookup(name)
	if !ok || value == "" || len(value) > maximumBytes || !utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return "", errTextUnavailableOrInvalid
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return "", errTextUnavailableOrInvalid
		}
	}
	return value, nil
}
