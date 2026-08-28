// Package cliopt provides deliberately narrow CLI parsing primitives.
package cliopt

import (
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"
)

const maximumPathBytes = 4 * 1024

var (
	errInvalidPath   = errors.New("configuration path is invalid")
	errDuplicatePath = errors.New("configuration path was provided more than once")
)

// OptionalPath records whether a path flag was supplied without exposing the
// path in flag diagnostics.
type OptionalPath struct {
	path    string
	present bool
}

func (p *OptionalPath) Set(value string) error {
	if p.present {
		return errDuplicatePath
	}
	if value == "" || len(value) > maximumPathBytes || !utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return errInvalidPath
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return errInvalidPath
		}
	}
	p.path = value
	p.present = true
	return nil
}

func (p *OptionalPath) String() string { return "" }

func (p OptionalPath) Present() bool { return p.present }

func (p OptionalPath) Path() string { return p.path }
