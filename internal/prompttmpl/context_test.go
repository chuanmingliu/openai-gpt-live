package prompttmpl

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateContextRejectsUndeclaredAndOversizedValues(t *testing.T) {
	schema := ContextSchema{Fields: map[string]Field{
		"customer_name": {Type: TypeString, Required: true, MaxBytes: 64, Templates: []string{"system.j2"}},
	}}
	_, err := ValidateContext(schema, Context{"customer_name": "Li", "secret": "x"}, "system.j2")
	require.ErrorIs(t, err, ErrContextInvalid)

	_, err = ValidateContext(schema, Context{"customer_name": strings.Repeat("x", 65)}, "system.j2")
	require.ErrorIs(t, err, ErrContextInvalid)
}

func TestValidateContextEnforcesRequiredNullAndTemplateAccess(t *testing.T) {
	schema := ContextSchema{Fields: map[string]Field{
		"required": {Type: TypeString, Required: true, Templates: []string{"system.j2"}},
		"optional": {Type: TypeBool},
	}}

	_, err := ValidateContext(schema, Context{"required": nil}, "system.j2")
	require.ErrorIs(t, err, ErrContextInvalid)

	got, err := ValidateContext(schema, Context{"required": "present", "optional": nil}, "system.j2")
	require.NoError(t, err)
	require.Equal(t, Context{"required": "present", "optional": nil}, got)

	aclSchema := ContextSchema{Fields: map[string]Field{
		"required": {Type: TypeString, Required: true, Templates: []string{"system.j2"}},
		"secret":   {Type: TypeString, Templates: []string{"turn_instruction.j2"}},
	}}
	_, err = ValidateContext(aclSchema, Context{"required": "present", "secret": "private"}, "system.j2")
	require.ErrorIs(t, err, ErrContextInvalid)
}

func TestValidateContextEnforcesFieldCollectionBounds(t *testing.T) {
	schema := ContextSchema{Fields: map[string]Field{
		"items": {Type: TypeArray, MaxItems: 1},
	}}
	_, err := ValidateContext(schema, Context{"items": []any{"one", "two"}}, "system.j2")
	require.ErrorIs(t, err, ErrContextInvalid)
}

func TestValidateContextRejectsInvalidFieldTypeEvenForNull(t *testing.T) {
	schema := ContextSchema{Fields: map[string]Field{"value": {Type: "unexpected"}}}
	_, err := ValidateContext(schema, Context{"value": nil}, "system.j2")
	require.ErrorIs(t, err, ErrContextInvalid)
}

func TestValidateContextRejectsInvalidUTF8AndResourceExhaustion(t *testing.T) {
	schema := ContextSchema{Fields: map[string]Field{
		"value": {Type: TypeArray, MaxItems: DefaultCollectionItems},
		"text":  {Type: TypeString},
	}}

	_, err := ValidateContext(schema, Context{"value": []any{"\xff"}}, "system.j2")
	require.ErrorIs(t, err, ErrContextInvalid)

	tooMany := make([]any, DefaultCollectionItems+1)
	_, err = ValidateContext(schema, Context{"value": tooMany}, "system.j2")
	require.ErrorIs(t, err, ErrContextInvalid)

	deep := any("leaf")
	for range 17 {
		deep = []any{deep}
	}
	_, err = ValidateContext(schema, Context{"value": deep}, "system.j2")
	require.ErrorIs(t, err, ErrContextInvalid)

	_, err = ValidateContext(schema, Context{"text": strings.Repeat("x", DefaultContextBytes)}, "system.j2")
	require.ErrorIs(t, err, ErrContextInvalid)
}

func TestValidateContextReturnsAnOwnedTree(t *testing.T) {
	schema := ContextSchema{Fields: map[string]Field{
		"customer": {Type: TypeObject},
		"items":    {Type: TypeArray},
		"number":   {Type: TypeNumber},
	}}
	input := Context{
		"customer": map[string]any{"name": "Li"},
		"items":    []any{map[string]any{"code": "A"}},
		"number":   json.Number("42"),
	}

	got, err := ValidateContext(schema, input, "system.j2")
	require.NoError(t, err)
	input["customer"].(map[string]any)["name"] = "changed"
	input["items"].([]any)[0].(map[string]any)["code"] = "changed"

	require.Equal(t, "Li", got["customer"].(map[string]any)["name"])
	require.Equal(t, "A", got["items"].([]any)[0].(map[string]any)["code"])
	require.Equal(t, json.Number("42"), got["number"])
}

func TestValidateContextAcceptsJSONGrammarNumbersOutsideMachineRange(t *testing.T) {
	schema := ContextSchema{Fields: map[string]Field{"number": {Type: TypeNumber}}}
	for _, number := range []json.Number{"9223372036854775808", "1e309", "-0.0000000000000000000000000000000000001"} {
		got, err := ValidateContext(schema, Context{"number": number}, "system.j2")
		require.NoError(t, err, number)
		require.Equal(t, number, got["number"])
	}
}

func TestValidateContextRejectsInvalidSchemaMetadataWithoutInput(t *testing.T) {
	invalid := []ContextSchema{
		{Fields: map[string]Field{"bad\nname": {Type: TypeString}}},
		{Fields: map[string]Field{"field": {Type: TypeString, MaxBytes: -1}}},
		{Fields: map[string]Field{"field": {Type: TypeArray, MaxItems: DefaultCollectionItems + 1}}},
		{Fields: map[string]Field{"field": {Type: TypeString, Templates: []string{"system.j2", "system.j2"}}}},
		{Fields: map[string]Field{"field": {Type: TypeString, Templates: []string{"bad\xff.j2"}}}},
	}
	for _, schema := range invalid {
		_, err := ValidateContext(schema, Context{}, "system.j2")
		require.ErrorIs(t, err, ErrContextInvalid)
	}
}
