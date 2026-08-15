package prompttmpl

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"
)

// ErrContextInvalid reports a context that is outside the declared schema or
// bounded JSON-like value domain. It deliberately carries no customer data.
var ErrContextInvalid = errors.New("context_invalid")

const maxContextSchemaDepth = 16

const maxContextSchemaTextBytes = 256

// ValidateContext admits only the declared values a template may access and
// returns a fully owned copy. Nil is the explicit representation of an
// optional value; a required value must be present and non-nil.
func ValidateContext(schema ContextSchema, input Context, templateName string) (Context, error) {
	if !validContextSchema(schema) || !validContextTemplateName(templateName) {
		return nil, ErrContextInvalid
	}
	if input == nil {
		input = Context{}
	}
	budget := contextValidationBudget{bytes: DefaultContextBytes, items: DefaultCollectionItems}
	if err := budget.add(1); err != nil { // {
		return nil, ErrContextInvalid
	}
	owned := make(Context, len(input))
	for name, value := range input {
		field, ok := schema.Fields[name]
		if !ok || !contextFieldAllows(field, templateName) || !validContextText(name, false) {
			return nil, ErrContextInvalid
		}
		if err := budget.item(); err != nil {
			return nil, ErrContextInvalid
		}
		if err := budget.jsonString(name); err != nil || budget.add(1) != nil { // :
			return nil, ErrContextInvalid
		}
		start := budget.used
		copied, err := cloneValidatedContextValue(&budget, value, 0, make(map[contextVisit]bool))
		if err != nil || !matchesContextField(field.Type, copied) || (field.Required && copied == nil) {
			return nil, ErrContextInvalid
		}
		if field.MaxBytes > 0 && contextFieldBytes(copied, budget.used-start) > field.MaxBytes {
			return nil, ErrContextInvalid
		}
		if field.MaxItems > 0 && contextFieldItems(copied) > field.MaxItems {
			return nil, ErrContextInvalid
		}
		owned[name] = copied
		if err := budget.add(1); err != nil { // comma or }
			return nil, ErrContextInvalid
		}
	}
	if len(input) == 0 {
		if err := budget.add(1); err != nil { // }
			return nil, ErrContextInvalid
		}
	}
	for name, field := range schema.Fields {
		if field.Required && contextFieldAllows(field, templateName) {
			if value, ok := input[name]; !ok || value == nil {
				return nil, ErrContextInvalid
			}
		}
	}
	return owned, nil
}

func validContextSchema(schema ContextSchema) bool {
	if len(schema.Fields) > DefaultCollectionItems {
		return false
	}
	for name, field := range schema.Fields {
		if !validContextText(name, false) || !validContextFieldType(field.Type) || field.MaxBytes < 0 || field.MaxBytes > MaxContextBytes || field.MaxItems < 0 || field.MaxItems > MaxCollectionItems || !validContextText(field.Sensitivity, true) || !validContextText(field.Source, true) || len(field.Templates) > DefaultCollectionItems || !validContextACL(field.Templates) {
			return false
		}
	}
	return true
}

func validContextACL(templates []string) bool {
	seen := make(map[string]struct{}, len(templates))
	for _, template := range templates {
		if !validContextTemplateName(template) {
			return false
		}
		if _, exists := seen[template]; exists {
			return false
		}
		seen[template] = struct{}{}
	}
	return true
}

func validContextTemplateName(name string) bool {
	if !validContextText(name, false) || len(name) < len("a.j2") || !strings.HasSuffix(name, ".j2") || !isContextLowerAlphaNum(name[0]) {
		return false
	}
	for _, byteValue := range []byte(name) {
		if !(byteValue >= 'a' && byteValue <= 'z' || byteValue >= '0' && byteValue <= '9' || byteValue == '_' || byteValue == '.' || byteValue == '/' || byteValue == '-') {
			return false
		}
	}
	for _, segment := range strings.Split(name, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return false
		}
	}
	return true
}

func isContextLowerAlphaNum(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= '0' && value <= '9'
}

func validContextText(value string, allowEmpty bool) bool {
	if (!allowEmpty && value == "") || len(value) > maxContextSchemaTextBytes || !utf8.ValidString(value) {
		return false
	}
	for _, runeValue := range value {
		if unicode.IsControl(runeValue) {
			return false
		}
	}
	return true
}

func validContextFieldType(fieldType FieldType) bool {
	return fieldType == TypeString || fieldType == TypeBool || fieldType == TypeNumber || fieldType == TypeObject || fieldType == TypeArray
}

func contextFieldBytes(value any, encoded int) int {
	if text, ok := value.(string); ok {
		return len(text)
	}
	return encoded
}

func contextFieldItems(value any) int {
	switch value := value.(type) {
	case map[string]any:
		return len(value)
	case []any:
		return len(value)
	default:
		return 0
	}
}

func contextFieldAllows(field Field, templateName string) bool {
	if len(field.Templates) == 0 {
		return true
	}
	for _, name := range field.Templates {
		if name == templateName {
			return true
		}
	}
	return false
}

func matchesContextField(want FieldType, value any) bool {
	if value == nil {
		return true
	}
	switch want {
	case TypeString:
		_, ok := value.(string)
		return ok
	case TypeBool:
		_, ok := value.(bool)
		return ok
	case TypeNumber:
		_, ok := value.(json.Number)
		return ok
	case TypeObject:
		_, ok := value.(map[string]any)
		return ok
	case TypeArray:
		_, ok := value.([]any)
		return ok
	default:
		return false
	}
}

type contextValidationBudget struct {
	used, count  int
	bytes, items int
}

func (b *contextValidationBudget) add(n int) error {
	if n < 0 || n > b.bytes-b.used {
		return ErrContextInvalid
	}
	b.used += n
	return nil
}

func (b *contextValidationBudget) item() error {
	if b.count >= b.items {
		return ErrContextInvalid
	}
	b.count++
	return nil
}

func (b *contextValidationBudget) jsonString(value string) error {
	if !utf8.ValidString(value) {
		return ErrContextInvalid
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return ErrContextInvalid
	}
	return b.add(len(encoded))
}

type contextVisit struct {
	kind reflect.Kind
	ptr  uintptr
}

func cloneValidatedContextValue(budget *contextValidationBudget, value any, depth int, visiting map[contextVisit]bool) (any, error) {
	if depth > maxContextSchemaDepth {
		return nil, ErrContextInvalid
	}
	switch value := value.(type) {
	case nil:
		return nil, budget.add(4)
	case string:
		return value, budget.jsonString(value)
	case bool:
		if value {
			return value, budget.add(4)
		}
		return value, budget.add(5)
	case json.Number:
		if !validJSONNumber(value) {
			return nil, ErrContextInvalid
		}
		if err := budget.add(len(value)); err != nil {
			return nil, err
		}
		return value, nil
	case map[string]any:
		visit := contextVisit{kind: reflect.Map, ptr: reflect.ValueOf(value).Pointer()}
		if visiting[visit] {
			return nil, ErrContextInvalid
		}
		if value == nil {
			return nil, budget.add(4)
		}
		if len(value) > budget.items-budget.count {
			return nil, ErrContextInvalid
		}
		visiting[visit] = true
		defer delete(visiting, visit)
		if err := budget.add(1); err != nil { // {
			return nil, err
		}
		copy := make(map[string]any, len(value))
		for key, nested := range value {
			if !utf8.ValidString(key) || budget.item() != nil || budget.jsonString(key) != nil || budget.add(1) != nil {
				return nil, ErrContextInvalid
			}
			cloned, err := cloneValidatedContextValue(budget, nested, depth+1, visiting)
			if err != nil {
				return nil, err
			}
			copy[key] = cloned
			if err := budget.add(1); err != nil {
				return nil, err
			}
		}
		if len(value) == 0 {
			if err := budget.add(1); err != nil {
				return nil, err
			}
		}
		return copy, nil
	case []any:
		if value == nil {
			return nil, budget.add(4)
		}
		visit := contextVisit{kind: reflect.Slice, ptr: reflect.ValueOf(value).Pointer()}
		if visiting[visit] {
			return nil, ErrContextInvalid
		}
		if len(value) > budget.items-budget.count {
			return nil, ErrContextInvalid
		}
		visiting[visit] = true
		defer delete(visiting, visit)
		if err := budget.add(1); err != nil { // [
			return nil, err
		}
		copy := make([]any, len(value))
		for index, nested := range value {
			if err := budget.item(); err != nil {
				return nil, err
			}
			cloned, err := cloneValidatedContextValue(budget, nested, depth+1, visiting)
			if err != nil {
				return nil, err
			}
			copy[index] = cloned
			if err := budget.add(1); err != nil {
				return nil, err
			}
		}
		if len(value) == 0 {
			if err := budget.add(1); err != nil {
				return nil, err
			}
		}
		return copy, nil
	default:
		return nil, ErrContextInvalid
	}
}

func validJSONNumber(value json.Number) bool {
	raw := string(value)
	if raw == "" || strings.TrimSpace(raw) != raw || (raw[0] != '-' && (raw[0] < '0' || raw[0] > '9')) || !json.Valid([]byte(raw)) {
		return false
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return false
	}
	_, ok := decoded.(json.Number)
	return ok
}
