package prompttmpl

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

type tokenKind uint8

const (
	tokenExpression tokenKind = iota
	tokenStatement
	tokenComment
)

type sourceToken struct {
	kind      tokenKind
	body      string
	line, col int
}

type policyFailure struct{ line, col int }

func (e *policyFailure) Error() string { return "invalid template syntax" }

func invalidAt(line, col int) error {
	return &policyFailure{line: line, col: col}
}

// ValidateSource validates a single template and every static dependency it
// names. It intentionally reports only a stable class and source location.
func ValidateSource(name, source string, bundleNames []string) error {
	_, err := inspectSource(name, source, bundleNames)
	return err
}

type sourceMeta struct{ StaticDependencies []string }

func inspectSource(name, source string, bundleNames []string) (sourceMeta, error) {
	if err := validateLogicalName(name); err != nil {
		return sourceMeta{}, normalizePolicyError(name, err)
	}
	tokens, err := lexSource(source)
	if err != nil {
		return sourceMeta{}, normalizePolicyError(name, err)
	}
	policy := newSourcePolicy(bundleNames)
	for _, token := range tokens {
		if err := policy.Accept(token); err != nil {
			return sourceMeta{}, normalizePolicyError(name, err)
		}
	}
	return sourceMeta{StaticDependencies: policy.dependencies}, nil
}

// ValidateBundleSources validates all template names, their static includes,
// and the resulting include graph's cycle and depth bounds.
func ValidateBundleSources(templates map[string]string) error {
	names := sortedTemplateNames(templates)
	graph := newIncludeGraph(len(names))
	for _, name := range names {
		meta, err := inspectSource(name, templates[name], names)
		if err != nil {
			return err
		}
		graph.Add(name, meta.StaticDependencies)
	}
	if err := graph.ValidateAcyclic(MaxIncludeDepth); err != nil {
		var failure *graphFailure
		if errors.As(err, &failure) {
			return normalizePolicyError(failure.name, invalidAt(1, 1))
		}
		return normalizePolicyError("bundle.j2", invalidAt(1, 1))
	}
	return nil
}

func sortedTemplateNames(templates map[string]string) []string {
	names := make([]string, 0, len(templates))
	for name := range templates {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func validateLogicalName(name string) error {
	if len(name) < len("a.j2") || !strings.HasSuffix(name, ".j2") || !isLowerAlphaNum(name[0]) {
		return invalidAt(1, 1)
	}
	for i := 0; i < len(name); i++ {
		if !isLogicalNameByte(name[i]) {
			return invalidAt(1, 1)
		}
	}
	for _, segment := range strings.Split(name, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return invalidAt(1, 1)
		}
	}
	return nil
}

func isLowerAlphaNum(b byte) bool { return b >= 'a' && b <= 'z' || b >= '0' && b <= '9' }

func isLogicalNameByte(b byte) bool {
	return isLowerAlphaNum(b) || b == '_' || b == '.' || b == '/' || b == '-'
}

func normalizePolicyError(name string, err error) error {
	line, col := 1, 1
	var failure *policyFailure
	if errors.As(err, &failure) {
		line, col = failure.line, failure.col
	}
	return fmt.Errorf("%w: %s:%d:%d", ErrTemplateInvalid, name, line, col)
}

// lexSource finds Jinja delimiters while respecting quoted strings inside a
// Jinja expression or statement. Token count is bounded by source bytes.
func lexSource(source string) ([]sourceToken, error) {
	tokens := make([]sourceToken, 0)
	line, col := 1, 1
	for i := 0; i < len(source); {
		if source[i] != '{' || i+1 >= len(source) {
			line, col = advanceLocation(source[i:i+1], line, col)
			i++
			continue
		}
		kind, close, ok := delimiterKind(source[i+1])
		if !ok {
			line, col = advanceLocation(source[i:i+1], line, col)
			i++
			continue
		}
		startLine, startCol := line, col
		end, found := findDelimiterEnd(source, i+2, close)
		if !found {
			return nil, invalidAt(startLine, startCol)
		}
		if len(tokens) == len(source) { // defensive: each token consumes bytes.
			return nil, invalidAt(startLine, startCol)
		}
		body := trimControlWhitespace(source[i+2 : end])
		tokens = append(tokens, sourceToken{kind: kind, body: body, line: startLine, col: startCol})
		line, col = advanceLocation(source[i:end+2], line, col)
		i = end + 2
	}
	return tokens, nil
}

func delimiterKind(second byte) (tokenKind, string, bool) {
	switch second {
	case '{':
		return tokenExpression, "}}", true
	case '%':
		return tokenStatement, "%}", true
	case '#':
		return tokenComment, "#}", true
	default:
		return 0, "", false
	}
}

func findDelimiterEnd(source string, from int, close string) (int, bool) {
	var quote byte
	for i := from; i < len(source); i++ {
		if quote != 0 {
			if source[i] == '\\' && i+1 < len(source) {
				i++
				continue
			}
			if source[i] == quote {
				quote = 0
			}
			continue
		}
		if source[i] == '\'' || source[i] == '"' {
			quote = source[i]
			continue
		}
		if strings.HasPrefix(source[i:], close) {
			return i, true
		}
	}
	return 0, false
}

func advanceLocation(text string, line, col int) (int, int) {
	for i := 0; i < len(text); i++ {
		if text[i] == '\n' {
			line, col = line+1, 1
		} else {
			col++
		}
	}
	return line, col
}

func trimControlWhitespace(body string) string {
	body = strings.TrimSpace(body)
	body = strings.TrimPrefix(body, "-")
	body = strings.TrimSuffix(body, "-")
	return strings.TrimSpace(body)
}

type sourcePolicy struct {
	bundleNames  map[string]struct{}
	macros       map[string]struct{}
	dependencies []string
}

func newSourcePolicy(bundleNames []string) *sourcePolicy {
	names := make(map[string]struct{}, len(bundleNames))
	for _, name := range bundleNames {
		names[name] = struct{}{}
	}
	return &sourcePolicy{bundleNames: names, macros: make(map[string]struct{})}
}

func (p *sourcePolicy) Accept(token sourceToken) error {
	if token.kind == tokenComment {
		return nil
	}
	if token.kind == tokenExpression {
		return p.validateExpression(token)
	}
	return p.validateStatement(token)
}

func (p *sourcePolicy) validateStatement(token sourceToken) error {
	keyword, rest := firstWord(token.body)
	switch keyword {
	case "if", "elif", "for", "set":
		if rest == "" {
			return invalidAt(token.line, token.col)
		}
		return p.validateExpressionAt(rest, token.line, token.col)
	case "else", "endif", "endfor", "endmacro":
		if strings.TrimSpace(rest) != "" {
			return invalidAt(token.line, token.col)
		}
		return nil
	case "macro":
		name, tail := firstWord(rest)
		if !isIdentifier(name) || !validMacroParameters(tail) {
			return invalidAt(token.line, token.col)
		}
		p.macros[name] = struct{}{}
		return nil
	case "include":
		dependency, ok := quotedInclude(rest)
		if !ok {
			return invalidAt(token.line, token.col)
		}
		if err := validateLogicalName(dependency); err != nil {
			return invalidAt(token.line, token.col)
		}
		if _, ok := p.bundleNames[dependency]; !ok {
			return invalidAt(token.line, token.col)
		}
		p.dependencies = append(p.dependencies, dependency)
		return nil
	default:
		return invalidAt(token.line, token.col)
	}
}

func (p *sourcePolicy) validateExpression(token sourceToken) error {
	return p.validateExpressionAt(token.body, token.line, token.col)
}

func (p *sourcePolicy) validateExpressionAt(expression string, line, col int) error {
	for i := 0; i < len(expression); {
		if expression[i] == '\'' || expression[i] == '"' {
			next, ok := skipQuoted(expression, i)
			if !ok {
				return invalidAt(line, col)
			}
			i = next
			continue
		}
		if !isIdentifierStart(expression[i]) {
			i++
			continue
		}
		start := i
		i++
		for i < len(expression) && isIdentifierContinue(expression[i]) {
			i++
		}
		word := expression[start:i]
		if word == "random" {
			return invalidAt(line, col)
		}
		next := skipSpaces(expression, i)
		if next < len(expression) && expression[next] == '(' {
			previous := previousNonSpace(expression, start)
			if previous == '.' {
				return invalidAt(line, col)
			}
			if previous == '|' {
				if !allowedFilter(word) {
					return invalidAt(line, col)
				}
				continue
			}
			if _, ok := p.macros[word]; !ok {
				return invalidAt(line, col)
			}
		}
		if previousNonSpace(expression, start) == '|' && !allowedFilter(word) {
			return invalidAt(line, col)
		}
	}
	return nil
}

func allowedFilter(name string) bool {
	switch name {
	case "lower", "upper", "trim", "length", "join", "default":
		return true
	default:
		return false
	}
}

func firstWord(text string) (string, string) {
	text = strings.TrimSpace(text)
	if text == "" {
		return "", ""
	}
	i := 0
	for i < len(text) && isIdentifierContinue(text[i]) {
		i++
	}
	return text[:i], strings.TrimSpace(text[i:])
}

func quotedInclude(rest string) (string, bool) {
	rest = strings.TrimSpace(rest)
	if len(rest) < 2 || (rest[0] != '\'' && rest[0] != '"') {
		return "", false
	}
	end, ok := skipQuoted(rest, 0)
	if !ok || strings.TrimSpace(rest[end:]) != "" {
		return "", false
	}
	return rest[1 : end-1], true
}

func validMacroParameters(tail string) bool {
	tail = strings.TrimSpace(tail)
	if len(tail) < 2 || tail[0] != '(' || tail[len(tail)-1] != ')' {
		return false
	}
	parameters := strings.TrimSpace(tail[1 : len(tail)-1])
	if parameters == "" {
		return true
	}
	for _, parameter := range strings.Split(parameters, ",") {
		if !isIdentifier(strings.TrimSpace(parameter)) {
			return false
		}
	}
	return true
}

func skipQuoted(text string, from int) (int, bool) {
	quote := text[from]
	for i := from + 1; i < len(text); i++ {
		if text[i] == '\\' && i+1 < len(text) {
			i++
			continue
		}
		if text[i] == quote {
			return i + 1, true
		}
	}
	return 0, false
}

func skipSpaces(text string, from int) int {
	for from < len(text) && (text[from] == ' ' || text[from] == '\t' || text[from] == '\n' || text[from] == '\r') {
		from++
	}
	return from
}

func previousNonSpace(text string, before int) byte {
	for i := before - 1; i >= 0; i-- {
		switch text[i] {
		case ' ', '\t', '\n', '\r':
			continue
		default:
			return text[i]
		}
	}
	return 0
}

func isIdentifier(name string) bool {
	if name == "" || !isIdentifierStart(name[0]) {
		return false
	}
	for i := 1; i < len(name); i++ {
		if !isIdentifierContinue(name[i]) {
			return false
		}
	}
	return true
}

func isIdentifierStart(b byte) bool    { return b == '_' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' }
func isIdentifierContinue(b byte) bool { return isIdentifierStart(b) || b >= '0' && b <= '9' }

type includeGraph struct{ edges map[string][]string }

func newIncludeGraph(capacity int) *includeGraph {
	return &includeGraph{edges: make(map[string][]string, capacity)}
}

func (g *includeGraph) Add(name string, dependencies []string) {
	g.edges[name] = append([]string(nil), dependencies...)
}

type graphFailure struct{ name string }

func (e *graphFailure) Error() string { return "invalid include graph" }

func (g *includeGraph) ValidateAcyclic(maxDepth int) error {
	state := make(map[string]uint8, len(g.edges))
	var walk func(string, int) error
	walk = func(name string, depth int) error {
		if depth > maxDepth || state[name] == 1 {
			return &graphFailure{name: name}
		}
		if state[name] == 2 {
			return nil
		}
		state[name] = 1
		for _, dependency := range g.edges[name] {
			if err := walk(dependency, depth+1); err != nil {
				return err
			}
		}
		state[name] = 2
		return nil
	}
	for _, name := range sortedGraphNames(g.edges) {
		if err := walk(name, 0); err != nil {
			return err
		}
	}
	return nil
}

func sortedGraphNames(edges map[string][]string) []string {
	names := make([]string, 0, len(edges))
	for name := range edges {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
