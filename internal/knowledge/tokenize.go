package knowledge

import (
	"strings"
	"unicode"
)

// Tokenize builds the deterministic search index for one piece of text.
//
// Latin script splits on word boundaries and lowercases. CJK has no word
// delimiters, so each run of ideographic characters contributes both single
// runes and adjacent pairs (bigrams): "保险等待期" yields 保, 险, 待, 期 and
// 保险, 险等, 等待, 待期. Bigrams are what make a two-character term like 保险
// rank above an incidental single-character overlap.
//
// The result is order-preserving and duplicate-free, so the same text always
// produces the same token list on every platform.
func Tokenize(text string) []string {
	tokens := make([]string, 0, 32)
	seen := make(map[string]struct{}, 32)
	add := func(token string) {
		if token == "" || len(tokens) >= MaxTokensPerExcerpt {
			return
		}
		if _, duplicate := seen[token]; duplicate {
			return
		}
		seen[token] = struct{}{}
		tokens = append(tokens, token)
	}

	var latin strings.Builder
	var ideographs []rune

	flushLatin := func() {
		if latin.Len() > 0 {
			add(strings.ToLower(latin.String()))
			latin.Reset()
		}
	}
	flushIdeographs := func() {
		for index, character := range ideographs {
			add(string(character))
			if index+1 < len(ideographs) {
				add(string(ideographs[index : index+2]))
			}
		}
		ideographs = ideographs[:0]
	}

	for _, character := range text {
		switch {
		case isIdeographic(character):
			flushLatin()
			ideographs = append(ideographs, character)
		case unicode.IsLetter(character) || unicode.IsDigit(character):
			flushIdeographs()
			latin.WriteRune(character)
		default:
			flushLatin()
			flushIdeographs()
		}
	}
	flushLatin()
	flushIdeographs()
	return tokens
}

// isIdeographic reports whether a rune belongs to a script written without word
// delimiters, where n-gram indexing is the only sound lexical strategy.
func isIdeographic(character rune) bool {
	return unicode.Is(unicode.Han, character) ||
		unicode.Is(unicode.Hiragana, character) ||
		unicode.Is(unicode.Katakana, character) ||
		unicode.Is(unicode.Hangul, character)
}
