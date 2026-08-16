package knowledge

import (
	"context"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	// MaxUploadBytes bounds one uploaded document before any decode.
	MaxUploadBytes = 8 << 20
	// MaxNormalizedBytes bounds the normalized text of one document.
	MaxNormalizedBytes = 4 << 20

	// excerptOverlapBytes repeats the tail of one excerpt at the head of the
	// next so a sentence spanning a chunk boundary is still findable.
	excerptOverlapBytes = 256
	maxHeadingBytes     = 256
	maxFilenameBytes    = 512
)

// contentClass is one accepted input format. Adding a format means registering
// a class here; every unregistered input is rejected without a parse attempt.
type contentClass struct {
	extension string
	mediaType string
	// headings reports whether this class expresses section structure that may
	// be lifted into citations.
	headings bool
}

var acceptedClasses = []contentClass{
	{extension: ".md", mediaType: "text/markdown", headings: true},
	{extension: ".markdown", mediaType: "text/markdown", headings: true},
	{extension: ".txt", mediaType: "text/plain", headings: false},
	{extension: ".text", mediaType: "text/plain", headings: false},
}

// Document is one upload awaiting ingestion. Content is the exact uploaded
// bytes; nothing here is persisted, and the bytes are discarded once excerpts
// are derived.
type Document struct {
	TenantID string
	PackID   string
	Filename string
	Content  []byte
	Actor    string
	At       time.Time
}

// Preview is the operator-visible result of ingestion: the exact excerpts and
// the content hash that publication will require.
type Preview struct {
	Hash      string
	Title     string
	MediaType string
	Excerpts  []Excerpt
}

// Ingestor turns accepted documents into bounded, citable excerpts. It holds no
// state, performs no I/O, and never retains upload bytes.
type Ingestor struct{}

func NewIngestor() *Ingestor { return &Ingestor{} }

// Validate reports whether a document is acceptable without building excerpts.
// It is the cheap pre-flight the upload endpoint runs before doing any work.
func (ingestor *Ingestor) Validate(ctx context.Context, document Document) (string, error) {
	if err := contextErr(ctx); err != nil {
		return "", err
	}
	if err := validateDocumentIdentity(document); err != nil {
		return "", err
	}
	class, err := classify(document)
	if err != nil {
		return "", err
	}
	if _, err := normalize(string(document.Content)); err != nil {
		return "", err
	}
	return class.mediaType, nil
}

// Preview normalizes, chunks, and hashes a document. The returned hash is what
// PublishRecord will demand, so an operator can only publish what they saw.
func (ingestor *Ingestor) Preview(ctx context.Context, document Document) (Preview, error) {
	if err := contextErr(ctx); err != nil {
		return Preview{}, err
	}
	if err := validateDocumentIdentity(document); err != nil {
		return Preview{}, err
	}
	class, err := classify(document)
	if err != nil {
		return Preview{}, err
	}
	text, err := normalize(string(document.Content))
	if err != nil {
		return Preview{}, err
	}
	if err := contextErr(ctx); err != nil {
		return Preview{}, err
	}
	excerpts, err := chunk(ctx, text, document.Filename, class.headings)
	if err != nil {
		return Preview{}, err
	}

	candidate := Version{
		TenantID:  document.TenantID,
		Ref:       VersionRef{PackID: document.PackID, Version: 1},
		Title:     document.Filename,
		MediaType: class.mediaType,
		Excerpts:  excerpts,
	}
	candidate.Ref.Hash = ContentHash(candidate)
	if err := candidate.Validate(); err != nil {
		return Preview{}, err
	}
	return Preview{
		Hash:      candidate.Ref.Hash,
		Title:     document.Filename,
		MediaType: class.mediaType,
		Excerpts:  excerpts,
	}, nil
}

// PublishRecord rebuilds the exact previewed content and refuses unless it
// hashes to previewedHash. Re-deriving rather than trusting a caller's excerpts
// means the bytes that reach the store are the bytes that were reviewed.
func (ingestor *Ingestor) PublishRecord(ctx context.Context, document Document, previewedHash string) (PublishRecord, error) {
	preview, err := ingestor.Preview(ctx, document)
	if err != nil {
		return PublishRecord{}, err
	}
	if !validHash(previewedHash) || preview.Hash != previewedHash {
		return PublishRecord{}, ErrInvalidKnowledge
	}
	return PublishRecord{
		TenantID:    document.TenantID,
		PackID:      document.PackID,
		Title:       preview.Title,
		MediaType:   preview.MediaType,
		Excerpts:    preview.Excerpts,
		Hash:        preview.Hash,
		PublishedAt: document.At,
		PublishedBy: document.Actor,
	}, nil
}

func validateDocumentIdentity(document Document) error {
	if !validIdentifier(document.TenantID) || !validIdentifier(document.PackID) || !validIdentifier(document.Actor) {
		return ErrInvalidKnowledge
	}
	if strings.Contains(document.PackID, "/") || strings.Contains(document.PackID, ".") {
		return ErrInvalidKnowledge
	}
	if !validText(document.Filename, maxFilenameBytes, false) || strings.ContainsAny(document.Filename, "/\\") {
		return ErrInvalidKnowledge
	}
	return nil
}

// classify is the closed format gate. Extension and content must both agree
// that this is text; a truthful extension alone is never sufficient, so a PDF
// renamed to .md is rejected before any decode.
func classify(document Document) (contentClass, error) {
	if len(document.Content) > MaxUploadBytes {
		return contentClass{}, ErrContentLimit
	}
	if len(document.Content) == 0 {
		return contentClass{}, ErrUnsupportedContent
	}
	lowered := strings.ToLower(document.Filename)
	var matched contentClass
	found := false
	for _, class := range acceptedClasses {
		if strings.HasSuffix(lowered, class.extension) {
			matched = class
			found = true
			break
		}
	}
	if !found {
		return contentClass{}, ErrUnsupportedContent
	}
	if !isPlainUTF8Text(document.Content) {
		return contentClass{}, ErrUnsupportedContent
	}
	return matched, nil
}

// isPlainUTF8Text accepts only valid UTF-8 whose control characters are limited
// to tab, newline, and carriage return. Every binary container this deployment
// might be handed - PDF, ZIP/DOCX, ELF, UTF-16 - fails on either the encoding
// check or a control byte, so no format-specific sniffing is needed.
func isPlainUTF8Text(content []byte) bool {
	if !utf8.Valid(content) {
		return false
	}
	for _, character := range string(content) {
		if character == '\t' || character == '\n' || character == '\r' {
			continue
		}
		if unicode.IsControl(character) || character == 0xFEFF {
			return false
		}
	}
	return true
}

// normalize collapses line endings and horizontal whitespace runs without
// touching semantic characters, so the same source bytes always produce the
// same excerpts and the same hash.
func normalize(raw string) (string, error) {
	if len(raw) > MaxUploadBytes {
		return "", ErrContentLimit
	}
	var builder strings.Builder
	builder.Grow(len(raw))
	pendingSpace := false
	newlines := 0
	wroteAny := false

	flushNewlines := func() {
		if newlines == 0 {
			return
		}
		if wroteAny {
			if newlines > 1 {
				builder.WriteString("\n\n")
			} else {
				builder.WriteString("\n")
			}
		}
		newlines = 0
		pendingSpace = false
	}

	for _, character := range raw {
		switch {
		case character == '\r':
			continue
		case character == '\n':
			newlines++
			pendingSpace = false
		case unicode.IsSpace(character):
			// Every horizontal space - including no-break and ideographic
			// space - normalizes to one plain space.
			if wroteAny || newlines > 0 {
				pendingSpace = true
			}
		default:
			flushNewlines()
			if pendingSpace && wroteAny {
				builder.WriteRune(' ')
			}
			pendingSpace = false
			builder.WriteRune(character)
			wroteAny = true
		}
	}
	normalized := builder.String()
	if len(normalized) > MaxNormalizedBytes {
		return "", ErrContentLimit
	}
	if strings.TrimSpace(normalized) == "" {
		return "", ErrUnsupportedContent
	}
	return normalized, nil
}

// chunk splits normalized text into bounded excerpts at paragraph boundaries,
// carrying a fixed overlap so material spanning a boundary stays findable.
func chunk(ctx context.Context, text, title string, headings bool) ([]Excerpt, error) {
	excerpts := make([]Excerpt, 0, 16)
	section := ""
	ordinal := 0

	for _, paragraph := range strings.Split(text, "\n\n") {
		if err := contextErr(ctx); err != nil {
			return nil, err
		}
		paragraph = strings.TrimSpace(paragraph)
		if paragraph == "" {
			continue
		}
		if headings {
			if heading, ok := markdownHeading(paragraph); ok {
				section = heading
				continue
			}
		}
		for _, body := range splitBounded(paragraph) {
			if ordinal >= MaxExcerpts {
				return nil, ErrContentLimit
			}
			excerpts = append(excerpts, Excerpt{
				Ordinal: ordinal,
				Text:    body,
				Tokens:  Tokenize(body),
				Citation: Citation{
					SourceTitle: title,
					Section:     section,
					ExcerptHash: ExcerptHash(body),
				},
			})
			ordinal++
		}
	}
	if len(excerpts) == 0 {
		return nil, ErrUnsupportedContent
	}
	return excerpts, nil
}

// markdownHeading reports an ATX heading's text. Only a heading that is its own
// paragraph labels the content that follows.
func markdownHeading(paragraph string) (string, bool) {
	if !strings.HasPrefix(paragraph, "#") || strings.Contains(paragraph, "\n") {
		return "", false
	}
	trimmed := strings.TrimLeft(paragraph, "#")
	if trimmed == paragraph || (trimmed != "" && !strings.HasPrefix(trimmed, " ")) {
		return "", false
	}
	heading := strings.TrimSpace(trimmed)
	if heading == "" || len(heading) > maxHeadingBytes {
		return "", false
	}
	return heading, true
}

// splitBounded cuts one paragraph into MaxExcerptBytes pieces on rune
// boundaries, preferring to break after a sentence terminator or space.
func splitBounded(paragraph string) []string {
	if len(paragraph) <= MaxExcerptBytes {
		return []string{paragraph}
	}
	pieces := make([]string, 0, len(paragraph)/MaxExcerptBytes+1)
	remaining := paragraph
	for len(remaining) > MaxExcerptBytes {
		cut := safeCut(remaining, MaxExcerptBytes)
		pieces = append(pieces, strings.TrimSpace(remaining[:cut]))
		overlapStart := cut - excerptOverlapBytes
		if overlapStart < 0 {
			overlapStart = 0
		}
		overlapStart = alignRune(remaining, overlapStart)
		remaining = strings.TrimSpace(remaining[overlapStart:])
		if overlapStart == 0 {
			// No forward progress is possible with overlap; drop it rather
			// than loop forever on pathological input.
			remaining = strings.TrimSpace(paragraph[cut:])
		}
	}
	if trimmed := strings.TrimSpace(remaining); trimmed != "" {
		pieces = append(pieces, trimmed)
	}
	return pieces
}

// safeCut finds the largest rune-aligned index at or before limit, preferring a
// sentence or word boundary within the last eighth of the window.
func safeCut(value string, limit int) int {
	if limit >= len(value) {
		return len(value)
	}
	aligned := alignRune(value, limit)
	window := aligned - limit/8
	if window < 1 {
		window = 1
	}
	for index := aligned; index > window; index-- {
		if index >= len(value) {
			continue
		}
		switch value[index-1] {
		case ' ', '\n', '.', '!', '?', ';', ':':
			return index
		}
		// CJK sentence terminators are multi-byte; check the rune before this
		// index only when it is a rune start.
		if utf8.RuneStart(value[index]) {
			previous, size := utf8.DecodeLastRuneInString(value[:index])
			if size > 1 && (previous == '。' || previous == '！' || previous == '？' || previous == '；') {
				return index
			}
		}
	}
	return aligned
}

// alignRune moves index back to the nearest rune boundary so no cut ever splits
// a multi-byte character.
func alignRune(value string, index int) int {
	if index >= len(value) {
		return len(value)
	}
	for index > 0 && !utf8.RuneStart(value[index]) {
		index--
	}
	return index
}

func contextErr(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidKnowledge
	}
	return ctx.Err()
}
