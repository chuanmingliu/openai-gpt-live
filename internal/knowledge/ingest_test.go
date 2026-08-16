package knowledge_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/knowledge"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ingestor() *knowledge.Ingestor { return knowledge.NewIngestor() }

func document(name string, body string) knowledge.Document {
	return knowledge.Document{
		TenantID: "tenant-a",
		PackID:   "policy",
		Filename: name,
		Content:  []byte(body),
		Actor:    "operator",
		At:       time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC),
	}
}

func TestOnlyTextAndMarkdownAreAccepted(t *testing.T) {
	require.NoError(t, func() error {
		_, err := ingestor().Validate(t.Context(), document("policy.md", "# Coverage\n\nwaiting period is thirty days"))
		return err
	}())

	for _, name := range []string{"policy.pdf", "policy.docx", "policy.zip", "policy.rtf", "policy", "policy.md.exe"} {
		t.Run(name, func(t *testing.T) {
			_, err := ingestor().Validate(t.Context(), document(name, "waiting period is thirty days"))
			assert.ErrorIs(t, err, knowledge.ErrUnsupportedContent)
		})
	}
}

// A truthful extension must never be sufficient on its own.
func TestFormatGateRejectsBinaryContentBeforeAnyDecode(t *testing.T) {
	for name, body := range map[string]string{
		"pdf magic":       "%PDF-1.7\n%\xE2\xE3\xCF\xD3",
		"zip magic":       "PK\x03\x04rest of a docx",
		"nul byte":        "coverage\x00hidden",
		"invalid utf8":    "coverage \xff\xfe body",
		"control byte":    "coverage\x07body",
		"utf16 bom":       "\xff\xfewaiting period",
		"elf magic":       "\x7fELF\x02\x01\x01",
		"escape sequence": "coverage\x1b[31mred",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ingestor().Validate(t.Context(), document("notes.md", body))
			assert.ErrorIs(t, err, knowledge.ErrUnsupportedContent)
		})
	}
}

func TestUploadAndNormalizedTextBoundsAreExact(t *testing.T) {
	// Two independent ceilings: raw upload bytes, and normalized text. Text
	// with no redundant whitespace normalizes to its own size, so an upload at
	// the raw ceiling still fails the normalized one.
	atNormalizedLimit := strings.Repeat("a", knowledge.MaxNormalizedBytes)
	_, err := ingestor().Validate(t.Context(), document("policy.txt", atNormalizedLimit))
	require.NoError(t, err)

	_, err = ingestor().Validate(t.Context(), document("policy.txt", atNormalizedLimit+"a"))
	assert.ErrorIs(t, err, knowledge.ErrContentLimit)

	_, err = ingestor().Validate(t.Context(), document("policy.txt", strings.Repeat("b", knowledge.MaxUploadBytes+1)))
	assert.ErrorIs(t, err, knowledge.ErrContentLimit)

	_, err = ingestor().Validate(t.Context(), document("policy.txt", ""))
	assert.ErrorIs(t, err, knowledge.ErrUnsupportedContent)

	_, err = ingestor().Validate(t.Context(), document("policy.txt", "   \n\n  \t "))
	assert.ErrorIs(t, err, knowledge.ErrUnsupportedContent, "whitespace alone carries no material")
}

func TestIdentityAndActorAreValidatedBeforeContent(t *testing.T) {
	hostile := document("policy.md", "waiting period")
	hostile.PackID = "../escape"
	_, err := ingestor().Validate(t.Context(), hostile)
	assert.ErrorIs(t, err, knowledge.ErrInvalidKnowledge)

	spaced := document("policy.md", "waiting period")
	spaced.TenantID = "tenant a"
	_, err = ingestor().Validate(t.Context(), spaced)
	assert.ErrorIs(t, err, knowledge.ErrInvalidKnowledge)

	anonymous := document("policy.md", "waiting period")
	anonymous.Actor = ""
	_, err = ingestor().Validate(t.Context(), anonymous)
	assert.ErrorIs(t, err, knowledge.ErrInvalidKnowledge)
}

func TestNormalizationIsDeterministicAndPreservesSemanticCharacters(t *testing.T) {
	body := "# Coverage\r\n\r\nWaiting  period is thirty days.\r\n\r\n## Exclusions\r\n\r\nPre-existing conditions—excluded."
	first, err := ingestor().Preview(t.Context(), document("policy.md", body))
	require.NoError(t, err)
	second, err := ingestor().Preview(t.Context(), document("policy.md", body))
	require.NoError(t, err)
	assert.Equal(t, first.Hash, second.Hash, "the same bytes must always preview to the same identity")

	joined := strings.Join(excerptTexts(first.Excerpts), "\n")
	assert.NotContains(t, joined, "\r", "CRLF is normalized away")
	assert.NotContains(t, joined, " ", "no-break space is normalized to a plain space")
	assert.Contains(t, joined, "—", "semantic punctuation survives normalization")
	assert.NotContains(t, joined, "Waiting  period", "runs of spaces collapse")
}

func TestMarkdownHeadingsBecomeCitationSections(t *testing.T) {
	body := "# Policy\n\nintro text\n\n## Coverage\n\nwaiting period is thirty days\n\n### Waiting period\n\nthirty days exactly"
	preview, err := ingestor().Preview(t.Context(), document("policy.md", body))
	require.NoError(t, err)
	require.NotEmpty(t, preview.Excerpts)

	sections := map[string]bool{}
	for _, excerpt := range preview.Excerpts {
		sections[excerpt.Citation.Section] = true
		assert.Equal(t, "policy.md", excerpt.Citation.SourceTitle)
		assert.Zero(t, excerpt.Citation.Page, "text input has no pagination")
	}
	assert.True(t, sections["Coverage"] || sections["Waiting period"], "headings label their following content")

	// Plain text has no heading syntax, so sections stay empty rather than
	// inventing structure that the source never expressed.
	plain, err := ingestor().Preview(t.Context(), document("policy.txt", "# not a heading\n\njust text"))
	require.NoError(t, err)
	for _, excerpt := range plain.Excerpts {
		assert.Empty(t, excerpt.Citation.Section)
	}
}

func TestChunkingRespectsExcerptBoundsAndKeepsDenseOrdinals(t *testing.T) {
	paragraph := strings.Repeat("waiting period is thirty days. ", 400)
	preview, err := ingestor().Preview(t.Context(), document("policy.txt", paragraph))
	require.NoError(t, err)
	require.Greater(t, len(preview.Excerpts), 1, "material larger than one excerpt must split")

	for index, excerpt := range preview.Excerpts {
		assert.Equal(t, index, excerpt.Ordinal)
		assert.LessOrEqual(t, len(excerpt.Text), knowledge.MaxExcerptBytes)
		assert.NotEmpty(t, excerpt.Tokens)
		assert.Equal(t, knowledge.ExcerptHash(excerpt.Text), excerpt.Citation.ExcerptHash)
		require.NoError(t, excerpt.Validate())
	}
}

func TestCJKMaterialTokenizesAndNeverSplitsARune(t *testing.T) {
	body := strings.Repeat("保险等待期为三十天。除外责任包括既往症。", 300)
	preview, err := ingestor().Preview(t.Context(), document("policy.txt", body))
	require.NoError(t, err)
	require.NotEmpty(t, preview.Excerpts)

	for _, excerpt := range preview.Excerpts {
		assert.True(t, utf8ValidString(excerpt.Text), "a chunk boundary must never split a rune")
		assert.NotEmpty(t, excerpt.Tokens)
	}
	assert.Contains(t, strings.Join(preview.Excerpts[0].Tokens, " "), "保险")
}

func TestExcerptCountCeilingIsEnforced(t *testing.T) {
	// Each paragraph becomes at least one excerpt.
	body := strings.Repeat("paragraph body\n\n", knowledge.MaxExcerpts+10)
	_, err := ingestor().Preview(t.Context(), document("policy.txt", body))
	assert.ErrorIs(t, err, knowledge.ErrContentLimit)
}

func TestPublishUsesThePreviewedHashAndDiscardsUploadBytes(t *testing.T) {
	source := document("policy.md", "# Coverage\n\nwaiting period is thirty days")
	preview, err := ingestor().Preview(t.Context(), source)
	require.NoError(t, err)

	record, err := ingestor().PublishRecord(t.Context(), source, preview.Hash)
	require.NoError(t, err)
	assert.Equal(t, preview.Hash, record.Hash)
	assert.Equal(t, "tenant-a", record.TenantID)
	assert.Equal(t, "policy", record.PackID)
	assert.Equal(t, "policy.md", record.Title)
	assert.Equal(t, "text/markdown", record.MediaType)
	assert.Equal(t, "operator", record.PublishedBy)
	assert.Equal(t, preview.Excerpts, record.Excerpts)

	_, err = ingestor().PublishRecord(t.Context(), source, strings.Repeat("a", 64))
	assert.ErrorIs(t, err, knowledge.ErrInvalidKnowledge,
		"publishing content the operator never previewed is refused")
}

func TestIngestionHonorsCancellation(t *testing.T) {
	ctx, cancel := contextWithCancel(t)
	cancel()
	_, err := ingestor().Preview(ctx, document("policy.txt", strings.Repeat("body ", 10_000)))
	assert.Error(t, err)
}

func TestPreviewNeverEchoesTheFilenameIntoAnError(t *testing.T) {
	_, err := ingestor().Validate(t.Context(), document("secret-customer-name.pdf", "body"))
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "secret-customer-name")
}

func excerptTexts(excerpts []knowledge.Excerpt) []string {
	texts := make([]string, 0, len(excerpts))
	for _, excerpt := range excerpts {
		texts = append(texts, excerpt.Text)
	}
	return texts
}

func utf8ValidString(value string) bool { return utf8.ValidString(value) }

func contextWithCancel(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithCancel(t.Context())
}

// FuzzIngestNeverPanics drives the whole ingestion path with arbitrary bytes
// and filenames. Any input must produce a value or a stable error class, never
// a panic and never an excerpt that violates its own invariants.
func FuzzIngestNeverPanics(f *testing.F) {
	f.Add("policy.md", "# Coverage\n\nwaiting period")
	f.Add("policy.txt", "保险等待期为三十天。")
	f.Add("policy.pdf", "%PDF-1.7")
	f.Add("notes.md", "\x00\x01\x02")
	f.Add("", "")
	f.Add("policy.md", strings.Repeat("#", 4096))
	f.Add("policy.txt", strings.Repeat("a\n\n", 1024))

	f.Fuzz(func(t *testing.T, name string, body string) {
		if len(body) > knowledge.MaxUploadBytes {
			t.Skip()
		}
		preview, err := ingestor().Preview(t.Context(), document(name, body))
		if err != nil {
			assert.True(t,
				errorsIsAny(err, knowledge.ErrUnsupportedContent, knowledge.ErrContentLimit, knowledge.ErrInvalidKnowledge),
				"unexpected error class: %v", err)
			return
		}
		require.LessOrEqual(t, len(preview.Excerpts), knowledge.MaxExcerpts)
		for index, excerpt := range preview.Excerpts {
			assert.Equal(t, index, excerpt.Ordinal)
			require.NoError(t, excerpt.Validate())
		}
		// Preview is a pure function of its input.
		repeat, err := ingestor().Preview(t.Context(), document(name, body))
		require.NoError(t, err)
		assert.Equal(t, preview.Hash, repeat.Hash)
	})
}

func errorsIsAny(err error, candidates ...error) bool {
	for _, candidate := range candidates {
		if errors.Is(err, candidate) {
			return true
		}
	}
	return false
}
