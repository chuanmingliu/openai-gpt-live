// Package knowledge defines immutable, tenant-scoped reference material that a
// vertical profile may pin by exact version. A published version is content
// addressed: its hash covers the excerpt text, citations, and their order, so a
// pinned reference either resolves to exactly the bytes that were reviewed or
// does not resolve at all.
package knowledge

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	// MaxExcerptBytes bounds one excerpt's normalized UTF-8 text.
	MaxExcerptBytes = 4 << 10
	// MaxExcerpts bounds how many excerpts one version may carry.
	MaxExcerpts = 2048
	// MaxVersionBytes bounds the aggregate normalized text of one version.
	MaxVersionBytes = 8 << 20
	// MaxTokensPerExcerpt bounds the search index built for one excerpt.
	MaxTokensPerExcerpt = 1024

	maxIdentifierBytes = 256
	maxTitleBytes      = 1024
	maxSectionBytes    = 1024
	maxPage            = 1 << 20

	contentHashVersion = "knowledge-version-v1"
)

var (
	// ErrInvalidKnowledge marks a structurally invalid record: bad identity,
	// mismatched hash, or malformed ordering.
	ErrInvalidKnowledge = errors.New("knowledge_invalid")
	// ErrContentLimit marks a well-formed record that exceeds an exact bound.
	ErrContentLimit = errors.New("knowledge_content_limit")
	// ErrUnsupportedContent marks input this deployment will not ingest.
	ErrUnsupportedContent = errors.New("unsupported_content")
	// ErrKnowledgeNotFound marks an exact reference that does not resolve.
	ErrKnowledgeNotFound = errors.New("knowledge_not_found")
	// ErrKnowledgeTombstoned marks a version that may no longer be pinned.
	ErrKnowledgeTombstoned = errors.New("knowledge_tombstoned")
	// ErrStoreUnavailable marks a dependency failure, never a content fault.
	ErrStoreUnavailable = errors.New("knowledge_store_unavailable")
)

// VersionRef identifies one immutable published knowledge version. The hash is
// an integrity identifier, never an authority: resolution always carries the
// tenant separately.
type VersionRef struct {
	PackID  string `json:"pack_id"`
	Version uint64 `json:"version"`
	Hash    string `json:"hash"`
}

func (ref VersionRef) Validate() error {
	if !validIdentifier(ref.PackID) || ref.Version == 0 || !validHash(ref.Hash) {
		return ErrInvalidKnowledge
	}
	return nil
}

// Citation records where one excerpt came from. Page is zero for input that has
// no pagination, which is every format this deployment currently ingests.
type Citation struct {
	SourceTitle string `json:"source_title"`
	Section     string `json:"section,omitempty"`
	Page        int    `json:"page,omitempty"`
	ExcerptHash string `json:"excerpt_hash"`
}

// Excerpt is one bounded, citable unit of normalized text. Tokens are the
// derived search index; they are never accepted from outside this package.
type Excerpt struct {
	Ordinal  int      `json:"ordinal"`
	Text     string   `json:"text"`
	Tokens   []string `json:"tokens,omitempty"`
	Citation Citation `json:"citation"`
}

func (excerpt Excerpt) Validate() error {
	if len(excerpt.Text) > MaxExcerptBytes {
		return ErrContentLimit
	}
	if len(excerpt.Tokens) > MaxTokensPerExcerpt {
		return ErrContentLimit
	}
	if excerpt.Ordinal < 0 || excerpt.Ordinal >= MaxExcerpts {
		return ErrInvalidKnowledge
	}
	if !validExcerptText(excerpt.Text) {
		return ErrInvalidKnowledge
	}
	if err := excerpt.Citation.validate(); err != nil {
		return err
	}
	if excerpt.Citation.ExcerptHash != ExcerptHash(excerpt.Text) {
		return ErrInvalidKnowledge
	}
	for _, token := range excerpt.Tokens {
		if token == "" || !utf8.ValidString(token) {
			return ErrInvalidKnowledge
		}
	}
	return nil
}

func (excerpt Excerpt) Clone() Excerpt {
	excerpt.Tokens = append([]string(nil), excerpt.Tokens...)
	return excerpt
}

func (citation Citation) validate() error {
	if !validText(citation.SourceTitle, maxTitleBytes, false) {
		return ErrInvalidKnowledge
	}
	if !validText(citation.Section, maxSectionBytes, true) {
		return ErrInvalidKnowledge
	}
	if citation.Page < 0 || citation.Page > maxPage {
		return ErrInvalidKnowledge
	}
	if !validHash(citation.ExcerptHash) {
		return ErrInvalidKnowledge
	}
	return nil
}

// Version is one immutable published knowledge version and its excerpts.
type Version struct {
	TenantID     string     `json:"tenant_id"`
	Ref          VersionRef `json:"ref"`
	Title        string     `json:"title"`
	MediaType    string     `json:"media_type"`
	Excerpts     []Excerpt  `json:"excerpts"`
	PublishedAt  time.Time  `json:"published_at"`
	PublishedBy  string     `json:"published_by"`
	TombstonedAt *time.Time `json:"tombstoned_at,omitempty"`
	TombstonedBy string     `json:"tombstoned_by,omitempty"`
}

func (version Version) Clone() Version {
	clone := version
	clone.Excerpts = make([]Excerpt, len(version.Excerpts))
	for index, excerpt := range version.Excerpts {
		clone.Excerpts[index] = excerpt.Clone()
	}
	if version.TombstonedAt != nil {
		moment := *version.TombstonedAt
		clone.TombstonedAt = &moment
	}
	return clone
}

// Bindable reports whether a new profile publication may pin this version. A
// tombstoned version is never bindable.
func (version Version) Bindable() bool { return version.TombstonedAt == nil }

// Readable reports whether work that already pinned this version may keep
// reading it. Tombstoning removes a version from new selection; it does not
// retract material from a job that is already running against it.
func (version Version) Readable() bool { return true }

func (version Version) Validate() error {
	if len(version.Excerpts) > MaxExcerpts {
		return ErrContentLimit
	}
	if !validIdentifier(version.TenantID) || !validIdentifier(version.Ref.PackID) || version.Ref.Version == 0 {
		return ErrInvalidKnowledge
	}
	if !validText(version.Title, maxTitleBytes, false) || !validText(version.MediaType, maxIdentifierBytes, true) {
		return ErrInvalidKnowledge
	}
	if len(version.Excerpts) == 0 {
		return ErrInvalidKnowledge
	}
	aggregate := 0
	for index, excerpt := range version.Excerpts {
		if err := excerpt.Validate(); err != nil {
			return err
		}
		if excerpt.Ordinal != index {
			return ErrInvalidKnowledge
		}
		aggregate += len(excerpt.Text)
		if aggregate > MaxVersionBytes {
			return ErrContentLimit
		}
	}
	if version.Ref.Hash != ContentHash(version) {
		return ErrInvalidKnowledge
	}
	if !validText(version.PublishedBy, maxIdentifierBytes, true) || !validText(version.TombstonedBy, maxIdentifierBytes, true) {
		return ErrInvalidKnowledge
	}
	if (version.TombstonedAt == nil) != (version.TombstonedBy == "") {
		return ErrInvalidKnowledge
	}
	return nil
}

// ExcerptHash is the content identity of one excerpt's exact text.
func ExcerptHash(text string) string {
	digest := sha256.Sum256([]byte(text))
	return hex.EncodeToString(digest[:])
}

// ContentHash is the content identity of one version. It covers tenant, pack,
// version number, title, media type, and every excerpt's ordinal, text, and
// citation, with explicit length framing so no field boundary is ambiguous.
// Publication bookkeeping - who published or tombstoned it, and when - is
// deliberately excluded, so re-recording provenance cannot change identity.
func ContentHash(version Version) string {
	digest := sha256.New()
	writeFramed(digest, contentHashVersion)
	writeFramed(digest, version.TenantID)
	writeFramed(digest, version.Ref.PackID)
	writeUint(digest, version.Ref.Version)
	writeFramed(digest, version.Title)
	writeFramed(digest, version.MediaType)
	writeUint(digest, uint64(len(version.Excerpts)))
	for _, excerpt := range version.Excerpts {
		writeUint(digest, uint64(excerpt.Ordinal))
		writeFramed(digest, excerpt.Text)
		writeFramed(digest, excerpt.Citation.SourceTitle)
		writeFramed(digest, excerpt.Citation.Section)
		writeUint(digest, uint64(excerpt.Citation.Page))
		writeFramed(digest, excerpt.Citation.ExcerptHash)
	}
	return hex.EncodeToString(digest.Sum(nil))
}

func writeFramed(digest interface{ Write([]byte) (int, error) }, value string) {
	writeUint(digest, uint64(len(value)))
	_, _ = digest.Write([]byte(value))
}

func writeUint(digest interface{ Write([]byte) (int, error) }, value uint64) {
	var buffer [8]byte
	binary.BigEndian.PutUint64(buffer[:], value)
	_, _ = digest.Write(buffer[:])
}

func validHash(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, character := range []byte(value) {
		if !(character >= '0' && character <= '9' || character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

// validIdentifier is stricter than validText: an identity that reaches a
// database key or a citation reference carries no whitespace at all.
func validIdentifier(value string) bool {
	if !validText(value, maxIdentifierBytes, false) {
		return false
	}
	for _, character := range value {
		if unicode.IsSpace(character) {
			return false
		}
	}
	return true
}

func validText(value string, maximum int, allowEmpty bool) bool {
	if (!allowEmpty && value == "") || len(value) > maximum || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) || unicode.IsSpace(character) && character != ' ' {
			return false
		}
	}
	return true
}

// validExcerptText is deliberately more permissive than validText: excerpt
// bodies keep their paragraph structure, so tab and newline are legal while
// every other control character stays rejected.
func validExcerptText(value string) bool {
	if value == "" || len(value) > MaxExcerptBytes || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if character == '\t' || character == '\n' {
			continue
		}
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}
