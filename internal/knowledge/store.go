package knowledge

import (
	"context"
	"time"
)

// Draft is unpublished, operator-visible material. A draft holds already
// normalized excerpts: normalization happens once, during ingestion, so the
// hash previewed to the operator is the hash that gets published.
type Draft struct {
	TenantID  string
	PackID    string
	Title     string
	MediaType string
	Excerpts  []Excerpt
	Hash      string
	CreatedAt time.Time
	CreatedBy string
}

func (draft Draft) Clone() Draft {
	clone := draft
	clone.Excerpts = make([]Excerpt, len(draft.Excerpts))
	for index, excerpt := range draft.Excerpts {
		clone.Excerpts[index] = excerpt.Clone()
	}
	return clone
}

// PublishRecord names the exact previewed content to publish. The version
// number is allocated by the store, never supplied by a caller, so two
// concurrent publications cannot claim the same number.
type PublishRecord struct {
	TenantID    string
	PackID      string
	Title       string
	MediaType   string
	Excerpts    []Excerpt
	Hash        string
	PublishedAt time.Time
	PublishedBy string
}

// SearchRequest is one bounded read against an exact set of pinned versions.
// Tenant and refs come from the caller's authority; a model may influence only
// the query text and the result count.
type SearchRequest struct {
	TenantID   string
	Refs       []VersionRef
	Query      string
	MaxResults int
}

// Store persists immutable knowledge versions for one deployment.
//
// Resolve reports (Version, false, nil) for a reference that does not exist for
// this tenant - absence is not an error, so a caller cannot distinguish "wrong
// tenant" from "no such pack" by error class alone.
type Store interface {
	CreateDraft(context.Context, Draft) (Draft, error)
	Publish(context.Context, PublishRecord) (Version, error)
	Resolve(context.Context, string, VersionRef) (Version, bool, error)
	List(context.Context, string, string, int) ([]Version, error)
	Tombstone(context.Context, string, VersionRef, string, time.Time) error
	Search(context.Context, SearchRequest) ([]Excerpt, error)
}
