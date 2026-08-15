package profile

import (
	"context"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/prompttmpl"
)

// PublishRecord is the complete, already-validated publication input. Stores
// persist its exact immutable manifest and snapshot in one transaction.
type PublishRecord struct {
	TenantID           string
	ProfileID          ID
	ExpectedGeneration int64
	DraftGeneration    int64
	Manifest           Manifest
	Snapshot           Snapshot
	EngineVersion      string
	Actor              string
	Activate           bool
	PublishedAt        time.Time
}

// Validate ensures the record cannot persist a different manifest, snapshot,
// tenant, or actor than the exact publication it names.
func (record PublishRecord) Validate() error {
	if !validIdentifier(record.TenantID) || record.ProfileID.Validate() != nil || record.ExpectedGeneration < 0 || record.DraftGeneration < 0 || !validIdentifier(record.Actor) || prompttmpl.ValidateEngineIdentity(record.EngineVersion) != nil || record.Snapshot.TenantID != record.TenantID || record.Snapshot.Ref.ID != record.ProfileID || record.Snapshot.EngineVersion != record.EngineVersion || record.Manifest.ValidateSnapshot(record.Snapshot) != nil {
		return ErrInvalidProfile
	}
	return nil
}

// PublishedSummary is safe metadata for paging a tenant's published versions.
type PublishedSummary struct {
	Ref           VersionRef
	Active        bool
	RevokedAt     *time.Time
	PublishedAt   time.Time
	EngineVersion string
	SnapshotHash  string
}

// Store is the tenant-scoped persistence boundary for drafts, immutable blobs,
// and immutable published profile versions. Implementations must copy all
// mutable inputs before retaining them and return owned maps, slices, raw JSON,
// byte slices, and time pointers. Every lookup and mutation is scoped by the
// explicit tenant argument or record tenant; hashes never grant authority.
type Store interface {
	// PutBlob recomputes the canonical lowercase SHA-256 and byte count. An
	// exact tenant/hash/media/content retry is idempotent; any supplied hash or
	// count mismatch and any conflicting tuple fail closed.
	PutBlob(context.Context, string, Blob) error
	// LoadBlob returns an owned copy selected by the exact tenant and hash.
	LoadBlob(context.Context, string, string) (Blob, bool, error)
	// LoadDraft returns an owned copy selected by the exact tenant and profile.
	LoadDraft(context.Context, string, ID) (Draft, bool, error)
	// SaveDraft inserts generation one only when expectedGeneration is zero.
	// Existing drafts advance exactly once only when their current generation
	// equals expectedGeneration; every loser returns ErrStaleDraft.
	SaveDraft(context.Context, Draft, int64) (Draft, error)
	// SaveTemplate atomically persists canonical blob content, changes the
	// logical reference, and advances the exact expected draft generation.
	SaveTemplate(context.Context, string, ID, string, []byte, int64, string) (Draft, error)
	// DeleteTemplate atomically removes a logical reference and advances the
	// exact expected draft generation. Shared immutable blobs are not deleted.
	DeleteTemplate(context.Context, string, ID, string, int64, string) (Draft, error)
	// NextVersion returns the advisory tenant/profile MAX(version)+1. Publish
	// independently rechecks allocation while holding the exact draft lock.
	NextVersion(context.Context, string, ID) (uint64, error)
	// Publish locks and verifies the exact draft generation, rechecks the next
	// version, and atomically stores immutable manifest/snapshot content and any
	// active-version swap. A draft generation can be published only once.
	Publish(context.Context, PublishRecord) (Published, error)
	// Resolve returns an owned immutable version selected by exact tenant,
	// profile, and version. Missing or cross-tenant rows are not observable.
	Resolve(context.Context, string, ID, uint64) (Published, bool, error)
	// ListPublished returns deterministic tenant-scoped keyset pages. Page size
	// and opaque cursor are bounded; an invalid cursor fails closed.
	ListPublished(context.Context, string, int, string) ([]PublishedSummary, string, error)
	// Activate atomically swaps the tenant/profile active version without
	// changing immutable publication content and rejects revoked versions.
	Activate(context.Context, string, ID, uint64, string) error
	// Revoke atomically records bounded audit metadata and makes the exact
	// version inactive. Only an exact revocation retry is idempotent.
	Revoke(context.Context, string, ID, uint64, string, time.Time) error
}
