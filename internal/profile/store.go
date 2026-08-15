package profile

import (
	"context"
	"time"
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
	if !validIdentifier(record.TenantID) || record.ProfileID.Validate() != nil || record.ExpectedGeneration < 0 || record.DraftGeneration < 0 || !validIdentifier(record.Actor) || !validShortText(record.EngineVersion) || record.Snapshot.TenantID != record.TenantID || record.Snapshot.Ref.ID != record.ProfileID || record.Snapshot.EngineVersion != record.EngineVersion || record.Manifest.ValidateSnapshot(record.Snapshot) != nil {
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
// and immutable published profile versions.
type Store interface {
	PutBlob(context.Context, string, Blob) error
	LoadBlob(context.Context, string, string) (Blob, bool, error)
	LoadDraft(context.Context, string, ID) (Draft, bool, error)
	SaveDraft(context.Context, Draft, int64) (Draft, error)
	SaveTemplate(context.Context, string, ID, string, []byte, int64, string) (Draft, error)
	DeleteTemplate(context.Context, string, ID, string, int64, string) (Draft, error)
	Publish(context.Context, PublishRecord) (Published, error)
	Resolve(context.Context, string, ID, uint64) (Published, bool, error)
	ListPublished(context.Context, string, int, string) ([]PublishedSummary, string, error)
	Activate(context.Context, string, ID, uint64, string) error
	Revoke(context.Context, string, ID, uint64, string, time.Time) error
}
