package knowledge

import (
	"context"

	"github.com/chuanmingliu/openai-gpt-live/internal/profile"
)

// maxProfileKnowledgeRefs mirrors the manifest's own ceiling. It is checked
// here before any store call so a hostile manifest cannot turn one publication
// into an unbounded number of database reads.
const maxProfileKnowledgeRefs = 32

// referenceValidator resolves every knowledge reference a profile pins, by
// exact tenant, pack, version, and hash.
type referenceValidator struct{ store Store }

// NewReferenceValidator adapts a knowledge store into the publisher's
// KnowledgeReferenceValidator seam. A profile may pin only material that
// already exists, still resolves to the exact reviewed bytes, and has not been
// tombstoned.
func NewReferenceValidator(store Store) profile.KnowledgeReferenceValidator {
	return referenceValidator{store: store}
}

func (validator referenceValidator) ValidateReferences(ctx context.Context, tenantID string, refs []profile.KnowledgeRef) error {
	if ctx == nil || validator.store == nil {
		return ErrStoreUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(refs) == 0 {
		return nil
	}
	if len(refs) > maxProfileKnowledgeRefs {
		return ErrContentLimit
	}
	if !validIdentifier(tenantID) {
		return ErrInvalidKnowledge
	}

	seen := make(map[VersionRef]struct{}, len(refs))
	for _, reference := range refs {
		ref := VersionRef{PackID: reference.ID, Version: reference.Version, Hash: reference.Hash}
		if err := ref.Validate(); err != nil {
			return err
		}
		// A manifest that pins the same version twice is malformed, not merely
		// redundant: it makes the pinned set ambiguous to audit.
		if _, duplicate := seen[ref]; duplicate {
			return ErrInvalidKnowledge
		}
		seen[ref] = struct{}{}

		version, ok, err := validator.store.Resolve(ctx, tenantID, ref)
		if err != nil {
			return err
		}
		if !ok {
			return ErrKnowledgeNotFound
		}
		if !version.Bindable() {
			return ErrKnowledgeTombstoned
		}
	}
	return nil
}
