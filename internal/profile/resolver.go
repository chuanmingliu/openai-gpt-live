package profile

import (
	"context"
	"math"

	"github.com/chuanmingliu/openai-gpt-live/internal/prompttmpl"
)

// ResolvePurpose determines which immutable-version lifecycle rules apply.
type ResolvePurpose string

const (
	ResolveNewSession   ResolvePurpose = "new_session"
	ResolveReconnect    ResolvePurpose = "reconnect"
	ResolveAcceptedWork ResolvePurpose = "accepted_work"
)

// Resolver resolves one exact immutable snapshot and its owned source bundle.
type Resolver interface {
	Resolve(context.Context, string, VersionRef, ResolvePurpose) (Resolved, error)
}

type exactResolver struct {
	store  Store
	engine prompttmpl.Engine
}

// NewResolver constructs a resolver. A nil Store is valid only for builtin/1;
// every resolution still requires a matching template engine.
func NewResolver(store Store, engine prompttmpl.Engine) Resolver {
	return &exactResolver{store: store, engine: engine}
}

func (resolver *exactResolver) Resolve(ctx context.Context, tenantID string, ref VersionRef, purpose ResolvePurpose) (Resolved, error) {
	if err := validateProfileBoundaryContext(ctx); err != nil {
		return Resolved{}, err
	}
	if !validIdentifier(tenantID) || ref.Validate() != nil || !validResolvePurpose(purpose) {
		return Resolved{}, ErrInvalidProfile
	}
	if resolver == nil || isNilProfileDependency(resolver.engine) {
		return Resolved{}, ErrProfileIncompatible
	}
	if ref.ID == builtinProfileID {
		if ref.Version != builtinVersion {
			return Resolved{}, ErrProfileIncompatible
		}
		return resolver.resolveBuiltin(ctx)
	}
	if ref.Version > uint64(math.MaxInt64) {
		return Resolved{}, ErrInvalidProfile
	}
	if isNilProfileDependency(resolver.store) {
		return Resolved{}, ErrProfileIncompatible
	}
	return resolver.resolveCustom(ctx, tenantID, ref, purpose)
}

func (resolver *exactResolver) resolveBuiltin(ctx context.Context) (Resolved, error) {
	resolved := Builtin()
	engineVersion := resolver.engine.Version()
	if err := contextBoundaryError(ctx); err != nil {
		return Resolved{}, err
	}
	if prompttmpl.ValidateEngineIdentity(engineVersion) != nil || engineVersion != resolved.Snapshot.EngineVersion {
		return Resolved{}, ErrProfileIncompatible
	}
	compiled, err := resolver.engine.Compile(ctx, cloneRenderBundle(resolved.Bundle), prompttmpl.DefaultLimits())
	if contextErr := contextBoundaryError(ctx); contextErr != nil {
		return Resolved{}, contextErr
	}
	if err != nil {
		return Resolved{}, normalizeCompileBoundaryError(ctx, err, resolved.Bundle.Templates)
	}
	if isNilProfileDependency(compiled) {
		return Resolved{}, ErrProfileIncompatible
	}
	compiledVersion := compiled.EngineVersion()
	if err := contextBoundaryError(ctx); err != nil {
		return Resolved{}, err
	}
	engineVersionAfter := resolver.engine.Version()
	if err := contextBoundaryError(ctx); err != nil {
		return Resolved{}, err
	}
	if compiledVersion != engineVersion || engineVersionAfter != engineVersion {
		return Resolved{}, ErrProfileIncompatible
	}
	if resolved.Snapshot.Validate() != nil {
		return Resolved{}, ErrInvalidProfile
	}
	return resolved.Clone(), nil
}

func (resolver *exactResolver) resolveCustom(ctx context.Context, tenantID string, ref VersionRef, purpose ResolvePurpose) (Resolved, error) {
	engineVersion := resolver.engine.Version()
	if err := contextBoundaryError(ctx); err != nil {
		return Resolved{}, err
	}
	if prompttmpl.ValidateEngineIdentity(engineVersion) != nil {
		return Resolved{}, ErrProfileIncompatible
	}
	published, ok, err := resolver.store.Resolve(ctx, tenantID, ref.ID, ref.Version)
	if contextErr := contextBoundaryError(ctx); contextErr != nil {
		return Resolved{}, contextErr
	}
	if err != nil {
		return Resolved{}, normalizeStoreBoundaryError(ctx, err)
	}
	if !ok {
		return Resolved{}, ErrProfileIncompatible
	}
	published = published.Clone()
	if published.TenantID != tenantID || published.Ref != ref || published.Snapshot.TenantID != tenantID || published.Snapshot.Ref != ref || published.Validate() != nil || !validPublishedLifecycle(published) {
		return Resolved{}, ErrInvalidProfile
	}
	if purpose != ResolveAcceptedWork {
		if published.RevokedAt != nil {
			return Resolved{}, ErrProfileRevoked
		}
		if !published.Active {
			return Resolved{}, ErrProfileIncompatible
		}
	}
	if published.EngineVersion != engineVersion || published.Snapshot.EngineVersion != engineVersion {
		return Resolved{}, ErrProfileIncompatible
	}
	if _, err := published.Manifest.Hash(); err != nil {
		return Resolved{}, ErrInvalidProfile
	}
	bundle, _, err := loadManifestBundle(ctx, resolver.store, tenantID, published.Manifest)
	if err != nil {
		return Resolved{}, err
	}
	compiled, err := resolver.engine.Compile(ctx, cloneRenderBundle(bundle), prompttmpl.DefaultLimits())
	if contextErr := contextBoundaryError(ctx); contextErr != nil {
		return Resolved{}, contextErr
	}
	if err != nil {
		return Resolved{}, normalizeCompileBoundaryError(ctx, err, bundle.Templates)
	}
	if isNilProfileDependency(compiled) {
		return Resolved{}, ErrProfileIncompatible
	}
	compiledVersion := compiled.EngineVersion()
	if err := contextBoundaryError(ctx); err != nil {
		return Resolved{}, err
	}
	engineVersionAfter := resolver.engine.Version()
	if err := contextBoundaryError(ctx); err != nil {
		return Resolved{}, err
	}
	if compiledVersion != engineVersion || engineVersionAfter != engineVersion {
		return Resolved{}, ErrProfileIncompatible
	}
	if published.Snapshot.Validate() != nil || published.Manifest.ValidateSnapshot(published.Snapshot) != nil {
		return Resolved{}, ErrInvalidProfile
	}
	return Resolved{Snapshot: published.Snapshot.Clone(), Bundle: cloneRenderBundle(bundle)}.Clone(), nil
}

func validResolvePurpose(purpose ResolvePurpose) bool {
	return purpose == ResolveNewSession || purpose == ResolveReconnect || purpose == ResolveAcceptedWork
}

func validPublishedLifecycle(published Published) bool {
	if published.DraftGeneration <= 0 || published.PublishedAt.IsZero() || published.PublishedAt.Year() < 1 || published.PublishedAt.Year() > 9999 || published.Active && published.RevokedAt != nil {
		return false
	}
	if published.RevokedAt != nil {
		if published.RevokedAt.IsZero() || published.RevokedAt.Year() < 1 || published.RevokedAt.Year() > 9999 || published.RevokedAt.Before(published.PublishedAt) {
			return false
		}
	}
	return true
}
