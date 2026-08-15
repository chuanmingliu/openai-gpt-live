package profile

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/prompttmpl"
	"github.com/stretchr/testify/require"
)

func publishedResolverFixture(t *testing.T) (*publisherStoreFake, *publisherEngine, Published) {
	t.Helper()
	manifest, blobs := publisherManifest(t)
	engine := &publisherEngine{delegate: prompttmpl.NewMiniJinja()}
	bundleHash, err := manifest.Bundle.Hash()
	require.NoError(t, err)
	snapshot := Snapshot{
		TenantID:               testTenant,
		Ref:                    VersionRef{ID: testProfile, Version: 4},
		EngineVersion:          engine.delegate.Version(),
		BundleHash:             bundleHash,
		System:                 manifest.Bundle.Templates["system.j2"],
		Opening:                manifest.Bundle.Templates["opening_message.j2"],
		Turn:                   manifest.Bundle.Templates["turn_instruction.j2"],
		Recipes:                cloneRecipes(manifest.Recipes),
		Bindings:               cloneBindings(manifest.Bindings),
		Knowledge:              append([]KnowledgeRef(nil), manifest.Knowledge...),
		AdapterRegistryVersion: "catalog-v1",
	}
	snapshotHash, err := snapshot.Hash()
	require.NoError(t, err)
	snapshot.SnapshotHash = snapshotHash
	published := Published{
		TenantID: testTenant, Ref: snapshot.Ref, DraftGeneration: 3,
		EngineVersion: snapshot.EngineVersion, Snapshot: snapshot, Manifest: manifest,
		Active: true, PublishedAt: testPublicationTime, PublishedBy: testActor,
	}
	require.NoError(t, published.Validate())
	store := &publisherStoreFake{blobs: blobs, resolved: published, resolveOK: true}
	return store, engine, published
}

func TestResolvePurposeMatrixEnforcesActiveRevokedAndAcceptedWorkRules(t *testing.T) {
	tests := []struct {
		name      string
		purpose   ResolvePurpose
		active    bool
		revoked   bool
		wantError error
	}{
		{"active new session", ResolveNewSession, true, false, nil},
		{"active reconnect", ResolveReconnect, true, false, nil},
		{"inactive new session", ResolveNewSession, false, false, ErrProfileIncompatible},
		{"inactive reconnect", ResolveReconnect, false, false, ErrProfileIncompatible},
		{"revoked new session", ResolveNewSession, false, true, ErrProfileRevoked},
		{"revoked reconnect", ResolveReconnect, false, true, ErrProfileRevoked},
		{"inactive accepted work", ResolveAcceptedWork, false, false, nil},
		{"revoked accepted work", ResolveAcceptedWork, false, true, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, engine, published := publishedResolverFixture(t)
			published.Active = tc.active
			if tc.revoked {
				revoked := testPublicationTime.Add(time.Minute)
				published.RevokedAt = &revoked
			}
			store.resolved = published
			resolved, err := NewResolver(store, engine).Resolve(t.Context(), testTenant, published.Ref, tc.purpose)
			if tc.wantError != nil {
				require.ErrorIs(t, err, tc.wantError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, published.Snapshot, resolved.Snapshot)
			require.Equal(t, "Hello {{ customer }}", resolved.Bundle.Templates["system.j2"])
			require.Equal(t, 1, engine.calls())
		})
	}
}

func TestResolveCustomRequiresConfiguredStoreAndStableMatchingEngine(t *testing.T) {
	store, engine, published := publishedResolverFixture(t)
	_, err := NewResolver(nil, engine).Resolve(t.Context(), testTenant, published.Ref, ResolveNewSession)
	require.ErrorIs(t, err, ErrProfileIncompatible)

	var typedNilStore *publisherStoreFake
	_, err = NewResolver(typedNilStore, engine).Resolve(t.Context(), testTenant, published.Ref, ResolveNewSession)
	require.ErrorIs(t, err, ErrProfileIncompatible)

	_, err = NewResolver(store, nil).Resolve(t.Context(), testTenant, published.Ref, ResolveNewSession)
	require.ErrorIs(t, err, ErrProfileIncompatible)

	engine.versions = []string{engine.delegate.Version(), engine.delegate.Version() + "/changed"}
	_, err = NewResolver(store, engine).Resolve(t.Context(), testTenant, published.Ref, ResolveNewSession)
	require.ErrorIs(t, err, ErrProfileIncompatible)

	store, engine, published = publishedResolverFixture(t)
	engine.compiledVersion = "different-engine"
	_, err = NewResolver(store, engine).Resolve(t.Context(), testTenant, published.Ref, ResolveNewSession)
	require.ErrorIs(t, err, ErrProfileIncompatible)
}

func TestResolveRejectsNoncompliantEngineIdentityBeforeStoreAccess(t *testing.T) {
	store, engine, published := publishedResolverFixture(t)
	engine.versions = []string{"minijinja-go/v2.24.0"}
	_, err := NewResolver(store, engine).Resolve(t.Context(), testTenant, published.Ref, ResolveNewSession)
	require.ErrorIs(t, err, ErrProfileIncompatible)
	require.Zero(t, store.resolveCalls)
}

func TestResolveRejectsVersionAboveStoreDomainBeforeStoreAccess(t *testing.T) {
	store, engine, _ := publishedResolverFixture(t)
	ref := VersionRef{ID: testProfile, Version: uint64(math.MaxInt64) + 1}
	_, err := NewResolver(store, engine).Resolve(t.Context(), testTenant, ref, ResolveNewSession)
	require.ErrorIs(t, err, ErrInvalidProfile)
	require.Zero(t, store.resolveCalls)
}

func TestResolveClassifiesRevocationBeforeCurrentEngineCompatibility(t *testing.T) {
	for _, purpose := range []ResolvePurpose{ResolveNewSession, ResolveReconnect, ResolveAcceptedWork} {
		t.Run(string(purpose), func(t *testing.T) {
			store, engine, published := publishedResolverFixture(t)
			published.Active = false
			revoked := testPublicationTime.Add(time.Minute)
			published.RevokedAt = &revoked
			published.EngineVersion = "jinja-go-v1/retired/v1"
			published.Snapshot.EngineVersion = published.EngineVersion
			snapshotHash, err := published.Snapshot.Hash()
			require.NoError(t, err)
			published.Snapshot.SnapshotHash = snapshotHash
			require.NoError(t, published.Validate())
			store.resolved = published

			_, err = NewResolver(store, engine).Resolve(t.Context(), testTenant, published.Ref, purpose)
			if purpose == ResolveAcceptedWork {
				require.ErrorIs(t, err, ErrProfileIncompatible)
				return
			}
			require.ErrorIs(t, err, ErrProfileRevoked)
		})
	}
}

func TestResolveRejectsWrongTenantReferenceVersionEngineAndHashes(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Published)
	}{
		{"tenant", func(p *Published) { p.TenantID = "published-tenant" }},
		{"snapshot tenant", func(p *Published) { p.Snapshot.TenantID = "published-tenant" }},
		{"profile id", func(p *Published) { p.Ref.ID = "other" }},
		{"snapshot ref", func(p *Published) { p.Snapshot.Ref.Version++ }},
		{"engine", func(p *Published) { p.EngineVersion = "other-engine" }},
		{"snapshot engine", func(p *Published) { p.Snapshot.EngineVersion = "other-engine" }},
		{"bundle hash", func(p *Published) { p.Snapshot.BundleHash = strings.Repeat("a", 64) }},
		{"snapshot hash", func(p *Published) { p.Snapshot.SnapshotHash = strings.Repeat("a", 64) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, engine, published := publishedResolverFixture(t)
			tc.mutate(&published)
			store.resolved = published
			_, err := NewResolver(store, engine).Resolve(t.Context(), testTenant, VersionRef{ID: testProfile, Version: 4}, ResolveNewSession)
			require.Error(t, err)
		})
	}
}

func TestResolveLoadsAndVerifiesEveryTemplateAndFixtureBlob(t *testing.T) {
	t.Run("all exact blobs", func(t *testing.T) {
		store, engine, published := publishedResolverFixture(t)
		resolved, err := NewResolver(store, engine).Resolve(t.Context(), testTenant, published.Ref, ResolveNewSession)
		require.NoError(t, err)
		require.Equal(t, len(published.Manifest.Bundle.Templates)+len(published.Manifest.Bundle.Fixtures), store.loadBlobCalls)
		require.NotEmpty(t, resolved.Bundle.Templates)
	})

	for _, tc := range []struct {
		name   string
		mutate func(*publisherStoreFake, Published)
	}{
		{"missing template", func(store *publisherStoreFake, published Published) {
			delete(store.blobs, published.Manifest.Bundle.Templates["system.j2"].Hash)
		}},
		{"missing fixture", func(store *publisherStoreFake, published Published) {
			delete(store.blobs, published.Manifest.Bundle.Fixtures["basic.json"].Hash)
		}},
		{"corrupt content", func(store *publisherStoreFake, published Published) {
			ref := published.Manifest.Bundle.Fixtures["basic.json"]
			blob := store.blobs[ref.Hash]
			blob.Content = append([]byte(nil), blob.Content...)
			blob.Content[0] = 'x'
			store.blobs[ref.Hash] = blob
		}},
		{"corrupt media", func(store *publisherStoreFake, published Published) {
			ref := published.Manifest.Bundle.Templates["system.j2"]
			blob := store.blobs[ref.Hash]
			blob.MediaType = "application/octet-stream"
			store.blobs[ref.Hash] = blob
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, engine, published := publishedResolverFixture(t)
			tc.mutate(store, published)
			_, err := NewResolver(store, engine).Resolve(t.Context(), testTenant, published.Ref, ResolveNewSession)
			require.ErrorIs(t, err, ErrInvalidProfile)
		})
	}
}

func TestResolveCompilesSourcesBeforeUseAndNormalizesErrors(t *testing.T) {
	store, engine, published := publishedResolverFixture(t)
	ref := published.Manifest.Bundle.Templates["system.j2"]
	unsafe := canonicalPublisherBlob(t, ref.MediaType, []byte("{{ forbidden_call() }}"))
	delete(store.blobs, ref.Hash)
	store.blobs[unsafe.Hash] = unsafe
	published.Manifest.Bundle.Templates["system.j2"] = refForBlob(unsafe)
	bundleHash, err := published.Manifest.Bundle.Hash()
	require.NoError(t, err)
	published.Snapshot.BundleHash = bundleHash
	published.Snapshot.System = refForBlob(unsafe)
	hash, err := published.Snapshot.Hash()
	require.NoError(t, err)
	published.Snapshot.SnapshotHash = hash
	store.resolved = published

	_, err = NewResolver(store, engine).Resolve(t.Context(), testTenant, published.Ref, ResolveNewSession)
	require.ErrorIs(t, err, prompttmpl.ErrTemplateInvalid)
	require.NotContains(t, err.Error(), "forbidden_call")
}

func TestResolveReturnsDeepOwnedSnapshotBundleAndSchema(t *testing.T) {
	store, engine, published := publishedResolverFixture(t)
	resolver := NewResolver(store, engine)
	first, err := resolver.Resolve(t.Context(), testTenant, published.Ref, ResolveNewSession)
	require.NoError(t, err)
	first.Bundle.Templates["system.j2"] = "mutated"
	first.Bundle.Schema.Fields["customer"] = prompttmpl.Field{Type: prompttmpl.TypeBool}
	first.Snapshot.System.Hash = strings.Repeat("b", 64)
	first.Snapshot.Bindings = append(first.Snapshot.Bindings, AdapterBinding{AdapterID: "mutated"})

	second, err := resolver.Resolve(t.Context(), testTenant, published.Ref, ResolveNewSession)
	require.NoError(t, err)
	require.Equal(t, "Hello {{ customer }}", second.Bundle.Templates["system.j2"])
	require.Equal(t, prompttmpl.TypeString, second.Bundle.Schema.Fields["customer"].Type)
	require.Equal(t, published.Snapshot.System.Hash, second.Snapshot.System.Hash)
	require.Equal(t, published.Snapshot.Bindings, second.Snapshot.Bindings)
}

func TestResolveRejectsMissingVersionInvalidPurposeNilContextAndCancellation(t *testing.T) {
	store, engine, published := publishedResolverFixture(t)
	store.resolveOK = false
	_, err := NewResolver(store, engine).Resolve(t.Context(), testTenant, published.Ref, ResolveNewSession)
	require.ErrorIs(t, err, ErrProfileIncompatible)

	store.resolveOK = true
	_, err = NewResolver(store, engine).Resolve(t.Context(), testTenant, published.Ref, ResolvePurpose("unknown"))
	require.ErrorIs(t, err, ErrInvalidProfile)
	_, err = NewResolver(store, engine).Resolve(nil, testTenant, published.Ref, ResolveNewSession)
	require.ErrorIs(t, err, ErrInvalidProfile)
	_, err = NewResolver(store, engine).Resolve(t.Context(), "bad\ntenant", published.Ref, ResolveNewSession)
	require.ErrorIs(t, err, ErrInvalidProfile)
	_, err = NewResolver(store, engine).Resolve(t.Context(), testTenant, VersionRef{}, ResolveNewSession)
	require.ErrorIs(t, err, ErrInvalidProfile)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = NewResolver(store, engine).Resolve(ctx, testTenant, published.Ref, ResolveNewSession)
	require.ErrorIs(t, err, context.Canceled)
}

func TestResolveMapsStoreAndEngineErrorsWithoutRawContent(t *testing.T) {
	secret := "credential private-prompt raw-provider-output reasoning"
	for _, setup := range []func(*publisherStoreFake, *publisherEngine){
		func(store *publisherStoreFake, _ *publisherEngine) { store.resolveErr = errors.New(secret) },
		func(store *publisherStoreFake, _ *publisherEngine) { store.loadBlobErr = errors.New(secret) },
		func(_ *publisherStoreFake, engine *publisherEngine) { engine.compileErr = errors.New(secret) },
	} {
		store, engine, published := publishedResolverFixture(t)
		setup(store, engine)
		_, err := NewResolver(store, engine).Resolve(t.Context(), testTenant, published.Ref, ResolveNewSession)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "private-prompt")
		require.NotContains(t, err.Error(), "raw-provider-output")
	}
}

func TestResolveStopsWhenEngineCancelsContextButReturnsCompiledBundle(t *testing.T) {
	store, engine, published := publishedResolverFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	engine.afterCompile = cancel
	_, err := NewResolver(store, engine).Resolve(ctx, testTenant, published.Ref, ResolveNewSession)
	require.ErrorIs(t, err, context.Canceled)
}

func TestResolvePrefersCancellationAfterMissingStoreResult(t *testing.T) {
	store, engine, published := publishedResolverFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	store.resolveOK = false
	store.resolveHook = cancel
	_, err := NewResolver(store, engine).Resolve(ctx, testTenant, published.Ref, ResolveNewSession)
	require.ErrorIs(t, err, context.Canceled)
}

func TestResolveFencesEngineIdentityReturns(t *testing.T) {
	t.Run("initial identity", func(t *testing.T) {
		store, engine, published := publishedResolverFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		engine.versions = []string{"implementation-only"}
		engine.versionHook = func(call int) {
			if call == 1 {
				cancel()
			}
		}
		_, err := NewResolver(store, engine).Resolve(ctx, testTenant, published.Ref, ResolveNewSession)
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, store.resolveCalls)
	})

	t.Run("post-compile identity", func(t *testing.T) {
		store, engine, published := publishedResolverFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		engine.versionHook = func(call int) {
			if call == 2 {
				cancel()
			}
		}
		_, err := NewResolver(store, engine).Resolve(ctx, testTenant, published.Ref, ResolveNewSession)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestResolveAcceptedWorkStillRejectsCorruptRevokedSnapshot(t *testing.T) {
	store, engine, published := publishedResolverFixture(t)
	revoked := testPublicationTime.Add(time.Minute)
	published.Active = false
	published.RevokedAt = &revoked
	published.Snapshot.SnapshotHash = strings.Repeat("f", 64)
	store.resolved = published
	_, err := NewResolver(store, engine).Resolve(t.Context(), testTenant, published.Ref, ResolveAcceptedWork)
	require.ErrorIs(t, err, ErrInvalidProfile)
}
