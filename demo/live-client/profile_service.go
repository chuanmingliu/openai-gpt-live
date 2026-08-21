package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/adaptercatalog"
	knowledgeadapter "github.com/chuanmingliu/openai-gpt-live/internal/adapters/knowledge"
	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/knowledge"
	"github.com/chuanmingliu/openai-gpt-live/internal/profile"
	"github.com/chuanmingliu/openai-gpt-live/internal/prompttmpl"
	"github.com/chuanmingliu/openai-gpt-live/internal/store/postgres"
	"github.com/chuanmingliu/openai-gpt-live/internal/tools/websearch"
	"github.com/jackc/pgx/v5/pgxpool"
)

// defaultSystemTemplate seeds every new profile draft so the stored manifest is
// valid from generation one and publishes without further template work. It has
// no trailing newline so the seeded fixture expectation is engine-exact.
const defaultSystemTemplate = "You are a realtime spoken assistant. Reply in the user's language using at most three short sentences unless the user explicitly asks for more detail. Use natural speech without markdown, headings, lists, or narrated reasoning. Keep background work silent and mention it only after a result is available."

// profileAdminService owns the loopback profile administration dependencies.
// The browser never sees tenant, actor, database, or engine configuration.
type profileAdminService struct {
	tenantID  string
	actor     string
	store     profile.Store
	knowledge knowledge.Store
	catalog   *adaptercatalog.Catalog
	publisher *profile.Publisher
	closeFn   func()
}

// newProfileAdminService composes the publisher. The knowledge validator may be
// nil: the publisher then accepts only manifests that pin no knowledge, so a
// deployment without a knowledge store cannot publish a profile whose material
// it could never resolve.
func newProfileAdminService(cfg profileAdminConfig, store profile.Store, knowledgeStore knowledge.Store, closeFn func()) (*profileAdminService, error) {
	if !cfg.Enabled || store == nil {
		return nil, errors.New("demo profile administration is unavailable")
	}
	engine := prompttmpl.NewMiniJinja()
	catalog, err := newDemoAdapterCatalog(cfg.AdapterRegistryVersion)
	if err != nil {
		return nil, errors.New("demo adapter catalog is unavailable")
	}
	var validator profile.KnowledgeReferenceValidator
	if knowledgeStore != nil {
		validator = knowledge.NewReferenceValidator(knowledgeStore)
	}
	return &profileAdminService{
		tenantID:  cfg.TenantID,
		actor:     cfg.Actor,
		store:     store,
		knowledge: knowledgeStore,
		catalog:   catalog,
		publisher: profile.NewPublisher(store, engine, catalog, validator),
		closeFn:   closeFn,
	}, nil
}

func newDemoAdapterCatalog(registryVersion string) (*adaptercatalog.Catalog, error) {
	webExecutor, err := websearch.New(net.DefaultResolver)
	if err != nil {
		return nil, err
	}
	webTool := websearch.RegisteredTool(webExecutor)
	webManifest := adaptercatalog.Manifest{
		AdapterID: "web_search", AdapterVersion: "native-v1", RegistryVersion: registryVersion,
		Tool: webTool.Definition.Clone(), ReadOnly: true, ParallelSafe: true, StableIdempotency: true,
		Approval: adaptercatalog.ApprovalNone, Timeout: 8 * time.Second, MaxConcurrency: 4,
		MaxArgumentBytes: 64 << 10, MaxResultBytes: 256 << 10, DestinationID: "public_web",
		TransportClass: adaptercatalog.TransportNative, HealthClass: adaptercatalog.HealthRequired,
	}
	return adaptercatalog.New(registryVersion, []adaptercatalog.Manifest{knowledgeadapter.Manifest(registryVersion), webManifest}, nil)
}

// openProfileAdminService resolves the private database URL by environment
// name only and never returns, logs, or serializes the URL value.
func openProfileAdminService(ctx context.Context, cfg profileAdminConfig, lookup func(string) (string, bool)) (*profileAdminService, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	if lookup == nil {
		return nil, errors.New("demo profile database is unavailable")
	}
	databaseURL, ok := lookup(cfg.DatabaseURLEnv)
	if !ok || databaseURL == "" {
		return nil, errors.New("demo profile database is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, errors.New("demo profile database is unavailable")
	}
	repository, err := postgres.NewRepository(pool, clock.Real{})
	if err != nil {
		pool.Close()
		return nil, errors.New("demo profile database is unavailable")
	}
	service, err := newProfileAdminService(cfg, repository, repository.KnowledgeStore(), pool.Close)
	if err != nil {
		pool.Close()
		return nil, err
	}
	return service, nil
}

func (service *profileAdminService) close() {
	if service != nil && service.closeFn != nil {
		service.closeFn()
	}
}

func (service *profileAdminService) now() time.Time {
	return time.Now().UTC().Truncate(time.Microsecond)
}

// seedDraft persists the default system template and a matching synthetic
// fixture, then creates generation one. The stored manifest is valid and
// publishable immediately; the store rejects an already-existing draft.
func (service *profileAdminService) seedDraft(ctx context.Context, id profile.ID, description string) (profile.Draft, error) {
	systemBlob, err := (profile.Blob{MediaType: "text/plain; charset=utf-8", Content: []byte(defaultSystemTemplate)}).Canonical()
	if err != nil {
		return profile.Draft{}, profile.ErrInvalidProfile
	}
	fixtureContent, err := json.Marshal(map[string]any{
		"template": "system.j2",
		"context":  map[string]any{},
		"expected": defaultSystemTemplate,
	})
	if err != nil {
		return profile.Draft{}, profile.ErrInvalidProfile
	}
	fixtureBlob, err := (profile.Blob{MediaType: "application/json", Content: fixtureContent}).Canonical()
	if err != nil {
		return profile.Draft{}, profile.ErrInvalidProfile
	}
	if err := service.store.PutBlob(ctx, service.tenantID, systemBlob); err != nil {
		return profile.Draft{}, err
	}
	if err := service.store.PutBlob(ctx, service.tenantID, fixtureBlob); err != nil {
		return profile.Draft{}, err
	}
	draft := profile.Draft{
		TenantID:   service.tenantID,
		ID:         id,
		Generation: 1,
		Manifest: profile.Manifest{
			Description: description,
			Bundle: profile.PromptBundle{
				Templates: map[string]profile.TemplateRef{
					"system.j2": {Hash: systemBlob.Hash, MediaType: systemBlob.MediaType, Bytes: systemBlob.ContentBytes},
				},
				Fixtures: map[string]profile.TemplateRef{
					"default.json": {Hash: fixtureBlob.Hash, MediaType: fixtureBlob.MediaType, Bytes: fixtureBlob.ContentBytes},
				},
			},
		},
		UpdatedAt: service.now(),
		UpdatedBy: service.actor,
	}
	return service.store.SaveDraft(ctx, draft, 0)
}
