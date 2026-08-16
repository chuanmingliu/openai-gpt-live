package main

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/profile"
	"github.com/chuanmingliu/openai-gpt-live/internal/prompttmpl"
	"github.com/chuanmingliu/openai-gpt-live/internal/store/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

// defaultSystemTemplate seeds every new profile draft so the stored manifest is
// valid from generation one and publishes without further template work. It has
// no trailing newline so the seeded fixture expectation is engine-exact.
const defaultSystemTemplate = "You are a realtime spoken assistant. Reply in the user's language using at most three short sentences unless the user explicitly asks for more detail. Use natural speech without markdown, headings, lists, or narrated reasoning. Keep background work silent and mention it only after a result is available."

// demoAdapterCatalog is the fixed empty catalog for the loopback demo: no
// adapters are installed, so any adapter binding fails publication closed.
type demoAdapterCatalog struct{}

func (demoAdapterCatalog) Version() string { return "demo-empty-catalog-v1" }

func (demoAdapterCatalog) ValidateBindings(_ context.Context, _ string, bindings []profile.AdapterBinding) error {
	if len(bindings) != 0 {
		return errors.New("no adapters are installed in the loopback demo")
	}
	return nil
}

// profileAdminService owns the loopback profile administration dependencies.
// The browser never sees tenant, actor, database, or engine configuration.
type profileAdminService struct {
	tenantID  string
	actor     string
	store     profile.Store
	publisher *profile.Publisher
	closeFn   func()
}

func newProfileAdminService(cfg profileAdminConfig, store profile.Store, closeFn func()) (*profileAdminService, error) {
	if !cfg.Enabled || store == nil {
		return nil, errors.New("demo profile administration is unavailable")
	}
	engine := prompttmpl.NewMiniJinja()
	return &profileAdminService{
		tenantID:  cfg.TenantID,
		actor:     cfg.Actor,
		store:     store,
		publisher: profile.NewPublisher(store, engine, demoAdapterCatalog{}, nil),
		closeFn:   closeFn,
	}, nil
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
	service, err := newProfileAdminService(cfg, repository, pool.Close)
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
