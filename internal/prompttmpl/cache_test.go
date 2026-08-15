package prompttmpl

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type countingEngine struct {
	mu       sync.Mutex
	calls    int
	block    <-chan struct{}
	compiled int
}

func (e *countingEngine) Version() string { return "counting-v1" }

func (e *countingEngine) Compile(ctx context.Context, bundle Bundle, _ Limits) (CompiledBundle, error) {
	e.mu.Lock()
	e.calls++
	e.compiled++
	version := e.compiled
	block := e.block
	e.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return fakeCompiled{hash: fmt.Sprintf("%d", version)}, nil
}

func (e *countingEngine) Calls() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}

type fakeCompiled struct{ hash string }

func (f fakeCompiled) Render(context.Context, string, Context) (string, error) { return f.hash, nil }
func (f fakeCompiled) Hash() string                                            { return f.hash }
func (f fakeCompiled) EngineVersion() string                                   { return "fake-v1" }
func (f fakeCompiled) ResidentBytes() int                                      { return 64 }

func cacheBundle(name, source string) Bundle {
	return Bundle{Templates: map[string]string{name: source}, Schema: ContextSchema{Fields: map[string]Field{"customer": {Type: TypeString}}}}
}

func TestCacheReturnsPreviouslyCompiledBundle(t *testing.T) {
	engine := &countingEngine{}
	cache, err := NewCache(engine, 2, 4096)
	require.NoError(t, err)
	first, err := cache.Compile(t.Context(), cacheBundle("system.j2", "hello"), DefaultLimits())
	require.NoError(t, err)
	second, err := cache.Compile(t.Context(), cacheBundle("system.j2", "hello"), DefaultLimits())
	require.NoError(t, err)
	assert.Equal(t, first.Hash(), second.Hash())
	assert.Equal(t, 1, engine.Calls())
}

func TestCacheEvictsLeastRecentlyUsedEntry(t *testing.T) {
	engine := &countingEngine{}
	cache, err := NewCache(engine, 2, 4096)
	require.NoError(t, err)
	_, err = cache.Compile(t.Context(), cacheBundle("a.j2", "a"), DefaultLimits())
	require.NoError(t, err)
	_, err = cache.Compile(t.Context(), cacheBundle("b.j2", "b"), DefaultLimits())
	require.NoError(t, err)
	_, err = cache.Compile(t.Context(), cacheBundle("a.j2", "a"), DefaultLimits())
	require.NoError(t, err)
	_, err = cache.Compile(t.Context(), cacheBundle("c.j2", "c"), DefaultLimits())
	require.NoError(t, err)
	_, err = cache.Compile(t.Context(), cacheBundle("b.j2", "b"), DefaultLimits())
	require.NoError(t, err)
	assert.Equal(t, 4, engine.Calls())
}

func TestCacheEvictsWhenByteBudgetCannotFit(t *testing.T) {
	engine := &countingEngine{}
	cache, err := NewCache(engine, 10, 80)
	require.NoError(t, err)
	_, err = cache.Compile(t.Context(), cacheBundle("a.j2", "aaaaaaaaaaaaaaaaaaaa"), DefaultLimits())
	require.NoError(t, err)
	_, err = cache.Compile(t.Context(), cacheBundle("b.j2", "bbbbbbbbbbbbbbbbbbbb"), DefaultLimits())
	require.NoError(t, err)
	_, err = cache.Compile(t.Context(), cacheBundle("a.j2", "aaaaaaaaaaaaaaaaaaaa"), DefaultLimits())
	require.NoError(t, err)
	assert.Equal(t, 3, engine.Calls())
}

func TestCacheDeduplicatesConcurrentMisses(t *testing.T) {
	gate := make(chan struct{})
	engine := &countingEngine{block: gate}
	cache, err := NewCache(engine, 2, 4096)
	require.NoError(t, err)
	const callers = 16
	results := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func() {
			_, err := cache.Compile(context.Background(), cacheBundle("system.j2", "hello"), DefaultLimits())
			results <- err
		}()
	}
	require.Eventually(t, func() bool { return engine.Calls() == 1 }, time.Second, 5*time.Millisecond)
	close(gate)
	for i := 0; i < callers; i++ {
		require.NoError(t, <-results)
	}
	assert.Equal(t, 1, engine.Calls())
}

func TestCacheDoesNotCacheCompileFailures(t *testing.T) {
	engine := &failingEngine{}
	cache, err := NewCache(engine, 2, 4096)
	require.NoError(t, err)
	_, err = cache.Compile(t.Context(), cacheBundle("system.j2", "hello"), DefaultLimits())
	require.Error(t, err)
	_, err = cache.Compile(t.Context(), cacheBundle("system.j2", "hello"), DefaultLimits())
	require.Error(t, err)
	assert.Equal(t, 2, engine.calls)
}

type failingEngine struct{ calls int }

func (e *failingEngine) Version() string { return "failing-v1" }

func (e *failingEngine) Compile(context.Context, Bundle, Limits) (CompiledBundle, error) {
	e.calls++
	return nil, ErrTemplateInvalid
}

type sizedCompiled struct {
	hash, version string
	resident      int
}

func (f sizedCompiled) Render(context.Context, string, Context) (string, error) { return f.hash, nil }
func (f sizedCompiled) Hash() string                                            { return f.hash }
func (f sizedCompiled) EngineVersion() string                                   { return f.version }
func (f sizedCompiled) ResidentBytes() int                                      { return f.resident }

type versionedEngine struct {
	calls    int
	version  string
	resident int
	seen     Bundle
}

func (e *versionedEngine) Version() string { return e.version }
func (e *versionedEngine) Compile(_ context.Context, bundle Bundle, _ Limits) (CompiledBundle, error) {
	e.calls++
	e.seen = bundle
	return sizedCompiled{hash: fmt.Sprintf("%s-%d", e.version, e.calls), version: e.version, resident: e.resident}, nil
}

func TestCacheSeparatesSchemaEncodingAndEngineVersion(t *testing.T) {
	engine := &versionedEngine{version: "engine-v1", resident: 64}
	cache, err := NewCache(engine, 8, 4096)
	require.NoError(t, err)
	first := cacheBundle("system.j2", "safe")
	first.Schema.Fields["customer"] = Field{Type: TypeString, Sensitivity: "a:b", Source: "c"}
	second := cacheBundle("system.j2", "safe")
	second.Schema.Fields["customer"] = Field{Type: TypeString, Sensitivity: "a", Source: "b:c"}
	_, err = cache.Compile(t.Context(), first, DefaultLimits())
	require.NoError(t, err)
	_, err = cache.Compile(t.Context(), second, DefaultLimits())
	require.NoError(t, err)
	assert.Equal(t, 2, engine.calls)
	engine.version = "engine-v2"
	_, err = cache.Compile(t.Context(), second, DefaultLimits())
	require.NoError(t, err)
	assert.Equal(t, 3, engine.calls)
}

func TestCacheDoesNotAdmitOversizeCompiledBundle(t *testing.T) {
	engine := &versionedEngine{version: "engine-v1", resident: 1 << 20}
	cache, err := NewCache(engine, 8, 1024)
	require.NoError(t, err)
	bundle := cacheBundle("system.j2", "safe")
	_, err = cache.Compile(t.Context(), bundle, DefaultLimits())
	require.NoError(t, err)
	_, err = cache.Compile(t.Context(), bundle, DefaultLimits())
	require.NoError(t, err)
	assert.Equal(t, 2, engine.calls)
}

func TestCacheEvictsWhenCompiledResidentBytesExceedBudget(t *testing.T) {
	engine := &versionedEngine{version: "engine-v1", resident: 1024}
	cache, err := NewCache(engine, 8, 1800)
	require.NoError(t, err)
	_, err = cache.Compile(t.Context(), cacheBundle("a.j2", "safe"), DefaultLimits())
	require.NoError(t, err)
	_, err = cache.Compile(t.Context(), cacheBundle("b.j2", "safe"), DefaultLimits())
	require.NoError(t, err)
	_, err = cache.Compile(t.Context(), cacheBundle("a.j2", "safe"), DefaultLimits())
	require.NoError(t, err)
	assert.Equal(t, 3, engine.calls)
}

func TestCacheClonesBundleBeforeAdmission(t *testing.T) {
	engine := &versionedEngine{version: "engine-v1", resident: 64}
	cache, err := NewCache(engine, 8, 4096)
	require.NoError(t, err)
	bundle := cacheBundle("system.j2", "safe")
	bundle.Schema.Fields["customer"] = Field{Type: TypeString, Templates: []string{"system.j2"}}
	lookup := cloneBundle(bundle)
	_, err = cache.Compile(t.Context(), bundle, DefaultLimits())
	require.NoError(t, err)
	bundle.Templates["system.j2"] = "mutated"
	field := bundle.Schema.Fields["customer"]
	field.Templates[0] = "mutated.j2"
	bundle.Schema.Fields["customer"] = Field{Type: TypeBool, Templates: []string{"other.j2"}}
	assert.Equal(t, "safe", engine.seen.Templates["system.j2"])
	assert.Equal(t, TypeString, engine.seen.Schema.Fields["customer"].Type)
	assert.Equal(t, []string{"system.j2"}, engine.seen.Schema.Fields["customer"].Templates)
	_, err = cache.Compile(t.Context(), lookup, DefaultLimits())
	require.NoError(t, err)
	assert.Equal(t, 1, engine.calls)
}

type bundleDerivedCompiled struct {
	bundle Bundle
	limits Limits
}

func (f bundleDerivedCompiled) Render(_ context.Context, name string, _ Context) (string, error) {
	field := f.bundle.Schema.Fields["customer"]
	return f.bundle.Templates[name] + "|" + strings.Join(field.Templates, ","), nil
}
func (f bundleDerivedCompiled) Hash() string          { return bundleFingerprint(f.bundle, f.limits) }
func (f bundleDerivedCompiled) EngineVersion() string { return "bundle-derived-v1" }
func (f bundleDerivedCompiled) ResidentBytes() int    { return 64 }

type bundleDerivedEngine struct{ calls int }

func (e *bundleDerivedEngine) Version() string { return "bundle-derived-v1" }
func (e *bundleDerivedEngine) Compile(_ context.Context, bundle Bundle, limits Limits) (CompiledBundle, error) {
	e.calls++
	return bundleDerivedCompiled{bundle: bundle, limits: limits}, nil
}

func TestCacheAdmissionKeepsBundleDerivedResultsImmutable(t *testing.T) {
	engine := &bundleDerivedEngine{}
	cache, err := NewCache(engine, 8, 4096)
	require.NoError(t, err)
	bundle := cacheBundle("system.j2", "safe")
	bundle.Schema.Fields["customer"] = Field{Type: TypeString, Templates: []string{"system.j2"}}
	lookup := cloneBundle(bundle)
	compiled, err := cache.Compile(t.Context(), bundle, DefaultLimits())
	require.NoError(t, err)
	before, err := compiled.Render(t.Context(), "system.j2", Context{})
	require.NoError(t, err)
	beforeHash := compiled.Hash()
	bundle.Templates["system.j2"] = "mutated"
	field := bundle.Schema.Fields["customer"]
	field.Templates[0] = "mutated.j2"
	bundle.Schema.Fields["customer"] = Field{Type: TypeBool, Templates: []string{"other.j2"}}
	after, err := compiled.Render(t.Context(), "system.j2", Context{})
	require.NoError(t, err)
	assert.Equal(t, before, after)
	assert.Equal(t, beforeHash, compiled.Hash())
	cached, err := cache.Compile(t.Context(), lookup, DefaultLimits())
	require.NoError(t, err)
	cachedResult, err := cached.Render(t.Context(), "system.j2", Context{})
	require.NoError(t, err)
	assert.Equal(t, before, cachedResult)
	assert.Equal(t, beforeHash, cached.Hash())
	assert.Equal(t, 1, engine.calls)
}

func TestNewCacheRejectsInvalidLimits(t *testing.T) {
	for _, tc := range []struct{ entries, bytes int }{{0, 1}, {1, 0}, {-1, 1}, {1, -1}} {
		t.Run(fmt.Sprintf("%d-%d", tc.entries, tc.bytes), func(t *testing.T) {
			_, err := NewCache(&countingEngine{}, tc.entries, tc.bytes)
			require.ErrorIs(t, err, ErrLimitsInvalid)
		})
	}
}

func TestCacheVersionDelegatesToConfiguredEngine(t *testing.T) {
	engine := &versionedEngine{version: "engine-v1", resident: 64}
	cache, err := NewCache(engine, 2, 4096)
	require.NoError(t, err)
	require.Equal(t, "engine-v1", cache.Version())

	engine.version = "engine-v2"
	require.Equal(t, "engine-v2", cache.Version(), "the cache must not freeze a stale engine identity")

	var nilCache *Cache
	require.Empty(t, nilCache.Version())
}
