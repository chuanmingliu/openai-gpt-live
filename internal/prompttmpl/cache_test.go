package prompttmpl

import (
	"context"
	"fmt"
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

func (e *failingEngine) Compile(context.Context, Bundle, Limits) (CompiledBundle, error) {
	e.calls++
	return nil, ErrTemplateInvalid
}

func TestNewCacheRejectsInvalidLimits(t *testing.T) {
	for _, tc := range []struct{ entries, bytes int }{{0, 1}, {1, 0}, {-1, 1}, {1, -1}} {
		t.Run(fmt.Sprintf("%d-%d", tc.entries, tc.bytes), func(t *testing.T) {
			_, err := NewCache(&countingEngine{}, tc.entries, tc.bytes)
			require.ErrorIs(t, err, ErrLimitsInvalid)
		})
	}
}
