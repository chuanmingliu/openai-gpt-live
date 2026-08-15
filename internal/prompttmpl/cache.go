package prompttmpl

import (
	"container/list"
	"context"
	"errors"
	"sync"
)

const cacheEntryOverhead = 256

// Cache retains a bounded, immutable set of compiled template bundles.
type Cache struct {
	engine               Engine
	maxEntries, maxBytes int
	mu                   sync.Mutex
	lru                  *list.List
	byKey                map[string]*list.Element
	inflight             map[string]*compileCall
	residentBytes        int
}

type cacheEntry struct {
	key   string
	value CompiledBundle
	bytes int
}

type compileCall struct {
	done  chan struct{}
	value CompiledBundle
	err   error
}

// NewCache creates a cache with independent entry and byte ceilings.
func NewCache(engine Engine, entries, bytes int) (*Cache, error) {
	if engine == nil || entries <= 0 || bytes <= 0 {
		return nil, ErrLimitsInvalid
	}
	return &Cache{
		engine:     engine,
		maxEntries: entries,
		maxBytes:   bytes,
		lru:        list.New(),
		byKey:      make(map[string]*list.Element),
		inflight:   make(map[string]*compileCall),
	}, nil
}

// Compile returns a cached immutable compiled bundle or compiles one once for
// all simultaneous cache misses of the same bundle and limits.
func (c *Cache) Compile(ctx context.Context, bundle Bundle, requested Limits) (CompiledBundle, error) {
	if c == nil || c.engine == nil {
		return nil, &CompileError{Err: ErrLimitsInvalid}
	}
	limits, err := requested.Validate()
	if err != nil {
		return nil, &CompileError{Err: ErrLimitsInvalid}
	}
	owned := cloneBundle(bundle)
	engineVersion := c.engine.Version()
	key := cacheFingerprint(engineVersion, owned, limits)

	c.mu.Lock()
	if element := c.byKey[key]; element != nil {
		c.lru.MoveToFront(element)
		value := element.Value.(*cacheEntry).value
		c.mu.Unlock()
		return value, nil
	}
	if call := c.inflight[key]; call != nil {
		c.mu.Unlock()
		select {
		case <-call.done:
			return call.value, call.err
		case <-nonNilContext(ctx).Done():
			return nil, &CompileError{Err: ErrTemplateInvalid}
		}
	}
	call := &compileCall{done: make(chan struct{})}
	c.inflight[key] = call
	c.mu.Unlock()

	value, err := c.engine.Compile(nonNilContext(ctx), owned, limits)
	if err == nil && value == nil {
		err = errors.New("engine returned nil compiled bundle")
	}

	c.mu.Lock()
	delete(c.inflight, key)
	call.value, call.err = value, err
	if err == nil {
		c.admit(key, value, estimateCacheBytes(engineVersion, key, owned, limits, value))
	}
	close(call.done)
	c.mu.Unlock()
	return value, err
}

func (c *Cache) admit(key string, value CompiledBundle, bytes int) {
	if bytes > c.maxBytes {
		return
	}
	for c.lru.Len() > 0 && (c.lru.Len() >= c.maxEntries || c.residentBytes+bytes > c.maxBytes) {
		tail := c.lru.Back()
		entry := tail.Value.(*cacheEntry)
		delete(c.byKey, entry.key)
		c.residentBytes -= entry.bytes
		c.lru.Remove(tail)
	}
	entry := &cacheEntry{key: key, value: value, bytes: bytes}
	c.byKey[key] = c.lru.PushFront(entry)
	c.residentBytes += bytes
}

func estimateCacheBytes(engineVersion, key string, bundle Bundle, limits Limits, compiled CompiledBundle) int {
	bytes := cacheEntryOverhead
	bytes = saturatingAdd(bytes, len(key))
	bytes = saturatingAdd(bytes, canonicalBundleBytes(engineVersion, bundle, limits))
	bytes = saturatingAdd(bytes, compiled.ResidentBytes())
	return bytes
}

func saturatingAdd(current, added int) int {
	maxInt := int(^uint(0) >> 1)
	if added < 0 || current > maxInt-added {
		return maxInt
	}
	return current + added
}
