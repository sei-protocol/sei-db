package memiavl

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/allegro/bigcache/v3"
)

// Cache is an in-memory structure to persist nodes for quick access.
// Please see lruCache for more details about why we need a custom
// cache implementation.
type Cache interface {
	// Adds key value to cache.
	Add(key []byte, value []byte)

	// Returns Node for the key, if exists. nil otherwise.
	Get(key []byte) []byte

	// Has returns true if node with key exists in cache, false otherwise.
	Has(key []byte) bool

	// Remove removes node with key from cache. The removed value is returned.
	// if not in cache, return nil.
	Remove(key []byte) []byte

	// Len returns the cache length.
	Len() int
}

type LRUCache struct {
	cache *bigcache.BigCache
}

// NewLRUCache creates a new LRU cache with the given size
func NewLRUCache(capacity int) *LRUCache {
	if capacity <= 0 {
		return nil
	}

	config := bigcache.Config{
		// number of shards (must be a power of 2)
		Shards: 1024,

		// time after which entry can be evicted
		LifeWindow: 60 * time.Minute,

		// Interval between removing expired entries (clean up).
		// If set to <= 0 then no action is performed.
		// Setting to < 1 second is counterproductive — bigcache has a one second resolution.
		CleanWindow: 0,

		// rps * lifeWindow, used only in initial memory allocation
		MaxEntriesInWindow: 1000 * 10 * 60,

		// max entry size in bytes, used only in initial memory allocation
		MaxEntrySize: int(math.Min(32, float64(capacity))) * 1024 * 1024,

		// prints information about additional memory allocation
		Verbose: false,

		// cache will not allocate more memory than this limit, value in MB
		// if value is reached then the oldest entries can be overridden for the new ones
		// 0 value means no size limit
		HardMaxCacheSize: capacity,

		// callback fired when the oldest entry is removed because of its expiration time or no space left
		// for the new entry, or because delete was called. A bitmask representing the reason will be returned.
		// Default value is nil which means no callback and it prevents from unwrapping the oldest entry.
		OnRemove: nil,

		// OnRemoveWithReason is a callback fired when the oldest entry is removed because of its expiration time or no space left
		// for the new entry, or because delete was called. A constant representing the reason will be passed through.
		// Default value is nil which means no callback and it prevents from unwrapping the oldest entry.
		// Ignored if OnRemove is specified.
		OnRemoveWithReason: nil,
	}
	fmt.Printf("Creating LRU cache with capacity of %d\n", capacity)
	cache, err := bigcache.New(context.Background(), config)
	if err != nil {
		panic(err)
	}
	return &LRUCache{
		cache: cache,
	}
}

func (c *LRUCache) Add(key []byte, value []byte) {
	_ = c.cache.Set(string(key), value)
}

func (c *LRUCache) Get(key []byte) []byte {
	value, err := c.cache.Get(string(key))
	if err != nil {
		return nil
	}
	return value
}

func (c *LRUCache) Has(key []byte) bool {
	_, err := c.cache.Get(string(key))
	return err == nil
}

func (c *LRUCache) Remove(key []byte) []byte {
	value := c.Get(key)
	_ = c.cache.Delete(string(key))
	return value
}

func (c *LRUCache) Len() int {
	return c.cache.Len()
}
