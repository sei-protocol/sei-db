package memiavl

import (
	"github.com/dgraph-io/ristretto/v2"
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
	Remove(key []byte)
}

type LRUCache struct {
	cache *ristretto.Cache[[]byte, []byte]
}

// NewLRUCache creates a new LRU cache with the given size
func NewLRUCache(capacity int) *LRUCache {
	if capacity <= 0 {
		return nil
	}
	cache, err := ristretto.NewCache(&ristretto.Config[[]byte, []byte]{
		NumCounters: 1e8,                           // number of keys to track frequency of (10M).
		MaxCost:     int64(capacity * 1024 * 1024), // maximum cost of cache (1GB).
		BufferItems: 64,                            // number of keys per Get buffer.
	})
	if err != nil {
		panic(err)
	}

	return &LRUCache{cache: cache}
	//config := bigcache.Config{
	//	// number of shards (must be a power of 2)
	//	Shards: 1024,
	//
	//	// time after which entry can be evicted
	//	LifeWindow: 60 * time.Minute,
	//
	//	// Interval between removing expired entries (clean up).
	//	// If set to <= 0 then no action is performed.
	//	// Setting to < 1 second is counterproductive — bigcache has a one second resolution.
	//	CleanWindow: 0,
	//
	//	// rps * lifeWindow, used only in initial memory allocation
	//	MaxEntriesInWindow: 1000 * 10 * 60,
	//
	//	// max entry size in bytes, used only in initial memory allocation
	//	MaxEntrySize: int(math.Min(16, float64(capacity))) * 1024 * 1024,
	//
	//	// prints information about additional memory allocation
	//	Verbose: false,
	//
	//	// cache will not allocate more memory than this limit, value in MB
	//	// if value is reached then the oldest entries can be overridden for the new ones
	//	// 0 value means no size limit
	//	HardMaxCacheSize: capacity,
	//
	//	// callback fired when the oldest entry is removed because of its expiration time or no space left
	//	// for the new entry, or because delete was called. A bitmask representing the reason will be returned.
	//	// Default value is nil which means no callback and it prevents from unwrapping the oldest entry.
	//	OnRemove: nil,
	//
	//	// OnRemoveWithReason is a callback fired when the oldest entry is removed because of its expiration time or no space left
	//	// for the new entry, or because delete was called. A constant representing the reason will be passed through.
	//	// Default value is nil which means no callback and it prevents from unwrapping the oldest entry.
	//	// Ignored if OnRemove is specified.
	//	OnRemoveWithReason: nil,
	//}
	//fmt.Printf("Creating LRU cache with capacity of %d\n", capacity)
	//cache, err := bigcache.New(context.Background(), config)
	//if err != nil {
	//	panic(err)
	//}
	//return &LRUCache{
	//	cache: cache,
	//}
}

func (c *LRUCache) Add(key []byte, value []byte) {
	_ = c.cache.Set(key, value, int64(len(key)+len(value)))
	c.cache.Wait()
}

func (c *LRUCache) Get(key []byte) []byte {

	value, found := c.cache.Get(key)
	if !found {
		return nil
	}
	return value
}

func (c *LRUCache) Has(key []byte) bool {
	return c.Get(key) == nil
}

func (c *LRUCache) Remove(key []byte) {
	c.cache.Del(key)
}
