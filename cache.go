package Group_Cache

import (
	"Group_Cache/store"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

// Cache
// Cache is a threadsafe wrapper around the selected store implementation.
type Cache struct {
	mu          sync.RWMutex
	store       store.Store
	opts        CacheOptions
	hits        int64
	misses      int64
	initialized int32
	closed      int32
}

// CacheOptions
type CacheOptions struct {
	CacheType    store.CacheType
	MaxBytes     int64
	BucketCount  uint16
	CapPerBucket uint16
	Level2Cap    uint16
	CleanupTime  time.Duration
	OnEvicted    func(key string, value store.Value)
}

// DefaultCacheOptions
func DefaultCacheOptions() CacheOptions {
	return CacheOptions{
		CacheType:    store.LRU2,
		MaxBytes:     8 * 1024 * 1024,
		BucketCount:  16,
		CapPerBucket: 512,
		Level2Cap:    256,
		CleanupTime:  time.Minute,
		OnEvicted:    nil,
	}
}

// NewCache
func NewCache(opts CacheOptions) *Cache {
	return &Cache{
		opts: opts,
	}
}

// ensureInitialized
// ensureInitialized lazily creates the underlying store on first use.
func (c *Cache) ensureInitialized() {
	if atomic.LoadInt32(&c.initialized) == 1 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.initialized == 0 {
		c.store = store.NewStore(c.opts.CacheType, store.Options{
			MaxBytes:        c.opts.MaxBytes,
			BucketCount:     c.opts.BucketCount,
			CapPerBucket:    c.opts.CapPerBucket,
			Level2Cap:       c.opts.Level2Cap,
			CleanupInterval: c.opts.CleanupTime,
			OnEvicted:       c.opts.OnEvicted,
		})
		atomic.StoreInt32(&c.initialized, 1)
		logrus.Infof("cache initialized with type %s, max bytes %d", c.opts.CacheType, c.opts.MaxBytes)
	}
}

// Add key-value
func (c *Cache) Add(key string, value ByteView) {
	if atomic.LoadInt32(&c.closed) == 1 {
		logrus.Warnf("Attempted to add to a closed cache: %s", key)
		return
	}
	c.ensureInitialized()
	if err := c.store.Set(key, value); err != nil {
		logrus.Warnf("Failed to add key %s to cache: %v", key, err)
	}
}

// Get keyvalue
// Get returns the cached value, tracking hit/miss metrics.
func (c *Cache) Get(ctx context.Context, key string) (value ByteView, ok bool) {
	if atomic.LoadInt32(&c.closed) == 1 {
		return ByteView{}, false
	}

	if atomic.LoadInt32(&c.initialized) == 0 {
		atomic.AddInt64(&c.misses, 1)
		return ByteView{}, false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	val, ok := c.store.Get(key)
	if !ok {
		atomic.AddInt64(&c.misses, 1)
		return ByteView{}, false
	}
	atomic.AddInt64(&c.hits, 1)
	if bv, ok := val.(ByteView); ok {
		return bv, true
	}

	logrus.Warnf("Failed to assert value for key %s to ByteView", key)
	atomic.AddInt64(&c.misses, 1)
	return ByteView{}, false
}

// AddWithExpiration key-value
// AddWithExpiration stores a value with a TTL derived from expirationTime.
func (c *Cache) AddWithExpiration(key string, value ByteView, expirationTime time.Time) {
	if atomic.LoadInt32(&c.closed) == 1 {
		logrus.Warnf("Attempted to add to a closed cache: %s", key)
		return
	}
	c.ensureInitialized()
	expiration := time.Until(expirationTime)
	if expiration <= 0 {
		logrus.Warnf("Attempted to add key %s with expired time in the past: %v", key, expirationTime)
		return
	}

	if err := c.store.SetWithExpiration(key, value, expiration); err != nil {
		logrus.Warnf("Failed to add key %s to cache: %v", key, err)
	}
}

// Delete key
func (c *Cache) Delete(key string) bool {
	if atomic.LoadInt32(&c.closed) == 1 || atomic.LoadInt32(&c.initialized) == 0 {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.store.Delete(key)
}

// Clear
func (c *Cache) Clear() {
	if atomic.LoadInt32(&c.closed) == 1 || atomic.LoadInt32(&c.initialized) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.store.Clear()

	atomic.StoreInt64(&c.hits, 0)
	atomic.StoreInt64(&c.misses, 0)
}

// Len
func (c *Cache) Len() int {
	if atomic.LoadInt32(&c.closed) == 1 || atomic.LoadInt32(&c.initialized) == 0 {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.store.Len()
}

// Close ,
// Close releases the underlying store and freezes the cache.
func (c *Cache) Close() {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.store != nil {
		if closer, ok := c.store.(interface{ Close() }); ok {
			closer.Close()
		}
		c.store = nil
	}

	atomic.StoreInt32(&c.initialized, 0)
	logrus.Infof("cache closed,hits:%d,misses:%d", atomic.LoadInt64(&c.hits), atomic.LoadInt64(&c.misses))
}

// Stats
// Stats exposes cache-level metrics and size.
func (c *Cache) Stats() map[string]interface{} {
	stats := map[string]interface{}{
		"initialized": atomic.LoadInt32(&c.initialized) == 1,
		"hits":        atomic.LoadInt64(&c.hits),
		"misses":      atomic.LoadInt64(&c.misses),
		"closed":      atomic.LoadInt32(&c.closed) == 1,
	}
	if atomic.LoadInt32(&c.initialized) == 1 {
		stats["size"] = c.Len()

		totalRequests := atomic.LoadInt64(&c.hits) + atomic.LoadInt64(&c.misses)
		if totalRequests > 0 {
			stats["hit_rate"] = float64(atomic.LoadInt64(&c.hits)) / float64(totalRequests)
		} else {
			stats["hit_rate"] = 0.0
		}
	}
	return stats
}
