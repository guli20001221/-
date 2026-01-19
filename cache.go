package Group_Cache

import (
	"Group_Cache/store"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

// Cache是对底层缓存的封装
type Cache struct {
	mu          sync.RWMutex
	store       store.Store
	opts        CacheOptions
	hits        int64
	misses      int64
	initialized int32
	closed      int32
}

// CacheOptions定义了缓存的配置选项
type CacheOptions struct {
	CacheType    store.CacheType
	MaxBytes     int64
	BucketCount  uint16
	CapPerBucket uint16
	Level2Cap    uint16
	CleanupTime  time.Duration
	OnEvicted    func(key string, value store.Value)
}

// DefaultCacheOptions返回默认的缓存配置选项
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

// NewCache创建一个新的缓存实例
func NewCache(opts CacheOptions) *Cache {
	return &Cache{
		opts: opts,
	}
}

// ensureInitialized确保缓存已初始化
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

// Add 向缓存中添加一个key-value对
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

// Get 从缓存中获取一个key对应的value
func (c *Cache) Get(ctx context.Context, key string) (value ByteView, ok bool) {
	if atomic.LoadInt32(&c.closed) == 1 {
		return ByteView{}, false
	}
	//如果缓存未初始化，直接返回未命中
	if atomic.LoadInt32(&c.initialized) == 0 {
		atomic.AddInt64(&c.misses, 1)
		return ByteView{}, false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	//从底层存储获取
	val, ok := c.store.Get(key)
	if !ok {
		atomic.AddInt64(&c.misses, 1)
		return ByteView{}, false
	}
	atomic.AddInt64(&c.hits, 1)
	if bv, ok := val.(ByteView); ok {
		return bv, true
	}
	//类型断言失败，返回未命中
	logrus.Warnf("Failed to assert value for key %s to ByteView", key)
	atomic.AddInt64(&c.misses, 1)
	return ByteView{}, false
}

// AddWithExpiration 向缓存中添加一个带有过期时间的key-value对
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
	//设置到底层存储
	if err := c.store.SetWithExpiration(key, value, expiration); err != nil {
		logrus.Warnf("Failed to add key %s to cache: %v", key, err)
	}
}

// Delete 从缓存中删除一个key
func (c *Cache) Delete(key string) bool {
	if atomic.LoadInt32(&c.closed) == 1 || atomic.LoadInt32(&c.initialized) == 0 {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.store.Delete(key)
}

// Clear 清空缓存
func (c *Cache) Clear() {
	if atomic.LoadInt32(&c.closed) == 1 || atomic.LoadInt32(&c.initialized) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.store.Clear()
	//重置统计信息
	atomic.StoreInt64(&c.hits, 0)
	atomic.StoreInt64(&c.misses, 0)
}

// Len 返回缓存中元素的数量
func (c *Cache) Len() int {
	if atomic.LoadInt32(&c.closed) == 1 || atomic.LoadInt32(&c.initialized) == 0 {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.store.Len()
}

// Close 关闭缓存,释放资源
func (c *Cache) Close() {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	//关闭底层存储
	if c.store != nil {
		if closer, ok := c.store.(interface{ Close() }); ok {
			closer.Close()
		}
		c.store = nil
	}
	//重置缓存状态
	atomic.StoreInt32(&c.initialized, 0)
	logrus.Infof("cache closed,hits:%d,misses:%d", atomic.LoadInt64(&c.hits), atomic.LoadInt64(&c.misses))
}

// Stats 返回缓存的统计信息
func (c *Cache) Stats() map[string]interface{} {
	stats := map[string]interface{}{
		"initialized": atomic.LoadInt32(&c.initialized) == 1,
		"hits":        atomic.LoadInt64(&c.hits),
		"misses":      atomic.LoadInt64(&c.misses),
		"closed":      atomic.LoadInt32(&c.closed) == 1,
	}
	if atomic.LoadInt32(&c.initialized) == 1 {
		stats["size"] = c.Len()
		//计算命中率
		totalRequests := atomic.LoadInt64(&c.hits) + atomic.LoadInt64(&c.misses)
		if totalRequests > 0 {
			stats["hit_rate"] = float64(atomic.LoadInt64(&c.hits)) / float64(totalRequests)
		} else {
			stats["hit_rate"] = 0.0
		}
	}
	return stats
}
