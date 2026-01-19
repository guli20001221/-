package store

import (
	"container/list"
	"sync"
	"time"
)

//lruCache是基于标准库list的LRU实现

type lruCache struct {
	mu              sync.RWMutex
	list            *list.List               //双向链表，用于维护LRU顺序
	items           map[string]*list.Element //键到链表节点的映射
	maxBytes        int64
	usedBytes       int64
	onEvicted       func(key string, value Value)
	expires         map[string]time.Time //过期时间映射
	cleanupInterval time.Duration
	cleanupTicker   *time.Ticker
	closeCh         chan struct{}
}

// lruEntry 表示缓存中的一个条目
type lruEntry struct {
	key   string
	value Value
}

//newLRUCache 创建一个新的LRU缓存实例

func newLRUCache(options Options) *lruCache {
	cleanupInterval := options.CleanupInterval
	if cleanupInterval <= 0 {
		cleanupInterval = time.Minute
	}
	c := &lruCache{
		list:            list.New(),
		items:           make(map[string]*list.Element),
		maxBytes:        options.MaxBytes,
		onEvicted:       options.OnEvicted,
		expires:         make(map[string]time.Time),
		cleanupInterval: cleanupInterval,
		closeCh:         make(chan struct{}),
	}
	c.cleanupTicker = time.NewTicker(cleanupInterval)
	go c.cleanupLoop()

	return c
}

// Get 获取指定键对应的缓存项，如果存在且未过期，则返回该项的值
func (c *lruCache) Get(key string) (Value, bool) {
	c.mu.RLock()
	elem, ok := c.items[key]
	if !ok {
		c.mu.RUnlock()
		return nil, false
	}
	if expTime, hasExp := c.expires[key]; hasExp {
		if expTime.Before(time.Now()) {
			c.mu.RUnlock()
			go c.Delete(key)
			return nil, false
		}
	}
	entry := elem.Value.(*lruEntry)
	value := entry.value
	c.mu.RUnlock()
	//更新LRU位置需要写锁
	c.mu.Lock()
	if _, ok := c.items[key]; ok {
		c.list.MoveToBack(elem)
	}
	c.mu.Unlock()
	return value, true
}

// Set 添加或更新缓存项
func (c *lruCache) Set(key string, value Value) error {
	return c.SetWithExpiration(key, value, 0)
}

func (c *lruCache) SetWithExpiration(key string, value Value, expiration time.Duration) error {
	if value == nil {
		c.Delete(key)
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	//计算过期时间
	var expTime time.Time
	if expiration > 0 {
		expTime = time.Now().Add(expiration)
		c.expires[key] = expTime
	} else {
		delete(c.expires, key)
	}
	if elem, ok := c.items[key]; ok {
		entry := elem.Value.(*lruEntry)
		c.usedBytes -= int64(entry.value.Len() + len(entry.key))
		entry.value = value
		c.usedBytes += int64(value.Len() + len(key))
		c.list.MoveToBack(elem)
	} else {
		entry := &lruEntry{key, value}
		elem := c.list.PushBack(entry)
		c.items[key] = elem
		c.usedBytes += int64(value.Len() + len(key))
	}
	c.evict()
	return nil
}
func (c *lruCache) cleanupLoop() {
	for {
		select {
		case <-c.cleanupTicker.C:
			c.mu.Lock()
			c.evict()
			c.mu.Unlock()
		case <-c.closeCh:
			return
		}
	}
}

func (c *lruCache) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[key]; ok {
		c.removeElement(elem)
		return true
	}
	return false
}
func (c *lruCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	//如果设置了回调函数，遍历所有项调用回调
	if c.onEvicted != nil {
		for k, v := range c.items {
			c.onEvicted(k, v.Value.(*lruEntry).value)
		}
	}
	c.list.Init()
	c.items = make(map[string]*list.Element)
	c.expires = make(map[string]time.Time)
	c.usedBytes = 0
}

// Len 返回缓存项的数量
func (c *lruCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.list.Len()
}
func (c *lruCache) evict() {
	//先清理过期项
	now := time.Now()
	for k, expTime := range c.expires {
		if expTime.Before(now) {
			if elem, ok := c.items[k]; ok {
				c.removeElement(elem)
			}
		}
	}
	if c.maxBytes <= 0 {
		return
	}
	for c.usedBytes > c.maxBytes {
		if !c.removeOldest() {
			break
		}
	}
}

// removeElement 从缓存中删除指定元素，调用此方法前必须持有锁
func (c *lruCache) removeElement(elem *list.Element) {
	entry := elem.Value.(*lruEntry)
	delete(c.items, entry.key)
	delete(c.expires, entry.key)
	c.list.Remove(elem)
	c.usedBytes -= int64(entry.value.Len() + len(entry.key))
	if c.onEvicted != nil {
		c.onEvicted(entry.key, entry.value)
	}
}

func (c *lruCache) removeOldest() bool {
	elem := c.list.Front()
	if elem == nil {
		return false
	}
	c.removeElement(elem)
	return true
}

// Close关闭缓存，停止清理协程
func (c *lruCache) Close() {
	if c.cleanupTicker != nil {
		c.cleanupTicker.Stop()
		close(c.closeCh)
	}
}

// GetWithExpiration 获取指定键对应的缓存项，如果存在且未过期，则返回该项的值和剩余的过期时间
func (c *lruCache) GetWithExpiration(key string) (Value, time.Duration, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	elem, ok := c.items[key]
	if !ok {
		return nil, 0, false
	}
	//检查是否过期
	now := time.Now()
	if expTime, hasExp := c.expires[key]; hasExp {
		if expTime.Before(now) {
			go c.Delete(key)
			return nil, 0, false
		}
		//计算剩余的过期时间
		ttl := expTime.Sub(now)
		c.list.MoveToBack(elem)
		return elem.Value.(*lruEntry).value, ttl, true
	}
	//无过期时间
	c.list.MoveToBack(elem)
	return elem.Value.(*lruEntry).value, 0, true
}

// GetExpiration 获取指定键对应的缓存项的过期时间
func (c *lruCache) GetExpiration(key string) (time.Time, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if expTime, hasExp := c.expires[key]; hasExp {
		return expTime, true
	}
	return time.Time{}, false
}

// UpdateExpiration 更新指定键对应的缓存项的过期时间
func (c *lruCache) UpdateExpiration(key string, expiration time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.items[key]; ok {
		if expiration > 0 {
			expTime := time.Now().Add(expiration)
			c.expires[key] = expTime
		} else {
			delete(c.expires, key)
		}
		return true
	}
	return false
}

// UsedBytes 返回已使用的字节数
func (c *lruCache) UsedBytes() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.usedBytes
}

// MaxBytes 获取缓存的最大字节数
func (c *lruCache) MaxBytes() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.maxBytes
}

// SetMaxBytes 设置缓存的最大字节数并触发淘汰
func (c *lruCache) SetMaxBytes(maxBytes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.maxBytes = maxBytes
	if maxBytes > 0 {
		c.evict()
	}
}
