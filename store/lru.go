package store

import (
	"container/list"
	"sync"
	"time"
)

//lruCachelistLRU

type lruCache struct {
	mu              sync.RWMutex
	list            *list.List               //LRU
	items           map[string]*list.Element
	maxBytes        int64
	usedBytes       int64
	onEvicted       func(key string, value Value)
	expires         map[string]time.Time
	cleanupInterval time.Duration
	cleanupTicker   *time.Ticker
	closeCh         chan struct{}
}

// lruEntry
type lruEntry struct {
	key   string
	value Value
}

//newLRUCache LRU

// newLRUCache builds an LRU cache with periodic expiration cleanup.
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

// Get
// Get returns the value and updates LRU order if present and not expired.
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
	//LRU
	// Move to the most-recent end under write lock.
	c.mu.Lock()
	if _, ok := c.items[key]; ok {
		c.list.MoveToBack(elem)
	}
	c.mu.Unlock()
	return value, true
}

// Set
func (c *lruCache) Set(key string, value Value) error {
	return c.SetWithExpiration(key, value, 0)
}

// SetWithExpiration stores a value and optional TTL.
func (c *lruCache) SetWithExpiration(key string, value Value, expiration time.Duration) error {
	if value == nil {
		c.Delete(key)
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

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
// cleanupLoop periodically evicts expired items.
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

// Len
func (c *lruCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.list.Len()
}
// evict clears expired entries then enforces size limits.
func (c *lruCache) evict() {

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

// removeElement
// removeElement deletes a list element and triggers onEvicted.
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

// Close
// Close stops the cleanup goroutine and releases resources.
func (c *lruCache) Close() {
	if c.cleanupTicker != nil {
		c.cleanupTicker.Stop()
		close(c.closeCh)
	}
}

// GetWithExpiration
// GetWithExpiration returns the value and remaining TTL when available.
func (c *lruCache) GetWithExpiration(key string) (Value, time.Duration, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	elem, ok := c.items[key]
	if !ok {
		return nil, 0, false
	}

	now := time.Now()
	if expTime, hasExp := c.expires[key]; hasExp {
		if expTime.Before(now) {
			go c.Delete(key)
			return nil, 0, false
		}

		ttl := expTime.Sub(now)
		c.list.MoveToBack(elem)
		return elem.Value.(*lruEntry).value, ttl, true
	}

	c.list.MoveToBack(elem)
	return elem.Value.(*lruEntry).value, 0, true
}

// GetExpiration
func (c *lruCache) GetExpiration(key string) (time.Time, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if expTime, hasExp := c.expires[key]; hasExp {
		return expTime, true
	}
	return time.Time{}, false
}

// UpdateExpiration
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

// UsedBytes
func (c *lruCache) UsedBytes() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.usedBytes
}

// MaxBytes
func (c *lruCache) MaxBytes() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.maxBytes
}

// SetMaxBytes
func (c *lruCache) SetMaxBytes(maxBytes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.maxBytes = maxBytes
	if maxBytes > 0 {
		c.evict()
	}
}
