// Package store provides cache storage implementations
package store

import (
	"container/list"
	"sync"
	"time"
)

// lru2Cache represents a LRU-2 (Least Recently Used 2) cache implementation
// LRU-2 keeps track of items that have been accessed at least twice before considering them for eviction
type lru2Cache struct {
	mu         sync.RWMutex
	mainList   *list.List
	candidateList *list.List
	mainItems  map[string]*list.Element
	candidates map[string]*list.Element
	maxBytes   int64
	usedBytes  int64
	onEvicted  func(key string, value Value)
}

// lru2Entry represents an entry in the LRU-2 cache
type lru2Entry struct {
	key   string
	value Value
	count int
}

// newLRU2Cache creates a new LRU-2 cache instance
func newLRU2Cache(options Options) *lru2Cache {
	return &lru2Cache{
		mainList:      list.New(),
		candidateList: list.New(),
		mainItems:     make(map[string]*list.Element),
		candidates:    make(map[string]*list.Element),
		maxBytes:      options.MaxBytes,
		onEvicted:     options.OnEvicted,
	}
}

// Get retrieves a value from the LRU-2 cache
// Get promotes candidates to main on the second access.
func (c *lru2Cache) Get(key string) (Value, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()


	if elem, ok := c.mainItems[key]; ok {
		entry := elem.Value.(*lru2Entry)

		c.mainList.MoveToBack(elem)
		return entry.value, true
	}


	if elem, ok := c.candidates[key]; ok {
		entry := elem.Value.(*lru2Entry)

		c.candidateList.Remove(elem)
		delete(c.candidates, key)


		entry.count++
		newElem := c.mainList.PushBack(entry)
		c.mainItems[key] = newElem

		return entry.value, true
	}

	return nil, false
}

// Set adds or updates a cache item
func (c *lru2Cache) Set(key string, value Value) error {
	return c.SetWithExpiration(key, value, 0)
}

// SetWithExpiration sets a key-value pair with an expiration time
// For simplicity, LRU-2 implementation doesn't handle expiration directly here
// SetWithExpiration ignores TTL and behaves like a normal set.
func (c *lru2Cache) SetWithExpiration(key string, value Value, expiration time.Duration) error {
	if value == nil {
		c.Delete(key)
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()


	if elem, ok := c.mainItems[key]; ok {
		oldEntry := elem.Value.(*lru2Entry)
		c.usedBytes -= int64(oldEntry.value.Len() + len(key))


		oldEntry.value = value
		c.usedBytes += int64(value.Len() + len(key))


		c.mainList.MoveToBack(elem)
	} else if elem, ok := c.candidates[key]; ok {

		entry := elem.Value.(*lru2Entry)
		entry.value = value
		entry.count++


		c.candidateList.Remove(elem)
		delete(c.candidates, key)


		newElem := c.mainList.PushBack(entry)
		c.mainItems[key] = newElem
		c.usedBytes += int64(value.Len() + len(key))
	} else {

		entry := &lru2Entry{
			key:   key,
			value: value,
			count: 1,
		}
		elem := c.candidateList.PushBack(entry)
		c.candidates[key] = elem
		c.usedBytes += int64(value.Len() + len(key))
	}


	c.evictIfNeeded()

	return nil
}

// Delete removes a key from the cache
func (c *lru2Cache) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.mainItems[key]; ok {
		c.removeItem(elem, true)
		delete(c.mainItems, key)
		return true
	}

	if elem, ok := c.candidates[key]; ok {
		c.removeItem(elem, false)
		delete(c.candidates, key)
		return true
	}

	return false
}

// removeItem removes an item from the cache and calls the eviction callback if set
// removeItem deletes an entry and adjusts size accounting.
func (c *lru2Cache) removeItem(elem *list.Element, isMain bool) {
	var entry *lru2Entry
	if isMain {
		entry = elem.Value.(*lru2Entry)
		c.mainList.Remove(elem)
	} else {
		entry = elem.Value.(*lru2Entry)
		c.candidateList.Remove(elem)
	}

	c.usedBytes -= int64(entry.value.Len() + len(entry.key))

	if c.onEvicted != nil {
		c.onEvicted(entry.key, entry.value)
	}
}

// Clear clears all entries from the cache
func (c *lru2Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()


	for key, elem := range c.mainItems {
		c.removeItem(elem, true)
		delete(c.mainItems, key)
	}


	for key, elem := range c.candidates {
		c.removeItem(elem, false)
		delete(c.candidates, key)
	}
}

// evictIfNeeded removes entries if the cache exceeds maxBytes
// evictIfNeeded trims only from the main list (second-touch items).
func (c *lru2Cache) evictIfNeeded() {
	for c.usedBytes > c.maxBytes && c.mainList.Len() > 0 {

		elem := c.mainList.Front()
		if elem != nil {
			entry := elem.Value.(*lru2Entry)
			c.removeItem(elem, true)
			delete(c.mainItems, entry.key)
		}
	}
}

// Len returns the number of items in the cache
func (c *lru2Cache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.mainItems) + len(c.candidates)
}

// Close closes the cache and releases resources
func (c *lru2Cache) Close() {
	c.Clear()
}
