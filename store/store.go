// Package store
package store

import "time"

// Value Len()

// Value reports its memory footprint for eviction accounting.
type Value interface {
	Len() int
}

// Store

// Store is the cache backend interface used by Cache.
type Store interface {
	// Get key

	Get(key string) (Value, bool)

	// Set key-value
	Set(key string, value Value) error

	// SetWithExpiration key-value
	SetWithExpiration(key string, value Value, expiration time.Duration) error

	// Delete key

	Delete(key string) bool

	// Clear
	Clear()

	// Len
	Len() int

	// Close
	Close()
}

// CacheType

type CacheType string


const (
	LRU  CacheType = "lru"  // LRU(Least Recently Used)
	LRU2 CacheType = "lru2" // LRU-2(Least Recently Used 2) LRU
)

// Options

type Options struct {
	MaxBytes        int64                         //  lru
	BucketCount     uint16                        //  lru-2
	CapPerBucket    uint16                        //  lru-2
	Level2Cap       uint16                        // lru-2  lru-2
	CleanupInterval time.Duration
	OnEvicted       func(key string, value Value)
}

// NewOptions
func NewOptions() Options {
	return Options{
		MaxBytes:        8192,        // 8KB
		BucketCount:     16,
		CapPerBucket:    512,
		Level2Cap:       256,
		CleanupInterval: time.Minute,
		OnEvicted:       nil,
	}
}

// NewStore
// LRU
// NewStore picks a store implementation by type.
func NewStore(cacheType CacheType, options Options) Store {
	switch cacheType {
	case LRU:
		// LRU
		return newLRUCache(options)
	case LRU2:
		// LRU2
		return newLRU2Cache(options)
	default:
		return newLRUCache(options)
	}
}
