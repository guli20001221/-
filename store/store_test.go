// Package store provides cache storage implementations
package store

import (
	"testing"
	"time"
)

//  Value
type storeTestValue struct {
	data string
}

func (t storeTestValue) Len() int {
	return len(t.data)
}

func TestNewStore(t *testing.T) {
	//  LRU
	lruOptions := NewOptions()
	lruOptions.MaxBytes = 100
	lruCache := NewStore(LRU, lruOptions)

	if lruCache == nil {
		t.Fatal("NewStore should return a valid LRU cache")
	}


	testKey := "test_store_key"
	testValue := storeTestValue{data: "test_store_value"}
	err := lruCache.Set(testKey, testValue)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	value, ok := lruCache.Get(testKey)
	if !ok {
		t.Fatal("Get failed: key not found")
	}

	tv, ok := value.(storeTestValue)
	if !ok {
		t.Fatal("Get failed: wrong type")
	}

	if tv.data != "test_store_value" {
		t.Errorf("Expected 'test_store_value', got '%s'", tv.data)
	}
}

func TestNewStoreWithExpiration(t *testing.T) {
	options := NewOptions()
	options.MaxBytes = 100
	cache := NewStore(LRU, options)

	testKey := "exp_store_test"
	testValue := storeTestValue{data: "exp_store_value"}


	err := cache.SetWithExpiration(testKey, testValue, time.Second)
	if err != nil {
		t.Fatalf("SetWithExpiration failed: %v", err)
	}


	value, ok := cache.Get(testKey)
	if !ok {
		t.Error("Value should exist immediately after setting")
	}

	if tv, ok := value.(storeTestValue); ok && tv.data != "exp_store_value" {
		t.Errorf("Expected 'exp_store_value', got '%s'", tv.data)
	}


	time.Sleep(2 * time.Second)


	_, ok = cache.Get(testKey)
	if ok {
		t.Error("Value should be expired and not retrievable")
	}
}

func TestStoreDelete(t *testing.T) {
	options := NewOptions()
	options.MaxBytes = 100
	cache := NewStore(LRU, options)

	testKey := "delete_store_test"
	testValue := storeTestValue{data: "delete_store_value"}

	err := cache.Set(testKey, testValue)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}


	_, ok := cache.Get(testKey)
	if !ok {
		t.Error("Value was not set properly")
	}


	result := cache.Delete(testKey)
	if !result {
		t.Error("Delete should return true when key exists")
	}


	_, ok = cache.Get(testKey)
	if ok {
		t.Error("Value should be deleted after Delete call")
	}
}

func TestStoreClear(t *testing.T) {
	options := NewOptions()
	options.MaxBytes = 100
	cache := NewStore(LRU, options)


	cache.Set("key1", storeTestValue{data: "value1"})
	cache.Set("key2", storeTestValue{data: "value2"})
	cache.Set("key3", storeTestValue{data: "value3"})


	_, ok1 := cache.Get("key1")
	_, ok2 := cache.Get("key2")
	_, ok3 := cache.Get("key3")
	if !ok1 || !ok2 || !ok3 {
		t.Error("Values were not set properly")
	}


	cache.Clear()


	_, ok1 = cache.Get("key1")
	_, ok2 = cache.Get("key2")
	_, ok3 = cache.Get("key3")
	if ok1 || ok2 || ok3 {
		t.Error("Values should be cleared after Clear call")
	}
}

func TestStoreDefaultLRU(t *testing.T) {
	// LRU
	options := NewOptions()
	options.MaxBytes = 100
	defaultCache := NewStore(CacheType("invalid"), options)

	if defaultCache == nil {
		t.Fatal("NewStore should return a default LRU cache for invalid type")
	}


	testKey := "default_test"
	testValue := storeTestValue{data: "default_value"}
	err := defaultCache.Set(testKey, testValue)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	value, ok := defaultCache.Get(testKey)
	if !ok {
		t.Fatal("Get failed: key not found in default cache")
	}

	tv, ok := value.(storeTestValue)
	if !ok {
		t.Fatal("Get failed: wrong type in default cache")
	}

	if tv.data != "default_value" {
		t.Errorf("Expected 'default_value', got '%s'", tv.data)
	}
}

func TestStoreLRU2(t *testing.T) {
	//  LRU2
	options := NewOptions()
	options.MaxBytes = 100
	lru2Cache := NewStore(LRU2, options)

	if lru2Cache == nil {
		t.Fatal("NewStore should return a valid LRU2 cache")
	}


	testKey := "lru2_test"
	testValue := storeTestValue{data: "lru2_value"}
	err := lru2Cache.Set(testKey, testValue)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	value, ok := lru2Cache.Get(testKey)
	if !ok {
		t.Fatal("Get failed: key not found in LRU2 cache")
	}

	tv, ok := value.(storeTestValue)
	if !ok {
		t.Fatal("Get failed: wrong type in LRU2 cache")
	}

	if tv.data != "lru2_value" {
		t.Errorf("Expected 'lru2_value', got '%s'", tv.data)
	}
}
