// Package store provides cache storage implementations
package store

import (
	"testing"
	"time"
)

// 实现一个简单的 Value 类型用于测试
type storeTestValue struct {
	data string
}

func (t storeTestValue) Len() int {
	return len(t.data)
}

func TestNewStore(t *testing.T) {
	// 测试创建 LRU 缓存
	lruOptions := NewOptions()
	lruOptions.MaxBytes = 100
	lruCache := NewStore(LRU, lruOptions)
	
	if lruCache == nil {
		t.Fatal("NewStore should return a valid LRU cache")
	}
	
	// 测试设置和获取值
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
	
	// 设置带1秒过期时间的值
	err := cache.SetWithExpiration(testKey, testValue, time.Second)
	if err != nil {
		t.Fatalf("SetWithExpiration failed: %v", err)
	}
	
	// 立即获取，应该能获取到
	value, ok := cache.Get(testKey)
	if !ok {
		t.Error("Value should exist immediately after setting")
	}
	
	if tv, ok := value.(storeTestValue); ok && tv.data != "exp_store_value" {
		t.Errorf("Expected 'exp_store_value', got '%s'", tv.data)
	}
	
	// 等待过期
	time.Sleep(2 * time.Second)
	
	// 再次获取，应该已经过期
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
	
	// 验证值存在
	_, ok := cache.Get(testKey)
	if !ok {
		t.Error("Value was not set properly")
	}
	
	// 删除键
	result := cache.Delete(testKey)
	if !result {
		t.Error("Delete should return true when key exists")
	}
	
	// 验证键已被删除
	_, ok = cache.Get(testKey)
	if ok {
		t.Error("Value should be deleted after Delete call")
	}
}

func TestStoreClear(t *testing.T) {
	options := NewOptions()
	options.MaxBytes = 100
	cache := NewStore(LRU, options)
	
	// 添加一些键值对
	cache.Set("key1", storeTestValue{data: "value1"})
	cache.Set("key2", storeTestValue{data: "value2"})
	cache.Set("key3", storeTestValue{data: "value3"})
	
	// 验证值存在
	_, ok1 := cache.Get("key1")
	_, ok2 := cache.Get("key2")
	_, ok3 := cache.Get("key3")
	if !ok1 || !ok2 || !ok3 {
		t.Error("Values were not set properly")
	}
	
	// 清空缓存
	cache.Clear()
	
	// 验证所有值都被清除
	_, ok1 = cache.Get("key1")
	_, ok2 = cache.Get("key2")
	_, ok3 = cache.Get("key3")
	if ok1 || ok2 || ok3 {
		t.Error("Values should be cleared after Clear call")
	}
}

func TestStoreDefaultLRU(t *testing.T) {
	// 测试默认情况下返回LRU缓存
	options := NewOptions()
	options.MaxBytes = 100
	defaultCache := NewStore(CacheType("invalid"), options)
	
	if defaultCache == nil {
		t.Fatal("NewStore should return a default LRU cache for invalid type")
	}
	
	// 测试基本功能
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
	// 测试创建 LRU2 缓存
	options := NewOptions()
	options.MaxBytes = 100
	lru2Cache := NewStore(LRU2, options)
	
	if lru2Cache == nil {
		t.Fatal("NewStore should return a valid LRU2 cache")
	}
	
	// 测试基本功能
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