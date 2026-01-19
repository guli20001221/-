package store

import (
	"testing"
	"time"
)

//  Value
type fakeValue struct {
	data string
}

func (f fakeValue) Len() int {
	return len(f.data)
}

func TestLRUCache_SetAndGet(t *testing.T) {
	cache := newLRUCache(Options{
		MaxBytes: 100,
	})


	testKey := "test_key"
	testValue := fakeValue{data: "test_value"}
	err := cache.Set(testKey, testValue)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	value, ok := cache.Get(testKey)
	if !ok {
		t.Fatal("Get failed: key not found")
	}

	fakeVal, ok := value.(fakeValue)
	if !ok {
		t.Fatal("Get failed: wrong type")
	}

	if fakeVal.data != "test_value" {
		t.Errorf("Expected 'test_value', got '%s'", fakeVal.data)
	}
}

func TestLRUCache_Delete(t *testing.T) {
	cache := newLRUCache(Options{
		MaxBytes: 100,
	})

	testKey := "delete_test"
	testValue := fakeValue{data: "delete_value"}
	err := cache.Set(testKey, testValue)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}


	_, ok := cache.Get(testKey)
	if !ok {
		t.Fatal("Value was not set properly")
	}


	result := cache.Delete(testKey)
	if !result {
		t.Error("Delete should return true when key exists")
	}


	_, ok = cache.Get(testKey)
	if ok {
		t.Error("Value should be deleted after Delete call")
	}


	result = cache.Delete("non_existent_key")
	if result {
		t.Error("Delete should return false when key doesn't exist")
	}
}

func TestLRUCache_Clear(t *testing.T) {
	cache := newLRUCache(Options{
		MaxBytes: 100,
	})


	cache.Set("key1", fakeValue{data: "value1"})
	cache.Set("key2", fakeValue{data: "value2"})
	cache.Set("key3", fakeValue{data: "value3"})


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

func TestLRUCache_MaxBytesEviction(t *testing.T) {

	cache := newLRUCache(Options{
		MaxBytes: 20,
	})



	err := cache.Set("key1", fakeValue{data: "very_long_value_that_exceeds_limit"})
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}


	_, ok := cache.Get("key1")
	if ok {
		t.Error("Value should be evicted when over capacity")
	}
}

func TestLRUCache_Expiration(t *testing.T) {
	cache := newLRUCache(Options{
		MaxBytes: 100,
	})

	testKey := "exp_test"
	testValue := fakeValue{data: "exp_value"}


	err := cache.SetWithExpiration(testKey, testValue, time.Second)
	if err != nil {
		t.Fatalf("SetWithExpiration failed: %v", err)
	}


	value, ok := cache.Get(testKey)
	if !ok {
		t.Error("Value should exist immediately after setting")
	}

	if fv, ok := value.(fakeValue); ok && fv.data != "exp_value" {
		t.Errorf("Expected 'exp_value', got '%s'", fv.data)
	}


	time.Sleep(2 * time.Second)


	_, ok = cache.Get(testKey)
	if ok {
		t.Error("Value should be expired and not retrievable")
	}
}

func TestLRUCache_GetNonExistentKey(t *testing.T) {
	cache := newLRUCache(Options{
		MaxBytes: 100,
	})

	_, ok := cache.Get("non_existent_key")
	if ok {
		t.Error("Get should return false for non-existent keys")
	}
}

func TestLRUCache_SetNilValue(t *testing.T) {
	cache := newLRUCache(Options{
		MaxBytes: 100,
	})

	testKey := "nil_test"
	cache.Set(testKey, fakeValue{data: "some_value"})

	//  nil
	err := cache.Set(testKey, nil)
	if err != nil {
		t.Errorf("Setting nil value should not return error: %v", err)
	}

	_, ok := cache.Get(testKey)
	if ok {
		t.Error("Key should be deleted after setting nil value")
	}
}
