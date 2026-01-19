package Group_Cache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGroupGetterOnMiss(t *testing.T) {
	var calls int64
	getter := GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		atomic.AddInt64(&calls, 1)
		return []byte("v1"), nil
	})

	g := NewGroup("test_group_miss", 1024, getter)
	defer g.Close()

	v, err := g.Get(context.Background(), "k1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got := v.String(); got != "v1" {
		t.Fatalf("unexpected value: %s", got)
	}

	v, err = g.Get(context.Background(), "k1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got := v.String(); got != "v1" {
		t.Fatalf("unexpected value: %s", got)
	}

	if n := atomic.LoadInt64(&calls); n != 1 {
		t.Fatalf("getter calls = %d, want 1", n)
	}
}

func TestGroupSingleflight(t *testing.T) {
	var calls int64
	getter := GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		atomic.AddInt64(&calls, 1)
		time.Sleep(50 * time.Millisecond)
		return []byte("v2"), nil
	})

	g := NewGroup("test_group_sf", 1024, getter)
	defer g.Close()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := g.Get(context.Background(), "k1"); err != nil {
				t.Errorf("Get failed: %v", err)
			}
		}()
	}
	wg.Wait()

	if n := atomic.LoadInt64(&calls); n != 1 {
		t.Fatalf("getter calls = %d, want 1", n)
	}
}
