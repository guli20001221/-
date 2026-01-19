package main

import (
	"Group_Cache/pb"
	"context"
	"flag"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	targets := flag.String("targets", "127.0.0.1:9000", "comma separated grpc targets")
	group := flag.String("group", "scores", "cache group name")
	concurrency := flag.Int("c", 20, "concurrency")
	total := flag.Int("n", 1000, "total requests")
	mode := flag.String("mode", "load", "load or verify")
	timeout := flag.Duration("timeout", 2*time.Second, "per-request timeout")
	flag.Parse()

	addrs := splitList(*targets)
	if len(addrs) == 0 {
		fmt.Println("no targets")
		return
	}

	clients, err := newClients(addrs)
	if err != nil {
		fmt.Printf("dial error: %v\n", err)
		return
	}
	defer closeClients(clients)

	switch *mode {
	case "verify":
		if err := runVerify(clients, *group, *timeout); err != nil {
			fmt.Printf("verify failed: %v\n", err)
			return
		}
		fmt.Println("verify ok")
	default:
		runLoad(clients, *group, *total, *concurrency, *timeout)
	}
}

func runVerify(clients []pb.GroupCacheClient, group string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	client := clients[0]
	key := "verify_key"
	val := []byte("verify_value")
	if _, err := client.Set(ctx, &pb.Request{Group: group, Key: key, Value: val}); err != nil {
		return err
	}
	resp, err := client.Get(ctx, &pb.Request{Group: group, Key: key})
	if err != nil {
		return err
	}
	if string(resp.GetValue()) != string(val) {
		return fmt.Errorf("unexpected value: %s", string(resp.GetValue()))
	}
	if _, err := client.Delete(ctx, &pb.Request{Group: group, Key: key}); err != nil {
		return err
	}
	return nil
}

func runLoad(clients []pb.GroupCacheClient, group string, total, concurrency int, timeout time.Duration) {
	var okCount int64
	var errCount int64
	start := time.Now()

	var wg sync.WaitGroup
	ch := make(chan int, total)
	for i := 0; i < total; i++ {
		ch <- i
	}
	close(ch)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + seed))
			for range ch {
				client := clients[r.Intn(len(clients))]
				op := r.Intn(100)
				key := fmt.Sprintf("k%d", r.Intn(1000))
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				if op < 70 {
					if _, err := client.Get(ctx, &pb.Request{Group: group, Key: key}); err != nil {
						atomic.AddInt64(&errCount, 1)
					} else {
						atomic.AddInt64(&okCount, 1)
					}
				} else if op < 90 {
					val := []byte(fmt.Sprintf("v%d", r.Intn(1000)))
					if _, err := client.Set(ctx, &pb.Request{Group: group, Key: key, Value: val}); err != nil {
						atomic.AddInt64(&errCount, 1)
					} else {
						atomic.AddInt64(&okCount, 1)
					}
				} else {
					if _, err := client.Delete(ctx, &pb.Request{Group: group, Key: key}); err != nil {
						atomic.AddInt64(&errCount, 1)
					} else {
						atomic.AddInt64(&okCount, 1)
					}
				}
				cancel()
			}
		}(int64(i))
	}
	wg.Wait()

	elapsed := time.Since(start)
	fmt.Printf("done: ok=%d err=%d qps=%.2f\n", okCount, errCount, float64(okCount+errCount)/elapsed.Seconds())
}

func newClients(addrs []string) ([]pb.GroupCacheClient, error) {
	clients := make([]pb.GroupCacheClient, 0, len(addrs))
	for _, addr := range addrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		clients = append(clients, pb.NewGroupCacheClient(conn))
	}
	return clients, nil
}

func closeClients(clients []pb.GroupCacheClient) {
	for _, c := range clients {
		if cc, ok := c.(interface{ Close() error }); ok {
			_ = cc.Close()
		}
	}
}

func splitList(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
