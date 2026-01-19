package etcd

import (
	"context"
	"fmt"
	"path"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const basePath = "/group_cache"

// Registrar registers services in etcd using a TTL lease.
type Registrar struct {
	client *clientv3.Client
	ttl    time.Duration
}

func NewRegistrar(client *clientv3.Client, ttl time.Duration) *Registrar {
	if ttl <= 0 {
		ttl = 10 * time.Second
	}
	return &Registrar{client: client, ttl: ttl}
}

// Register creates a lease-backed key and keeps it alive.
func (r *Registrar) Register(ctx context.Context, service string, addr string) error {
	if r.client == nil {
		return fmt.Errorf("etcd client is nil")
	}
	key := serviceKey(service, addr)
	lease, err := r.client.Grant(ctx, int64(r.ttl.Seconds()))
	if err != nil {
		return err
	}
	if _, err := r.client.Put(ctx, key, addr, clientv3.WithLease(lease.ID)); err != nil {
		return err
	}
	ka, err := r.client.KeepAlive(context.Background(), lease.ID)
	if err != nil {
		return err
	}
	go func() {
		for range ka {
		}
	}()
	return nil
}

// Deregister removes the service entry from etcd.
func (r *Registrar) Deregister(ctx context.Context, service string, addr string) error {
	if r.client == nil {
		return fmt.Errorf("etcd client is nil")
	}
	_, err := r.client.Delete(ctx, serviceKey(service, addr))
	return err
}

// serviceKey builds the etcd key used for a service instance.
func serviceKey(service string, addr string) string {
	return path.Join(basePath, service, addr)
}

// ServicePrefix returns the etcd prefix for service discovery.
func ServicePrefix(service string) string {
	return path.Join(basePath, service) + "/"
}
