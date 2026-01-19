package registry

import "context"

// Registrar defines service registration behavior (e.g. etcd).
type Registrar interface {
	Register(ctx context.Context, service string, addr string) error
	Deregister(ctx context.Context, service string, addr string) error
}

// NopRegistrar is a no-op registrar for local development.
type NopRegistrar struct{}

func (NopRegistrar) Register(ctx context.Context, service string, addr string) error   { return nil }
func (NopRegistrar) Deregister(ctx context.Context, service string, addr string) error { return nil }
