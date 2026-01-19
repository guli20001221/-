package Group_Cache

import (
	"context"
	"strings"

	etcdreg "Group_Cache/registry/etcd"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc"
)

// WatchEtcd keeps peer list in sync with etcd service registrations.
func WatchEtcd(ctx context.Context, client *clientv3.Client, service string, picker *ClientPicker, dialOpts ...grpc.DialOption) error {
	if client == nil || picker == nil {
		return nil
	}
	prefix := etcdreg.ServicePrefix(service)

	// update rebuilds the peer list from etcd keys.
	update := func(kvs []*mvccpb.KeyValue) {
		addrs := make([]string, 0, len(kvs))
		for _, kv := range kvs {
			addr := strings.TrimPrefix(string(kv.Key), prefix)
			if addr != "" {
				addrs = append(addrs, addr)
			}
		}
		picker.UpdatePeers(addrs, func(addr string) (Peer, error) {
			return NewGRPCPeer(addr, dialOpts...)
		})
	}

	resp, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	update(resp.Kvs)

	// Watch for changes and refresh the peer set.
	watchCh := client.Watch(ctx, prefix, clientv3.WithPrefix())
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case wresp, ok := <-watchCh:
				if !ok {
					return
				}
				if wresp.Err() != nil {
					continue
				}
				resp, err := client.Get(ctx, prefix, clientv3.WithPrefix())
				if err != nil {
					continue
				}
				update(resp.Kvs)
			}
		}
	}()
	return nil
}
