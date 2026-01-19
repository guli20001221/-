package main

import (
	"Group_Cache"
	"Group_Cache/pb"
	etcdreg "Group_Cache/registry/etcd"
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// This example can run as a server or a client against gRPC + etcd.
	mode := flag.String("mode", "server", "server or client")
	addr := flag.String("addr", "127.0.0.1:9000", "grpc listen address for server")
	target := flag.String("target", "127.0.0.1:9000", "grpc target for client")
	etcdEndpoints := flag.String("etcd", "127.0.0.1:2379", "etcd endpoints, comma separated")
	service := flag.String("service", "GroupCache", "service name for discovery")
	group := flag.String("group", "scores", "cache group name")
	op := flag.String("op", "get", "client op: get|set|delete")
	key := flag.String("key", "k1", "cache key")
	value := flag.String("value", "v1", "cache value")
	flag.Parse()

	switch *mode {
	case "server":
		if err := runServer(*addr, *etcdEndpoints, *service, *group); err != nil {
			fmt.Printf("server error: %v\n", err)
			os.Exit(1)
		}
	case "client":
		if err := runClient(*target, *group, *op, *key, *value); err != nil {
			fmt.Printf("client error: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Println("invalid mode: use server or client")
		os.Exit(1)
	}
}

func runServer(addr, endpoints, service, groupName string) error {
	// Server registers itself in etcd and serves GroupCache over gRPC.
	cli, err := newEtcdClient(endpoints)
	if err != nil {
		return err
	}
	defer cli.Close()

	ctx, stop := signalContext()
	defer stop()

	g := Group_Cache.NewGroup(groupName, 1<<20, Group_Cache.GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return []byte("value-" + key), nil
	}))

	picker := Group_Cache.NewClientPicker(addr)
	if err := Group_Cache.WatchEtcd(ctx, cli, service, picker, grpc.WithTransportCredentials(insecure.NewCredentials())); err != nil {
		return err
	}
	g.RegisterPeers(picker)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	registrar := etcdreg.NewRegistrar(cli, 10*time.Second)
	return Group_Cache.ServeGRPC(ctx, lis, Group_Cache.GRPCServerOptions{
		ServiceName: service,
		Registrar:   registrar,
	})
}

func runClient(target, group, op, key, value string) error {
	// Client issues a single get/set/delete request against a target node.
	conn, err := grpc.Dial(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewGroupCacheClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	switch op {
	case "get":
		resp, err := client.Get(ctx, &pb.Request{Group: group, Key: key})
		if err != nil {
			return err
		}
		fmt.Printf("get %s => %s\n", key, string(resp.GetValue()))
	case "set":
		_, err := client.Set(ctx, &pb.Request{Group: group, Key: key, Value: []byte(value)})
		if err != nil {
			return err
		}
		fmt.Printf("set %s = %s\n", key, value)
	case "delete":
		resp, err := client.Delete(ctx, &pb.Request{Group: group, Key: key})
		if err != nil {
			return err
		}
		fmt.Printf("delete %s => %v\n", key, resp.GetValue())
	default:
		return fmt.Errorf("invalid op: %s", op)
	}
	return nil
}

func newEtcdClient(endpoints string) (*clientv3.Client, error) {
	// newEtcdClient parses the endpoint list and connects to etcd.
	parts := strings.Split(endpoints, ",")
	cfg := clientv3.Config{
		Endpoints:   parts,
		DialTimeout: 3 * time.Second,
	}
	return clientv3.New(cfg)
}

func signalContext() (context.Context, func()) {
	// signalContext cancels on SIGINT/SIGTERM for graceful shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-ch
		cancel()
	}()
	return ctx, func() {
		signal.Stop(ch)
		cancel()
	}
}
