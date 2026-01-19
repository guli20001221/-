package main

import (
	"Group_Cache"
	"Group_Cache/pb"
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Local cluster helper: run as server or client without etcd.
	mode := flag.String("mode", "server", "server or client")
	port := flag.Int("port", 8001, "server listen port")
	node := flag.String("node", "A", "node name for display")
	addr := flag.String("addr", "", "grpc listen address for server")
	peers := flag.String("peers", "", "comma separated peer addresses")
	group := flag.String("group", "scores", "cache group name")

	target := flag.String("target", "127.0.0.1:8001", "grpc target for client")
	op := flag.String("op", "get", "client op: get|set|delete")
	key := flag.String("key", "k1", "cache key")
	value := flag.String("value", "v1", "cache value")
	flag.Parse()

	if *addr == "" {
		*addr = fmt.Sprintf("127.0.0.1:%d", *port)
	}

	switch *mode {
	case "server":
		if err := runServer(*node, *addr, *peers, *group); err != nil {
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

func runServer(node, addr, peers, groupName string) error {
	// Server bootstraps peers from a static list and serves gRPC.
	ctx, stop := signalContext()
	defer stop()

	g := Group_Cache.NewGroup(groupName, 1<<20, Group_Cache.GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return []byte("value-" + key), nil
	}))

	picker := Group_Cache.NewClientPicker(addr)
	peerAddrs := parsePeers(addr, peers)
	peerMap, err := buildPeers(addr, peerAddrs)
	if err != nil {
		return err
	}
	picker.SetPeers(peerMap)
	g.RegisterPeers(picker)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	fmt.Printf("node %s listening on %s\n", node, addr)
	err = Group_Cache.ServeGRPC(ctx, lis, Group_Cache.GRPCServerOptions{})
	_ = picker.Close()
	return err
}

func runClient(target, group, op, key, value string) error {
	// Client issues one request against a target node.
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

func parsePeers(self, peers string) []string {
	// parsePeers normalizes the peer list and ensures self is present.
	if strings.TrimSpace(peers) == "" {
		return []string{self}
	}
	parts := strings.Split(peers, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return []string{self}
	}
	return out
}

func buildPeers(self string, addrs []string) (map[string]Group_Cache.Peer, error) {
	// buildPeers wires LocalPeer for self and GRPCPeer for others.
	peers := make(map[string]Group_Cache.Peer, len(addrs))
	for _, addr := range addrs {
		if addr == self {
			peers[addr] = &Group_Cache.LocalPeer{}
			continue
		}
		peer, err := Group_Cache.NewGRPCPeer(addr)
		if err != nil {
			return nil, err
		}
		peers[addr] = peer
	}
	return peers, nil
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
