package Group_Cache

import (
	"Group_Cache/pb"
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GRPCPeer struct {
	addr   string
	conn   *grpc.ClientConn
	client pb.GroupCacheClient
}

func NewGRPCPeer(addr string, opts ...grpc.DialOption) (*GRPCPeer, error) {
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}
	return &GRPCPeer{
		addr:   addr,
		conn:   conn,
		client: pb.NewGroupCacheClient(conn),
	}, nil
}

func (p *GRPCPeer) Get(ctx context.Context, group string, key string) ([]byte, error) {
	resp, err := p.client.Get(ctx, &pb.Request{Group: group, Key: key})
	if err != nil {
		return nil, err
	}
	return resp.GetValue(), nil
}

func (p *GRPCPeer) Set(ctx context.Context, group string, key string, value []byte) error {
	_, err := p.client.Set(ctx, &pb.Request{Group: group, Key: key, Value: value})
	return err
}

func (p *GRPCPeer) Delete(ctx context.Context, group string, key string) (bool, error) {
	resp, err := p.client.Delete(ctx, &pb.Request{Group: group, Key: key})
	if err != nil {
		return false, err
	}
	return resp.GetValue(), nil
}

func (p *GRPCPeer) Close() error {
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}
