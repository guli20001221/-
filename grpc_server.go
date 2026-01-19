package Group_Cache

import (
	"Group_Cache/pb"
	"Group_Cache/registry"
	"context"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GRPCServer struct {
	pb.UnimplementedGroupCacheServer
}

func (s *GRPCServer) Get(ctx context.Context, req *pb.Request) (*pb.ResponseForGet, error) {
	group, key, err := validateRequest(req)
	if err != nil {
		return nil, err
	}
	g := GetGroup(group)
	if g == nil {
		return nil, status.Error(codes.NotFound, "group not found")
	}
	v, err := g.Get(ctx, key)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.ResponseForGet{Value: v.ByteSlice()}, nil
}

func (s *GRPCServer) Set(ctx context.Context, req *pb.Request) (*pb.ResponseForGet, error) {
	group, key, err := validateRequest(req)
	if err != nil {
		return nil, err
	}
	if len(req.GetValue()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "value is required")
	}
	g := GetGroup(group)
	if g == nil {
		return nil, status.Error(codes.NotFound, "group not found")
	}
	if err := g.Set(ctx, key, req.GetValue()); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.ResponseForGet{Value: req.GetValue()}, nil
}

func (s *GRPCServer) Delete(ctx context.Context, req *pb.Request) (*pb.ResponseForDelete, error) {
	group, key, err := validateRequest(req)
	if err != nil {
		return nil, err
	}
	g := GetGroup(group)
	if g == nil {
		return nil, status.Error(codes.NotFound, "group not found")
	}
	if err := g.Delete(ctx, key); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.ResponseForDelete{Value: true}, nil
}

type GRPCServerOptions struct {
	ServiceName string
	Registrar   registry.Registrar
}

// ServeGRPC starts a gRPC server and optionally registers it via the registrar.
func ServeGRPC(ctx context.Context, lis net.Listener, opts GRPCServerOptions) error {
	server := grpc.NewServer()
	pb.RegisterGroupCacheServer(server, &GRPCServer{})

	serviceName := opts.ServiceName
	if serviceName == "" {
		serviceName = defaultSvcName
	}
	if opts.Registrar != nil {
		if err := opts.Registrar.Register(ctx, serviceName, lis.Addr().String()); err != nil {
			return err
		}
		defer opts.Registrar.Deregister(context.Background(), serviceName, lis.Addr().String())
	}

	go func() {
		<-ctx.Done()
		server.GracefulStop()
	}()
	return server.Serve(lis)
}

// validateRequest performs basic argument checks and returns group/key.
func validateRequest(req *pb.Request) (string, string, error) {
	if req == nil {
		return "", "", status.Error(codes.InvalidArgument, "request is required")
	}
	if req.GetGroup() == "" {
		return "", "", status.Error(codes.InvalidArgument, "group is required")
	}
	if req.GetKey() == "" {
		return "", "", status.Error(codes.InvalidArgument, "key is required")
	}
	return req.GetGroup(), req.GetKey(), nil
}
