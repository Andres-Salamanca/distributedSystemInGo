package server

import(
	"context"
	api "github.com/Andres-Salamanca/proglog/api/v1"
	"google.golang.org/grpc"
)


type Config struct {
	CommitLog CommitLog
}

// Ensure that grpcServer implements the api.LogServer interface.
var _ api.LogServer = (*grpcServer)(nil)

// grpcServer is the main struct representing the gRPC server.
type grpcServer struct {

	api.UnimplementedLogServer
	*Config

}

// newgrpcServer creates a new instance of grpcServer with the provided configuration.
func newgrpcServer(config *Config) (srv *grpcServer, err error) {

	srv = &grpcServer{
		Config: config,
	}

	return srv, nil
}

// Produce is the gRPC handler for producing log records.
func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (
	*api.ProduceResponse, error) {
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
	return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

// Consume is the gRPC handler for consuming log records.
func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest)(*api.ConsumeResponse, error) {

	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
	return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil

}
	