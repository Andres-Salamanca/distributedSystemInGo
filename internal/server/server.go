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
func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
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


// ProduceStream handles streaming production of log records.
func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	// Infinite loop to continuously handle incoming streaming requests.
	for {
		// Receive a ProduceRequest from the client stream.
		req, err := stream.Recv()
		if err != nil {
			// If an error occurs during reception (e.g., if the client closes the stream), return the error.
			return err
		}

		// Call the Produce method of the server to produce a log record based on the received request.
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			// If an error occurs during the production process, return the error.
			return err
		}

		// Send the produced response (ProduceResponse) back to the client via the stream.
		if err = stream.Send(res); err != nil {
			// If an error occurs during the sending process, return the error.
			return err
		}
	}
}

// ConsumeStream handles streaming consumption of log records.
func  (s *grpcServer) ConsumeStream(req *api.ConsumeRequest,stream api.Log_ConsumeStreamServer,)  error  {
	 for  {
			// Use a select statement to handle both streaming context cancellation and log consumption.
			 select  {
			 case  <-stream.Context().Done():
				// If the client's context is canceled (stream closed), return nil to terminate the streaming.
					 return  nil
			 default :

					res, err := s.Consume(stream.Context(), req)
					 switch  err.( type ) {
						// If no error occurred during log consumption, continue to the next iteration.
					 case  nil:
						// If the offset is out of range, continue to the next iteration without returning an error.
					 case  api.ErrOffsetOutOfRange:
							 continue
						// If an unexpected error occurs during log consumption, return the error.
					 default :
							 return  err
					}

					// Send the consumed log record back to the client via the stream.
					 if  err = stream.Send(res); err != nil {
							 return  err
					}
					req.Offset++
			}
	}
}
