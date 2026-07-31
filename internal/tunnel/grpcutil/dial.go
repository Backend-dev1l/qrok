package grpcutil

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"qrok/pkg/fault"
)

// Dial открывает gRPC-соединение без TLS (dev/MVP).
func Dial(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fault.ErrServiceUnavail.
			Wrap(err, "не удалось подключиться к gateway").
			WithOp("grpc.dial").
			WithArg("addr", addr).
			WithHint("проверьте, что сервер запущен (make run-server)")
	}
	return conn, nil
}

// WithBearer добавляет authorization: Bearer <token> в исходящий контекст.
func WithBearer(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}
