package grpcutil

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"

	"qrok/pkg/fault"
)

type Config struct {
	UseTLS     bool
	ServerName string
	CAFile     string
}

// Dial opens a gRPC connection with keepalive and optional dev-only plaintext.
func Dial(ctx context.Context, addr string, cfg Config) (*grpc.ClientConn, error) {
	if err := ctx.Err(); err != nil {
		return nil, fault.FromError(err)
	}
	transportCredentials, err := clientCredentials(cfg)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(transportCredentials),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, fault.ErrServiceUnavail.
			Wrap(err, "failed to connect to gateway").
			WithOp("grpc.dial").
			WithArg("addr", addr).
			WithHint("check that the server is running (make run-server)")
	}
	return conn, nil
}

func clientCredentials(cfg Config) (credentials.TransportCredentials, error) {
	if !cfg.UseTLS {
		return insecure.NewCredentials(), nil
	}

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: cfg.ServerName,
	}
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fault.ErrValidation.Wrap(err, "failed to read gRPC CA file").WithOp("grpc.tls")
		}
		roots, err := x509.SystemCertPool()
		if err != nil {
			return nil, fault.ErrInternal.Wrap(err, "failed to load system CA pool").WithOp("grpc.tls")
		}
		if !roots.AppendCertsFromPEM(pem) {
			return nil, fault.ErrValidation.New("gRPC CA file contains no certificates").WithOp("grpc.tls")
		}
		tlsConfig.RootCAs = roots
	}
	return credentials.NewTLS(tlsConfig), nil
}

// WithBearer добавляет authorization: Bearer <token> в исходящий контекст.
func WithBearer(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}
