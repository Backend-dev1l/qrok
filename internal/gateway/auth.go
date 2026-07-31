package gateway

import (
	"context"
	"strings"

	"google.golang.org/grpc/metadata"

	"qrok/pkg/fault"
)

const bearerPrefix = "bearer "

func bearerToken(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", fault.ErrUnauthorized.New("отсутствуют gRPC-metadata").WithOp("gateway.auth")
	}
	values := md.Get("authorization")
	if len(values) == 0 {
		return "", fault.ErrUnauthorized.New("отсутствует authorization").WithOp("gateway.auth")
	}
	raw := strings.TrimSpace(values[0])
	if len(raw) < len(bearerPrefix) || !strings.EqualFold(raw[:len(bearerPrefix)], bearerPrefix) {
		return "", fault.ErrUnauthorized.New("ожидается Bearer-токен").WithOp("gateway.auth")
	}
	token := strings.TrimSpace(raw[len(bearerPrefix):])
	if token == "" {
		return "", fault.ErrUnauthorized.New("пустой Bearer-токен").WithOp("gateway.auth")
	}
	return token, nil
}
