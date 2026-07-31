package stream

import (
	"context"
	"io"

	"google.golang.org/grpc"

	qrokv1 "qrok/internal/proto/qrok/v1"
	"qrok/internal/tunnel/grpcutil"
	"qrok/pkg/fault"
)

// ListenClient — gRPC ListenStream.
type ListenClient struct {
	conn   *grpc.ClientConn
	client qrokv1.TunnelServiceClient
}

func NewListenClient(ctx context.Context, gatewayAddr string) (*ListenClient, error) {
	conn, err := grpcutil.Dial(ctx, gatewayAddr)
	if err != nil {
		return nil, err
	}
	return &ListenClient{
		conn:   conn,
		client: qrokv1.NewTunnelServiceClient(conn),
	}, nil
}

func (c *ListenClient) Close() error {
	return c.conn.Close()
}

// Run открывает ListenStream и вызывает onEvent для каждого события.
// sendResult отправляет DeliveryResult обратно в облако.
func (c *ListenClient) Run(
	ctx context.Context,
	token string,
	sub *qrokv1.Subscribe,
	onEvent func(ctx context.Context, ev *qrokv1.EventEnvelope, sendResult func(*qrokv1.DeliveryResult) error) error,
) error {
	streamCtx := ctx
	if token != "" {
		streamCtx = grpcutil.WithBearer(ctx, token)
	}

	stream, err := c.client.ListenStream(streamCtx)
	if err != nil {
		return fault.ErrServiceUnavail.Wrap(err, "не удалось открыть ListenStream").WithOp("devcli.stream.open")
	}

	if err := stream.Send(&qrokv1.ListenStreamRequest{
		Msg: &qrokv1.ListenStreamRequest_Subscribe{Subscribe: sub},
	}); err != nil {
		return fault.ErrServiceUnavail.Wrap(err, "не удалось отправить subscribe").WithOp("devcli.stream.subscribe")
	}

	sendResult := func(result *qrokv1.DeliveryResult) error {
		return stream.Send(&qrokv1.ListenStreamRequest{
			Msg: &qrokv1.ListenStreamRequest_DeliveryResult{DeliveryResult: result},
		})
	}

	recvDone := make(chan error, 1)
	go func() {
		for {
			resp, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					recvDone <- nil
					return
				}
				recvDone <- fault.ErrServiceUnavail.Wrap(err, "ошибка чтения ListenStream").WithOp("devcli.stream.recv")
				return
			}
			ev := resp.GetEvent()
			if ev == nil {
				continue
			}
			if err := onEvent(ctx, ev, sendResult); err != nil {
				recvDone <- err
				return
			}
		}
	}()

	select {
	case <-ctx.Done():
		return nil
	case err := <-recvDone:
		return err
	}
}
