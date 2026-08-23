package stream

import (
	"context"
	"io"
	"sync"

	"google.golang.org/grpc"

	qrokv1 "qrok/internal/proto/qrok/v1"
	"qrok/internal/tunnel/grpcutil"
	"qrok/pkg/fault"
)

// AgentClient — gRPC AgentStream с ожиданием Ack по event_id.
type AgentClient struct {
	conn   *grpc.ClientConn
	client qrokv1.TunnelServiceClient

	mu   sync.Mutex
	acks map[string]chan struct{}
}

func NewAgentClient(ctx context.Context, gatewayAddr, token string, tlsConfig grpcutil.Config) (*AgentClient, error) {
	conn, err := grpcutil.Dial(ctx, gatewayAddr, tlsConfig)
	if err != nil {
		return nil, err
	}
	return &AgentClient{
		conn:   conn,
		client: qrokv1.NewTunnelServiceClient(conn),
		acks:   make(map[string]chan struct{}),
	}, nil
}

func (c *AgentClient) Close() error {
	return c.conn.Close()
}

// RunSession открывает стрим, шлёт hello и обрабатывает события через send/waitAck.
func (c *AgentClient) RunSession(
	ctx context.Context,
	token string,
	hello *qrokv1.AgentHello,
	handle func(ctx context.Context, send func(context.Context, *qrokv1.EventEnvelope) error) error,
) error {
	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	streamCtx := grpcutil.WithBearer(sessionCtx, token)
	stream, err := c.client.AgentStream(streamCtx)
	if err != nil {
		return fault.ErrServiceUnavail.Wrap(err, "failed to open AgentStream").WithOp("agent.stream.open")
	}

	if err := stream.Send(&qrokv1.AgentStreamRequest{
		Msg: &qrokv1.AgentStreamRequest_Hello{Hello: hello},
	}); err != nil {
		return fault.ErrServiceUnavail.Wrap(err, "failed to send hello").WithOp("agent.stream.hello")
	}

	recvDone := make(chan error, 1)
	go func() {
		recvDone <- c.recvLoop(stream)
		cancel()
	}()

	send := func(sendCtx context.Context, ev *qrokv1.EventEnvelope) error {
		ack, unregister := c.registerAck(ev.GetEventId())
		defer unregister()

		if err := stream.Send(&qrokv1.AgentStreamRequest{
			Msg: &qrokv1.AgentStreamRequest_Event{Event: ev},
		}); err != nil {
			return fault.ErrServiceUnavail.Wrap(err, "failed to send event").WithOp("agent.stream.send")
		}
		return waitAck(sendCtx, ack)
	}

	err = handle(sessionCtx, send)

	_ = stream.CloseSend()
	select {
	case recvErr := <-recvDone:
		if err == nil && recvErr != nil && recvErr != io.EOF {
			return recvErr
		}
	default:
	}
	return err
}

func (c *AgentClient) recvLoop(stream grpc.BidiStreamingClient[qrokv1.AgentStreamRequest, qrokv1.AgentStreamResponse]) error {
	for {
		resp, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fault.ErrServiceUnavail.Wrap(err, "AgentStream read error").WithOp("agent.stream.recv")
		}
		if ack := resp.GetAck(); ack != nil {
			c.mu.Lock()
			if ch, ok := c.acks[ack.GetEventId()]; ok {
				select {
				case ch <- struct{}{}:
				default:
				}
			}
			c.mu.Unlock()
		}
	}
}

func (c *AgentClient) registerAck(eventID string) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	c.mu.Lock()
	c.acks[eventID] = ch
	c.mu.Unlock()

	return ch, func() {
		c.mu.Lock()
		delete(c.acks, eventID)
		c.mu.Unlock()
	}
}

func waitAck(ctx context.Context, ch <-chan struct{}) error {
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return fault.ErrTimeout.Wrap(ctx.Err(), "timeout waiting for ack from cloud").WithOp("agent.stream.wait_ack")
	}
}
