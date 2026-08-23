// Package server — облачная часть qrok: Tunnel Gateway + Control Plane.
package server

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"qrok/internal/bus/inproc"
	"qrok/internal/config"
	"qrok/internal/controlplane/infrastructure/auth"
	delivery "qrok/internal/controlplane/infrastructure/delivery"
	"qrok/internal/controlplane/infrastructure/eventstore"
	"qrok/internal/controlplane/service"
	qrokv1 "qrok/internal/proto/qrok/v1"
	gateway "qrok/internal/transport/grpc"
	httpapi "qrok/internal/transport/http"
	"qrok/pkg/fault"
	"qrok/pkg/logger"
	"qrok/pkg/objectstore"
	"qrok/pkg/postgres"
)

// Run запускает server: gRPC gateway + HTTP control plane.
func Run(configPath string) error {
	cfg, err := config.LoadServer(configPath)
	if err != nil {
		return err
	}

	log := logger.MustNew(os.Stdout, logger.Config{
		Level:  cfg.Log.Level,
		Format: cfg.Log.Format,
	})
	log.Info("config loaded",
		"http_addr", cfg.HTTP.Addr,
		"grpc_addr", cfg.GRPC.Addr,
		"payload_threshold_bytes", cfg.Events.PayloadThresholdBytes,
		"gateway_insecure_listen", cfg.Gateway.AllowInsecureListen,
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.New(ctx, cfg.Postgres.Pool())
	if err != nil {
		return err
	}
	defer pool.Close()

	objects, err := objectstore.New(ctx, objectstore.Config{
		Endpoint:  cfg.S3.Endpoint,
		Bucket:    cfg.S3.Bucket,
		AccessKey: cfg.S3.AccessKey,
		SecretKey: cfg.S3.SecretKey,
		UseSSL:    cfg.S3.UseSSL,
	})
	if err != nil {
		return err
	}

	eventRepo := eventstore.New(pool, objects, eventstore.Config{
		PayloadThresholdBytes: cfg.Events.PayloadThresholdBytes,
	})
	deliveryRepo := delivery.NewRepository(pool)
	authRepo := auth.NewRepository(pool)

	eventBus := inproc.New(256)
	authSvc := service.NewAuthService(authRepo)
	deviceSvc := service.NewDeviceService(service.DeviceAuthRepository(authRepo))
	eventSvc := service.NewEventService(eventRepo, deliveryRepo, authRepo)
	deliverySvc := service.NewDeliveryService(deliveryRepo, authRepo)
	replaySvc := service.NewReplayService(eventRepo, deliveryRepo, authRepo, eventBus)

	grpcServer := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             20 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	gw := gateway.NewServer(gateway.Config{
		AllowInsecureListen: cfg.Gateway.AllowInsecureListen,
		MaxPayloadBytes:     cfg.Events.MaxPayloadBytes,
	}, eventBus, eventSvc, authSvc, deliverySvc, log)
	qrokv1.RegisterTunnelServiceServer(grpcServer, gw)

	lis, err := net.Listen("tcp", cfg.GRPC.Addr)
	if err != nil {
		return fault.ErrServiceUnavail.
			Wrap(err, "failed to open gRPC port").
			WithOp("server.listen").
			WithArg("addr", cfg.GRPC.Addr)
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("gRPC gateway started", "addr", cfg.GRPC.Addr)
		errCh <- grpcServer.Serve(lis)
	}()

	httpHandler := httpapi.NewHandler(eventSvc, replaySvc, deviceSvc, authSvc, log)
	ready := func(ctx context.Context) error {
		if err := pool.Ping(ctx); err != nil {
			return err
		}
		return objects.Ping(ctx)
	}
	requestTimeout := time.Duration(cfg.HTTP.RequestTimeout)
	httpServer := &http.Server{
		Addr:              cfg.HTTP.Addr,
		Handler:           httpapi.NewRouter(cfg.HTTP, httpHandler, ready),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       requestTimeout + 5*time.Second,
		WriteTimeout:      requestTimeout + 5*time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		dashboardLocal, dashboardLAN := httpDashboardURLs(cfg.HTTP.Addr)
		attrs := []any{
			"addr", cfg.HTTP.Addr,
			"dashboard", dashboardLocal,
			"health", httpListenURL(cfg.HTTP.Addr) + "/health",
		}
		if dashboardLAN != "" {
			attrs = append(attrs, "dashboard_wsl", dashboardLAN)
		}
		log.Info("HTTP API started", attrs...)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Info("shutting down server...")
	case err := <-errCh:
		if err != nil {
			return fault.ErrInternal.Wrap(err, "server error").WithOp("server.run")
		}
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	_ = httpServer.Shutdown(shutdownCtx)

	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		grpcServer.Stop()
	}

	log.Info("server stopped")
	return nil
}

// httpListenURL превращает ":8080" в URL для браузера (WSL/Windows).
func httpListenURL(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "http://127.0.0.1" + addr
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port)
}
