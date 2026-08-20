package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	grpchealth "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	chunkmanagerv1 "github.com/tandemn-labs/chunk-manager/gen/go/tandemn/chunkmanager/v1"
	"github.com/tandemn-labs/chunk-manager/internal/api"
	"github.com/tandemn-labs/chunk-manager/internal/observability"
	"github.com/tandemn-labs/chunk-manager/internal/postgres"
	"github.com/tandemn-labs/chunk-manager/internal/reconcile"
)

const (
	applicationName        = "chunk-manager"
	livenessHealthService  = "liveness"
	readinessHealthService = "readiness"
)

type readinessState struct {
	mu           sync.Mutex
	server       *grpchealth.Server
	shuttingDown bool
}

func main() {
	cfg, err := loadConfig(os.LookupEnv)
	if err != nil {
		logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
		logger.Error("invalid configuration", slog.Any("error", err))
		os.Exit(1)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.logLevel}))
	slog.SetDefault(logger)
	if err := run(cfg, logger); err != nil {
		logger.Error("chunk manager stopped with an error", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(cfg config, logger *slog.Logger) error {
	signalContext, stopSignals := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stopSignals()

	pool, err := postgres.OpenPool(signalContext, postgres.PoolConfig{
		DatabaseURL:     cfg.databaseURL,
		ApplicationName: applicationName,
		MaxConnections:  cfg.postgresMaxConnections,
	})
	if err != nil {
		return fmt.Errorf("open PostgreSQL pool: %w", err)
	}
	closePool := true
	defer func() {
		if closePool {
			pool.Close()
		}
	}()
	if err := postgres.CheckReady(signalContext, pool); err != nil {
		return fmt.Errorf("check PostgreSQL readiness: %w", err)
	}

	store, err := postgres.NewStore(pool, postgres.StoreConfig{
		LockTimeout: cfg.storeLockTimeout,
	})
	if err != nil {
		return fmt.Errorf("create PostgreSQL store: %w", err)
	}
	runner, err := reconcile.NewRunner(store, logger, reconcile.Config{
		Interval:         cfg.reconcileInterval,
		OperationTimeout: cfg.reconcileOperationLimit,
	})
	if err != nil {
		return fmt.Errorf("create reconciliation runner: %w", err)
	}
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(observability.UnaryServerInterceptor(logger)))
	chunkmanagerv1.RegisterPlannerServiceServer(grpcServer, api.NewPlannerServer(store))
	chunkmanagerv1.RegisterWorkerServiceServer(grpcServer, api.NewWorkerServer(store))

	healthServer := grpchealth.NewServer()
	healthServer.SetServingStatus(livenessHealthService, healthpb.HealthCheckResponse_SERVING)
	readiness := &readinessState{server: healthServer}
	readiness.update(true)
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	reflection.Register(grpcServer)

	grpcListener, err := net.Listen("tcp", cfg.grpcListenAddr)
	if err != nil {
		return fmt.Errorf("listen for gRPC on %q: %w", cfg.grpcListenAddr, err)
	}

	backgroundContext, cancelBackground := context.WithCancel(context.Background())
	defer cancelBackground()
	componentErrors := make(chan error, 2)
	var workers sync.WaitGroup

	workers.Add(1)
	go func() {
		defer workers.Done()
		if err := grpcServer.Serve(grpcListener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			componentErrors <- fmt.Errorf("serve gRPC: %w", err)
		}
	}()

	workers.Add(1)
	go func() {
		defer workers.Done()
		if err := runner.Run(backgroundContext); err != nil && !errors.Is(err, context.Canceled) {
			componentErrors <- fmt.Errorf("run reconciliation: %w", err)
		}
	}()

	workers.Add(1)
	go func() {
		defer workers.Done()
		monitorReadiness(
			backgroundContext,
			cfg.healthCheckInterval,
			readiness,
			logger,
			func(ctx context.Context) error {
				return postgres.CheckReady(ctx, pool)
			},
		)
	}()

	logger.Info("chunk manager started", slog.String("grpc_listen_addr", cfg.grpcListenAddr))

	var runtimeError error
	select {
	case <-signalContext.Done():
		logger.Info("shutdown signal received")
	case runtimeError = <-componentErrors:
		logger.Error("runtime component failed", slog.Any("error", runtimeError))
	}
	stopSignals()

	readiness.beginShutdown()
	cancelBackground()

	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), cfg.shutdownTimeout)
	defer cancelShutdown()
	if !stopGRPCServer(shutdownContext, grpcServer, logger) {
		closePool = false
	}

	workersDone := make(chan struct{})
	go func() {
		workers.Wait()
		close(workersDone)
	}()
	select {
	case <-workersDone:
	case <-shutdownContext.Done():
		closePool = false
		logger.Warn("background work did not stop before the shutdown timeout")
	}

	logger.Info("chunk manager stopped")
	return runtimeError
}

func (state *readinessState) update(writable bool) {
	state.mu.Lock()
	defer state.mu.Unlock()

	if writable && state.shuttingDown {
		return
	}
	state.updateLocked(writable)
}

func (state *readinessState) beginShutdown() {
	state.mu.Lock()
	defer state.mu.Unlock()

	state.shuttingDown = true
	state.updateLocked(false)
}

func (state *readinessState) updateLocked(writable bool) {
	status := healthpb.HealthCheckResponse_NOT_SERVING
	if writable {
		status = healthpb.HealthCheckResponse_SERVING
	}
	for _, service := range []string{
		"",
		readinessHealthService,
		chunkmanagerv1.PlannerService_ServiceDesc.ServiceName,
		chunkmanagerv1.WorkerService_ServiceDesc.ServiceName,
	} {
		state.server.SetServingStatus(service, status)
	}
}

func monitorReadiness(
	ctx context.Context,
	interval time.Duration,
	readiness *readinessState,
	logger *slog.Logger,
	checkWritable func(context.Context) error,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	writable := true
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			checkContext, cancel := context.WithTimeout(ctx, interval)
			err := checkWritable(checkContext)
			cancel()
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				readiness.update(false)
				if writable {
					logger.ErrorContext(ctx, "PostgreSQL readiness check failed", slog.Any("error", err))
				}
				writable = false
				continue
			}

			readiness.update(true)
			if !writable {
				logger.InfoContext(ctx, "PostgreSQL readiness restored")
			}
			writable = true
		}
	}
}

func stopGRPCServer(
	ctx context.Context,
	grpcServer *grpc.Server,
	logger *slog.Logger,
) bool {
	grpcDone := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(grpcDone)
	}()

	select {
	case <-grpcDone:
		return true
	case <-ctx.Done():
		logger.Warn("graceful shutdown timed out; forcing gRPC server to stop")
		grpcServer.Stop()
		return false
	}
}
