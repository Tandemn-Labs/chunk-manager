package observability

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/tandemn-labs/chunk-manager/internal/reconcile"
)

type RPCMetrics struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

func NewRPCMetrics(registerer prometheus.Registerer) (*RPCMetrics, error) {
	if registerer == nil {
		return nil, fmt.Errorf("Prometheus registerer is required")
	}

	metrics := &RPCMetrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "chunk_manager",
			Subsystem: "grpc_server",
			Name:      "requests_total",
			Help:      "Total number of unary gRPC requests.",
		}, []string{"method", "status"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "chunk_manager",
			Subsystem: "grpc_server",
			Name:      "request_duration_seconds",
			Help:      "Duration of unary gRPC requests in seconds.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"method", "status"}),
	}
	if err := registerer.Register(metrics.requests); err != nil {
		return nil, fmt.Errorf("register gRPC request counter: %w", err)
	}
	if err := registerer.Register(metrics.duration); err != nil {
		registerer.Unregister(metrics.requests)
		return nil, fmt.Errorf("register gRPC request duration histogram: %w", err)
	}
	return metrics, nil
}

func (metrics *RPCMetrics) UnaryServerInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	if logger == nil {
		logger = slog.Default()
	}

	return func(
		ctx context.Context,
		request any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (response any, err error) {
		started := time.Now()
		defer func() {
			if recovered := recover(); recovered != nil {
				logger.ErrorContext(
					ctx,
					"panic while handling gRPC request",
					slog.String("rpc_method", info.FullMethod),
					slog.Any("panic", recovered),
					slog.String("stack", string(debug.Stack())),
				)
				response = nil
				err = grpcstatus.Error(codes.Internal, "internal server error")
			}

			elapsed := time.Since(started)
			statusName := grpcstatus.Code(err).String()
			metrics.requests.WithLabelValues(info.FullMethod, statusName).Inc()
			metrics.duration.WithLabelValues(info.FullMethod, statusName).Observe(elapsed.Seconds())

			attributes := []slog.Attr{
				slog.String("rpc_method", info.FullMethod),
				slog.String("rpc_status", statusName),
				slog.Duration("duration", elapsed),
			}
			if err != nil {
				attributes = append(attributes, slog.Any("error", err))
			}
			logger.LogAttrs(ctx, slog.LevelInfo, "gRPC request completed", attributes...)
		}()

		return handler(ctx, request)
	}
}

type ReconcileMetrics struct {
	sweeps   *prometheus.CounterVec
	examined prometheus.Counter
	requeued prometheus.Counter
	deleted  prometheus.Counter
	errors   prometheus.Counter
}

func NewReconcileMetrics(registerer prometheus.Registerer) (*ReconcileMetrics, error) {
	if registerer == nil {
		return nil, fmt.Errorf("Prometheus registerer is required")
	}
	metrics := &ReconcileMetrics{
		sweeps: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "chunk_manager",
			Subsystem: "reconciliation",
			Name:      "sweeps_total",
			Help:      "Total number of draining-chain reconciliation sweeps.",
		}, []string{"result"}),
		examined: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "chunk_manager",
			Subsystem: "reconciliation",
			Name:      "associations_examined_total",
			Help:      "Total number of draining chain associations examined.",
		}),
		requeued: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "chunk_manager",
			Subsystem: "reconciliation",
			Name:      "chunks_requeued_total",
			Help:      "Total number of chunks requeued from draining chains.",
		}),
		deleted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "chunk_manager",
			Subsystem: "reconciliation",
			Name:      "associations_deleted_total",
			Help:      "Total number of draining chain associations deleted.",
		}),
		errors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "chunk_manager",
			Subsystem: "reconciliation",
			Name:      "association_errors_total",
			Help:      "Total number of per-association reconciliation errors.",
		}),
	}
	collectors := []prometheus.Collector{
		metrics.sweeps,
		metrics.examined,
		metrics.requeued,
		metrics.deleted,
		metrics.errors,
	}
	for _, collector := range collectors {
		if err := registerer.Register(collector); err != nil {
			return nil, fmt.Errorf("register reconciliation metric: %w", err)
		}
	}
	return metrics, nil
}

func (metrics *ReconcileMetrics) Observe(result reconcile.SweepResult, err error) {
	resultLabel := "success"
	if err != nil {
		resultLabel = "error"
	}
	metrics.sweeps.WithLabelValues(resultLabel).Inc()
	metrics.examined.Add(float64(result.Examined))
	metrics.requeued.Add(float64(result.Requeued))
	metrics.deleted.Add(float64(result.Deleted))
	metrics.errors.Add(float64(result.Errors))
}
