package observability

import (
	"context"
	"log/slog"
	"runtime/debug"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

func UnaryServerInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
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
