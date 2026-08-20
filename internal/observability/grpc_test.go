package observability

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

func TestUnaryServerInterceptorRecoversPanic(t *testing.T) {
	interceptor := UnaryServerInterceptor(slog.New(slog.NewTextHandler(io.Discard, nil)))

	response, err := interceptor(
		context.Background(),
		nil,
		&grpc.UnaryServerInfo{FullMethod: "/test.Service/Panic"},
		func(context.Context, any) (any, error) {
			panic("boom")
		},
	)

	if response != nil {
		t.Errorf("response = %v, want nil", response)
	}
	if code := grpcstatus.Code(err); code != codes.Internal {
		t.Errorf("status code = %s, want %s", code, codes.Internal)
	}
}
