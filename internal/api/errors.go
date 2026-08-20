package api

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tandemn-labs/chunk-manager/internal/postgres"
)

const errorDomain = "chunkmanager.tandemn.com"

func rpcError(err error) error {
	if err == nil {
		return nil
	}

	code := codes.Internal
	reason := "INTERNAL"
	message := "internal server error"

	switch {
	case errors.Is(err, context.Canceled):
		code, reason, message = codes.Canceled, "CANCELLED", "request cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		code, reason, message = codes.DeadlineExceeded, "DEADLINE_EXCEEDED", "request deadline exceeded"
	case errors.Is(err, postgres.ErrInvalidArgument):
		code, reason, message = codes.InvalidArgument, "INVALID_ARGUMENT", err.Error()
	case errors.Is(err, postgres.ErrNotFound):
		code, reason, message = codes.NotFound, "NOT_FOUND", err.Error()
	case errors.Is(err, postgres.ErrConflict):
		code, reason, message = codes.AlreadyExists, "CONFLICT", err.Error()
	case errors.Is(err, postgres.ErrRegistrationIncomplete):
		code, reason, message = codes.FailedPrecondition, "REGISTRATION_INCOMPLETE", err.Error()
	case errors.Is(err, postgres.ErrChainNotActive):
		code, reason, message = codes.FailedPrecondition, "CHAIN_NOT_ACTIVE", err.Error()
	case errors.Is(err, postgres.ErrStaleLease):
		code, reason, message = codes.FailedPrecondition, "STALE_LEASE", err.Error()
	case errors.Is(err, postgres.ErrLeaseExpired):
		code, reason, message = codes.FailedPrecondition, "LEASE_EXPIRED", err.Error()
	case errors.Is(err, postgres.ErrInvalidState):
		code, reason, message = codes.FailedPrecondition, "INVALID_STATE", err.Error()
	case errors.Is(err, postgres.ErrAborted):
		code, reason, message = codes.Aborted, "TRANSACTION_ABORTED", "transaction aborted"
	case errors.Is(err, postgres.ErrUnavailable):
		code, reason, message = codes.Unavailable, "POSTGRESQL_UNAVAILABLE", "PostgreSQL unavailable"
	}
	if code == codes.Internal || code == codes.Aborted || code == codes.Unavailable {
		slog.Default().Error(
			"gRPC operation failed",
			slog.String("grpc_code", code.String()),
			slog.Any("error", err),
		)
	}

	details := &errdetails.ErrorInfo{Reason: reason, Domain: errorDomain}
	withDetails, detailErr := status.New(code, message).WithDetails(details)
	if detailErr != nil {
		return status.Error(code, message)
	}
	return withDetails.Err()
}
