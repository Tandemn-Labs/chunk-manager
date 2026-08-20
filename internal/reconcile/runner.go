package reconcile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/tandemn-labs/chunk-manager/internal/postgres"
)

const (
	defaultInterval         = 30 * time.Second
	defaultPageSize         = int32(100)
	defaultOperationTimeout = 10 * time.Second
)

type Store interface {
	ListDrainingChainAssociations(
		context.Context,
		*postgres.ChainAssociationCursor,
		int32,
	) ([]postgres.ChainAssociation, error)
	ReconcileDrainingAssociation(
		context.Context,
		ulid.ULID,
		postgres.ChainIdentity,
	) (postgres.ReconcileDrainingResult, error)
}

type Config struct {
	Interval         time.Duration
	PageSize         int32
	OperationTimeout time.Duration
}

type SweepResult struct {
	Examined int
	Requeued int
	Deleted  int
	Errors   int
}

type Runner struct {
	store            Store
	logger           *slog.Logger
	interval         time.Duration
	pageSize         int32
	operationTimeout time.Duration
}

func NewRunner(store Store, logger *slog.Logger, config Config) (*Runner, error) {
	if store == nil {
		return nil, errors.New("reconciliation store is required")
	}
	if config.Interval < 0 {
		return nil, errors.New("reconciliation interval cannot be negative")
	}
	if config.PageSize < 0 {
		return nil, errors.New("reconciliation page size cannot be negative")
	}
	if config.OperationTimeout < 0 {
		return nil, errors.New("reconciliation operation timeout cannot be negative")
	}

	if config.Interval == 0 {
		config.Interval = defaultInterval
	}
	if config.PageSize == 0 {
		config.PageSize = defaultPageSize
	}
	if config.OperationTimeout == 0 {
		config.OperationTimeout = defaultOperationTimeout
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Runner{
		store:            store,
		logger:           logger,
		interval:         config.Interval,
		pageSize:         config.PageSize,
		operationTimeout: config.OperationTimeout,
	}, nil
}

func (runner *Runner) Run(ctx context.Context) error {
	runner.runSweep(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}

	ticker := time.NewTicker(runner.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			runner.runSweep(ctx)
			if err := ctx.Err(); err != nil {
				return err
			}
		}
	}
}

func (runner *Runner) Sweep(ctx context.Context) (SweepResult, error) {
	var result SweepResult
	var cursor *postgres.ChainAssociationCursor

	for {
		associations, err := runner.store.ListDrainingChainAssociations(ctx, cursor, runner.pageSize)
		if err != nil {
			return result, fmt.Errorf("list draining chain associations: %w", err)
		}
		if len(associations) == 0 {
			return result, nil
		}

		for _, association := range associations {
			result.Examined++

			operationCtx, cancel := context.WithTimeout(ctx, runner.operationTimeout)
			reconcileResult, err := runner.store.ReconcileDrainingAssociation(
				operationCtx,
				association.JobID,
				postgres.ChainIdentity{
					RankID:  association.RankID,
					ChainID: association.ChainID,
				},
			)
			cancel()
			if err != nil {
				result.Errors++
				runner.logger.ErrorContext(
					ctx,
					"failed to reconcile draining chain association",
					slog.String("job_id", association.JobID.String()),
					slog.String("rank_id", association.RankID.String()),
					slog.Int64("chain_id", association.ChainID),
					slog.Any("error", err),
				)
				continue
			}

			result.Requeued += len(reconcileResult.RequeuedChunkIDs)
			if reconcileResult.AssociationDeleted {
				result.Deleted++
			}
		}

		last := associations[len(associations)-1]
		cursor = &postgres.ChainAssociationCursor{
			JobID:   last.JobID,
			RankID:  last.RankID,
			ChainID: last.ChainID,
		}
		if len(associations) < int(runner.pageSize) {
			return result, nil
		}
	}
}

func (runner *Runner) runSweep(ctx context.Context) {
	result, err := runner.Sweep(ctx)
	attributes := []any{
		slog.Int("examined", result.Examined),
		slog.Int("requeued", result.Requeued),
		slog.Int("deleted", result.Deleted),
		slog.Int("errors", result.Errors),
	}
	if err != nil {
		attributes = append(attributes, slog.Any("error", err))
		runner.logger.ErrorContext(ctx, "draining chain reconciliation sweep failed", attributes...)
		return
	}
	runner.logger.InfoContext(ctx, "draining chain reconciliation sweep completed", attributes...)
}
