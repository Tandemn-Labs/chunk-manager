package postgres

import (
	"context"
	"fmt"

	"github.com/oklog/ulid/v2"

	"github.com/tandemn-labs/chunk-manager/internal/postgres/db"
)

func (store *Store) AddChainAssociation(
	ctx context.Context,
	jobID ulid.ULID,
	identity ChainIdentity,
) (ChainAssociation, error) {
	if identity.ChainID < 0 {
		return ChainAssociation{}, fmt.Errorf("%w: chain ID cannot be negative", ErrInvalidArgument)
	}

	result, err := withTransaction(ctx, store, func(queries *db.Queries) (ChainAssociation, error) {
		job, err := queries.LockJob(ctx, dbUUID(jobID))
		if err != nil {
			return ChainAssociation{}, fmt.Errorf("lock job: %w", err)
		}
		key := chainKey{rankID: identity.RankID, chainID: identity.ChainID}
		associations, err := lockChainAssociations(ctx, queries, jobID, []chainKey{key})
		if err != nil {
			return ChainAssociation{}, err
		}
		if association, exists := associations[key]; exists {
			return chainFromDB(association), nil
		}
		if job.State != db.JobStatePENDING && job.State != db.JobStateRUNNING {
			return ChainAssociation{}, fmt.Errorf(
				"%w: chain cannot be associated with a terminal job",
				ErrInvalidState,
			)
		}

		now, err := databaseTime(ctx, queries)
		if err != nil {
			return ChainAssociation{}, err
		}
		row, err := queries.CreateChainAssociation(ctx, db.CreateChainAssociationParams{
			JobID:   dbUUID(jobID),
			RankID:  dbUUID(identity.RankID),
			ChainID: identity.ChainID,
			DbTime:  dbTimestamp(now),
		})
		if err != nil {
			return ChainAssociation{}, fmt.Errorf("create chain association: %w", err)
		}
		return chainFromDB(row), nil
	})
	return result, normalizeDatabaseError(err)
}

func (store *Store) DrainChainAssociation(
	ctx context.Context,
	jobID ulid.ULID,
	identity ChainIdentity,
) (ChainAssociation, error) {
	if identity.ChainID < 0 {
		return ChainAssociation{}, fmt.Errorf("%w: chain ID cannot be negative", ErrInvalidArgument)
	}

	result, err := withTransaction(ctx, store, func(queries *db.Queries) (ChainAssociation, error) {
		job, err := queries.LockJob(ctx, dbUUID(jobID))
		if err != nil {
			return ChainAssociation{}, fmt.Errorf("lock job: %w", err)
		}
		key := chainKey{rankID: identity.RankID, chainID: identity.ChainID}
		associations, err := lockChainAssociations(ctx, queries, jobID, []chainKey{key})
		if err != nil {
			return ChainAssociation{}, err
		}
		association, exists := associations[key]
		if !exists {
			return ChainAssociation{}, fmt.Errorf("%w: chain association", ErrNotFound)
		}
		if association.State == db.ChainStateDRAINING {
			return chainFromDB(association), nil
		}
		if job.State != db.JobStateRUNNING {
			return ChainAssociation{}, fmt.Errorf("%w: only a running job can drain a chain", ErrInvalidState)
		}

		now, err := databaseTime(ctx, queries)
		if err != nil {
			return ChainAssociation{}, err
		}
		updated, err := queries.MarkChainDraining(ctx, db.MarkChainDrainingParams{
			DbTime:  dbTimestamp(now),
			JobID:   dbUUID(jobID),
			RankID:  dbUUID(identity.RankID),
			ChainID: identity.ChainID,
		})
		if err != nil {
			return ChainAssociation{}, fmt.Errorf("mark chain draining: %w", err)
		}
		return chainFromDB(updated), nil
	})
	return result, normalizeDatabaseError(err)
}

func (store *Store) ListDrainingChainAssociations(
	ctx context.Context,
	cursor *ChainAssociationCursor,
	limit int32,
) ([]ChainAssociation, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("%w: result limit must be positive", ErrInvalidArgument)
	}
	params := db.ListDrainingChainAssociationsParams{ResultLimit: limit}
	if cursor != nil {
		if cursor.ChainID < 0 {
			return nil, fmt.Errorf("%w: cursor chain ID cannot be negative", ErrInvalidArgument)
		}
		params.HasCursor = true
		params.AfterJobID = dbUUID(cursor.JobID)
		params.AfterRankID = dbUUID(cursor.RankID)
		params.AfterChainID = cursor.ChainID
	}
	rows, err := db.New(store.pool).ListDrainingChainAssociations(ctx, params)
	if err != nil {
		return nil, normalizeDatabaseError(fmt.Errorf("list draining chain associations: %w", err))
	}
	result := make([]ChainAssociation, len(rows))
	for index, row := range rows {
		result[index] = chainFromDB(row)
	}
	return result, nil
}
