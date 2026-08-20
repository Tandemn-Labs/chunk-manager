package postgres

import (
	"context"
	"fmt"

	"github.com/oklog/ulid/v2"

	"github.com/tandemn-labs/chunk-manager/internal/postgres/db"
)

func (store *Store) ReconcileDrainingAssociation(
	ctx context.Context,
	jobID ulid.ULID,
	identity ChainIdentity,
) (ReconcileDrainingResult, error) {
	if identity.ChainID < 0 {
		return ReconcileDrainingResult{}, fmt.Errorf(
			"%w: chain ID cannot be negative",
			ErrInvalidArgument,
		)
	}

	result, err := withTransaction(ctx, store, func(queries *db.Queries) (ReconcileDrainingResult, error) {
		if _, err := queries.LockJob(ctx, dbUUID(jobID)); err != nil {
			return ReconcileDrainingResult{}, fmt.Errorf("lock job: %w", err)
		}

		key := chainKey{rankID: identity.RankID, chainID: identity.ChainID}
		associations, err := lockChainAssociations(ctx, queries, jobID, []chainKey{key})
		if err != nil {
			return ReconcileDrainingResult{}, err
		}
		association, exists := associations[key]
		if !exists {
			return ReconcileDrainingResult{}, nil
		}
		if association.State != db.ChainStateDRAINING {
			return ReconcileDrainingResult{}, fmt.Errorf(
				"%w: chain association is not draining",
				ErrInvalidState,
			)
		}

		discoveryTime, err := databaseTime(ctx, queries)
		if err != nil {
			return ReconcileDrainingResult{}, err
		}
		chainID := identity.ChainID
		candidateIDs, err := queries.FindExpiredDrainingChunkIDs(
			ctx,
			db.FindExpiredDrainingChunkIDsParams{
				JobID:          dbUUID(jobID),
				RankID:         nullableDBUUID(identity.RankID),
				ChainID:        &chainID,
				DiscoveryTime:  dbTimestamp(discoveryTime),
				CandidateLimit: store.reconcileBatchSize,
			},
		)
		if err != nil {
			return ReconcileDrainingResult{}, fmt.Errorf("find expired draining chunks: %w", err)
		}
		if len(candidateIDs) > 0 {
			if _, err := queries.LockChunks(ctx, db.LockChunksParams{
				JobID:    dbUUID(jobID),
				ChunkIds: candidateIDs,
			}); err != nil {
				return ReconcileDrainingResult{}, fmt.Errorf("lock expired draining chunks: %w", err)
			}
		}

		now, err := databaseTime(ctx, queries)
		if err != nil {
			return ReconcileDrainingResult{}, err
		}
		requeued := []int64{}
		if len(candidateIDs) > 0 {
			requeued, err = queries.RequeueExpiredDrainingChunks(
				ctx,
				db.RequeueExpiredDrainingChunksParams{
					DbTime:   dbTimestamp(now),
					JobID:    dbUUID(jobID),
					ChunkIds: candidateIDs,
					RankID:   nullableDBUUID(identity.RankID),
					ChainID:  &chainID,
				},
			)
			if err != nil {
				return ReconcileDrainingResult{}, fmt.Errorf("requeue draining chunks: %w", err)
			}
		}

		hasLeases, err := queries.ChainHasLeasedChunks(ctx, db.ChainHasLeasedChunksParams{
			JobID:   dbUUID(jobID),
			RankID:  nullableDBUUID(identity.RankID),
			ChainID: &chainID,
		})
		if err != nil {
			return ReconcileDrainingResult{}, fmt.Errorf("check draining chain leases: %w", err)
		}
		deleted := false
		if !hasLeases {
			rows, err := queries.DeleteDrainingChainAssociation(
				ctx,
				db.DeleteDrainingChainAssociationParams{
					JobID:   dbUUID(jobID),
					RankID:  dbUUID(identity.RankID),
					ChainID: identity.ChainID,
				},
			)
			if err != nil {
				return ReconcileDrainingResult{}, fmt.Errorf("delete draining chain association: %w", err)
			}
			deleted = rows == 1
		}

		return ReconcileDrainingResult{
			RequeuedChunkIDs:   requeued,
			AssociationDeleted: deleted,
		}, nil
	})
	return result, normalizeDatabaseError(err)
}
