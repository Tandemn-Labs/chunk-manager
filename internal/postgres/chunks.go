package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/oklog/ulid/v2"

	"github.com/tandemn-labs/chunk-manager/internal/postgres/db"
)

func (store *Store) ClaimChunks(ctx context.Context, params ClaimChunksParams) ([]Lease, error) {
	if params.ChainID < 0 {
		return nil, fmt.Errorf("%w: chain ID cannot be negative", ErrInvalidArgument)
	}
	if params.MaxChunks <= 0 || params.MaxChunks > store.maxClaimChunks {
		return nil, fmt.Errorf(
			"%w: max chunks must be between 1 and %d",
			ErrInvalidArgument,
			store.maxClaimChunks,
		)
	}

	leases, err := withTransaction(ctx, store, func(queries *db.Queries) ([]Lease, error) {
		job, err := queries.LockJob(ctx, dbUUID(params.JobID))
		if err != nil {
			return nil, fmt.Errorf("lock job: %w", err)
		}
		if job.State != db.JobStateRUNNING {
			return nil, fmt.Errorf("%w: job is not running", ErrInvalidState)
		}

		discoveryTime, err := databaseTime(ctx, queries)
		if err != nil {
			return nil, err
		}
		candidates, err := queries.FindClaimCandidates(ctx, db.FindClaimCandidatesParams{
			JobID:          dbUUID(params.JobID),
			DiscoveryTime:  dbTimestamp(discoveryTime),
			CandidateLimit: params.MaxChunks,
		})
		if err != nil {
			return nil, fmt.Errorf("find claim candidates: %w", err)
		}

		targetKey := chainKey{rankID: params.RankID, chainID: params.ChainID}
		associationKeys := []chainKey{targetKey}
		chunkIDs := make([]int64, len(candidates))

		// Collect chainKey(s) of the previous chains of the chunks in the list
		// Only add it if there is a valid rank_id + chain_id
		// If those fields are not there, the chunk can be in READY state without prev owner
		for index, candidate := range candidates {
			chunkIDs[index] = candidate.ChunkID
			if key, ok := chainKeyFromLeaseFields(
				candidate.CurrentRankID,
				candidate.CurrentChainID,
			); ok {
				associationKeys = append(associationKeys, key)
			}
		}

		associations, err := lockChainAssociations(
			ctx,
			queries,
			params.JobID,
			associationKeys,
		)
		if err != nil {
			return nil, err
		}
		targetAssociation, exists := associations[targetKey]
		if !exists {
			return nil, fmt.Errorf("%w: claiming chain association", ErrNotFound)
		}
		if targetAssociation.State != db.ChainStateACTIVE {
			return nil, ErrChainNotActive
		}

		// This section onwards actually locks and work on chunks
		lockedChunks, err := queries.LockChunks(ctx, db.LockChunksParams{
			JobID:    dbUUID(params.JobID),
			ChunkIds: chunkIDs,
		})
		if err != nil {
			return nil, fmt.Errorf("lock claim candidates: %w", err)
		}
		chunksByID := make(map[int64]db.Chunk, len(lockedChunks))
		for _, chunk := range lockedChunks {
			chunksByID[chunk.ChunkID] = chunk
		}

		now, err := databaseTime(ctx, queries)
		if err != nil {
			return nil, err
		}
		expiresAt := now.Add(time.Duration(job.LeaseDurationMs) * time.Millisecond)
		result := make([]Lease, 0, params.MaxChunks)

		for _, candidate := range candidates {
			if len(result) == int(params.MaxChunks) {
				break
			}
			chunk, exists := chunksByID[candidate.ChunkID]
			if !exists {
				continue
			}

			retryCount := chunk.RetryCount
			recordExpiration := false
			switch chunk.State {
			case db.ChunkStateREADY:
				if !chunk.NotBefore.Valid || chunk.NotBefore.Time.After(now) {
					continue
				}
			case db.ChunkStateLEASED:
				if !chunk.LeaseExpiresAt.Valid || chunk.LeaseExpiresAt.Time.After(now) {
					continue
				}
				sourceKey, ok := chainKeyFromChunk(chunk)
				if !ok {
					return nil, fmt.Errorf("%w: leased chunk has no owner", ErrInvalidState)
				}
				sourceAssociation, exists := associations[sourceKey]
				if !exists {
					return nil, fmt.Errorf("%w: current chain association", ErrNotFound)
				}
				if sourceAssociation.State == db.ChainStateACTIVE {
					retryCount++
					recordExpiration = true
					if chunk.RetryCount >= job.MaxRetries {
						failureClass := "LEASE_EXPIRED"
						failureMessage := "active chain lease expired"
						_, err := queries.FailChunk(ctx, db.FailChunkParams{
							RetryCount:     retryCount,
							FailureClass:   &failureClass,
							FailureMessage: &failureMessage,
							DbTime:         dbTimestamp(now),
							JobID:          dbUUID(params.JobID),
							ChunkID:        chunk.ChunkID,
						})
						if err != nil {
							return nil, fmt.Errorf("fail exhausted chunk: %w", err)
						}
						job, err = queries.RecordJobChunkFailed(ctx, db.RecordJobChunkFailedParams{
							DbTime: dbTimestamp(now),
							JobID:  dbUUID(params.JobID),
						})
						if err != nil {
							return nil, fmt.Errorf("record exhausted chunk: %w", err)
						}
						continue
					}
				}
			case db.ChunkStateSUCCEEDED, db.ChunkStateFAILED, db.ChunkStateCANCELLED:
				continue
			default:
				return nil, fmt.Errorf("%w: unknown chunk state %q", ErrInvalidState, chunk.State)
			}

			chainID := params.ChainID
			assigned, err := queries.AssignChunk(ctx, db.AssignChunkParams{
				RankID:           nullableDBUUID(params.RankID),
				ChainID:          &chainID,
				LeaseExpiresAt:   dbTimestamp(expiresAt),
				RetryCount:       retryCount,
				RecordExpiration: recordExpiration,
				DbTime:           dbTimestamp(now),
				JobID:            dbUUID(params.JobID),
				ChunkID:          chunk.ChunkID,
			})
			if err != nil {
				return nil, fmt.Errorf("assign chunk %d: %w", chunk.ChunkID, err)
			}
			lease, err := leaseFromChunk(assigned)
			if err != nil {
				return nil, err
			}
			result = append(result, lease)
		}

		return result, nil
	})
	return leases, normalizeDatabaseError(err)
}

func (store *Store) RenewLeases(
	ctx context.Context,
	params RenewLeasesParams,
) (RenewLeasesResult, error) {
	if params.ChainID < 0 {
		return RenewLeasesResult{}, fmt.Errorf("%w: chain ID cannot be negative", ErrInvalidArgument)
	}
	if len(params.Leases) == 0 || len(params.Leases) > store.maxRenewLeases {
		return RenewLeasesResult{}, fmt.Errorf(
			"%w: renewal batch must contain between 1 and %d leases",
			ErrInvalidArgument,
			store.maxRenewLeases,
		)
	}
	seen := make(map[int64]struct{}, len(params.Leases))
	chunkIDs := make([]int64, len(params.Leases))
	generations := make([]int64, len(params.Leases))
	for index, lease := range params.Leases {
		if lease.ChunkID < 0 || lease.Generation <= 0 {
			return RenewLeasesResult{}, fmt.Errorf("%w: invalid lease reference", ErrInvalidArgument)
		}
		if _, exists := seen[lease.ChunkID]; exists {
			return RenewLeasesResult{}, fmt.Errorf(
				"%w: duplicate chunk ID %d",
				ErrInvalidArgument,
				lease.ChunkID,
			)
		}
		seen[lease.ChunkID] = struct{}{}
		chunkIDs[index] = lease.ChunkID
		generations[index] = lease.Generation
	}

	result, err := withTransaction(ctx, store, func(queries *db.Queries) (RenewLeasesResult, error) {
		job, err := queries.LockJob(ctx, dbUUID(params.JobID))
		if err != nil {
			return RenewLeasesResult{}, fmt.Errorf("lock job: %w", err)
		}
		if job.State != db.JobStateRUNNING {
			return RenewLeasesResult{}, fmt.Errorf("%w: job is not running", ErrInvalidState)
		}

		discovered, err := queries.GetChunks(ctx, db.GetChunksParams{
			JobID:    dbUUID(params.JobID),
			ChunkIds: chunkIDs,
		})
		if err != nil {
			return RenewLeasesResult{}, fmt.Errorf("get renewal chunks: %w", err)
		}
		targetKey := chainKey{rankID: params.RankID, chainID: params.ChainID}
		associationKeys := relevantChainKeys(targetKey, discovered)
		associations, err := lockChainAssociations(
			ctx,
			queries,
			params.JobID,
			associationKeys,
		)
		if err != nil {
			return RenewLeasesResult{}, err
		}
		association, exists := associations[targetKey]
		if !exists {
			return RenewLeasesResult{}, fmt.Errorf("%w: renewing chain association", ErrNotFound)
		}
		if association.State != db.ChainStateACTIVE && association.State != db.ChainStateDRAINING {
			return RenewLeasesResult{}, fmt.Errorf("%w: chain cannot renew leases", ErrInvalidState)
		}
		if _, err := queries.LockChunks(ctx, db.LockChunksParams{
			JobID:    dbUUID(params.JobID),
			ChunkIds: chunkIDs,
		}); err != nil {
			return RenewLeasesResult{}, fmt.Errorf("lock renewal chunks: %w", err)
		}

		now, err := databaseTime(ctx, queries)
		if err != nil {
			return RenewLeasesResult{}, err
		}
		expiresAt := now.Add(time.Duration(job.LeaseDurationMs) * time.Millisecond)
		chainID := params.ChainID
		renewedRows, err := queries.RenewMatchingLeases(ctx, db.RenewMatchingLeasesParams{
			LeaseExpiresAt:   dbTimestamp(expiresAt),
			DbTime:           dbTimestamp(now),
			JobID:            dbUUID(params.JobID),
			RankID:           nullableDBUUID(params.RankID),
			ChainID:          &chainID,
			ChunkIds:         chunkIDs,
			LeaseGenerations: generations,
		})
		if err != nil {
			return RenewLeasesResult{}, fmt.Errorf("renew leases: %w", err)
		}

		renewedByChunk := make(map[int64]db.RenewMatchingLeasesRow, len(renewedRows))
		for _, row := range renewedRows {
			renewedByChunk[row.ChunkID] = row
		}
		response := RenewLeasesResult{
			Renewed: make([]RenewedLease, 0, len(renewedRows)),
			Stale:   make([]LeaseReference, 0, len(params.Leases)-len(renewedRows)),
		}
		for _, requested := range params.Leases {
			row, renewed := renewedByChunk[requested.ChunkID]
			if !renewed {
				response.Stale = append(response.Stale, requested)
				continue
			}
			response.Renewed = append(response.Renewed, RenewedLease{
				ChunkID:    row.ChunkID,
				Generation: row.LeaseGeneration,
				ExpiresAt:  row.LeaseExpiresAt.Time,
			})
		}
		return response, nil
	})
	return result, normalizeDatabaseError(err)
}

func (store *Store) CompleteChunk(
	ctx context.Context,
	params CompleteChunkParams,
) (CompleteChunkResult, error) {
	if params.ChainID < 0 || params.ChunkID < 0 || params.Generation <= 0 {
		return CompleteChunkResult{}, fmt.Errorf("%w: invalid lease identity", ErrInvalidArgument)
	}
	if strings.TrimSpace(params.OutputURI) == "" || strings.TrimSpace(params.Checksum) == "" {
		return CompleteChunkResult{}, fmt.Errorf("%w: output URI and checksum are required", ErrInvalidArgument)
	}
	if params.OutputSize < 0 {
		return CompleteChunkResult{}, fmt.Errorf("%w: output size cannot be negative", ErrInvalidArgument)
	}

	result, err := withTransaction(ctx, store, func(queries *db.Queries) (CompleteChunkResult, error) {
		job, err := queries.LockJob(ctx, dbUUID(params.JobID))
		if err != nil {
			return CompleteChunkResult{}, fmt.Errorf("lock job: %w", err)
		}
		discovered, err := queries.GetChunks(ctx, db.GetChunksParams{
			JobID:    dbUUID(params.JobID),
			ChunkIds: []int64{params.ChunkID},
		})
		if err != nil {
			return CompleteChunkResult{}, fmt.Errorf("get completion chunk: %w", err)
		}
		if len(discovered) == 0 {
			return CompleteChunkResult{}, fmt.Errorf("%w: chunk", ErrNotFound)
		}

		requestKey := chainKey{rankID: params.RankID, chainID: params.ChainID}
		associations := map[chainKey]db.JobChainAssociation{}
		if discovered[0].State == db.ChunkStateLEASED {
			associations, err = lockChainAssociations(
				ctx,
				queries,
				params.JobID,
				relevantChainKeys(requestKey, discovered),
			)
			if err != nil {
				return CompleteChunkResult{}, err
			}
		}
		locked, err := queries.LockChunks(ctx, db.LockChunksParams{
			JobID:    dbUUID(params.JobID),
			ChunkIds: []int64{params.ChunkID},
		})
		if err != nil {
			return CompleteChunkResult{}, fmt.Errorf("lock completion chunk: %w", err)
		}
		if len(locked) == 0 {
			return CompleteChunkResult{}, fmt.Errorf("%w: chunk", ErrNotFound)
		}
		now, err := databaseTime(ctx, queries)
		if err != nil {
			return CompleteChunkResult{}, err
		}
		chunk := locked[0]

		if chunk.State == db.ChunkStateSUCCEEDED {
			if completionMatches(chunk, params) {
				return CompleteChunkResult{JobState: JobState(job.State), Replay: true}, nil
			}
			if chunk.LeaseGeneration == params.Generation {
				return CompleteChunkResult{}, fmt.Errorf("%w: committed output differs", ErrConflict)
			}
			return CompleteChunkResult{}, ErrStaleLease
		}
		if err := validateCurrentLease(job, chunk, requestKey, params.Generation, associations, now); err != nil {
			return CompleteChunkResult{}, err
		}

		outputURI := params.OutputURI
		checksum := params.Checksum
		outputSize := params.OutputSize
		if _, err := queries.SucceedChunk(ctx, db.SucceedChunkParams{
			OutputUri:       &outputURI,
			OutputChecksum:  &checksum,
			OutputSizeBytes: &outputSize,
			DbTime:          dbTimestamp(now),
			JobID:           dbUUID(params.JobID),
			ChunkID:         params.ChunkID,
		}); err != nil {
			return CompleteChunkResult{}, fmt.Errorf("complete chunk: %w", err)
		}
		updatedJob, err := queries.RecordJobChunkSucceeded(ctx, db.RecordJobChunkSucceededParams{
			DbTime: dbTimestamp(now),
			JobID:  dbUUID(params.JobID),
		})
		if err != nil {
			return CompleteChunkResult{}, fmt.Errorf("record completed chunk: %w", err)
		}
		return CompleteChunkResult{JobState: JobState(updatedJob.State)}, nil
	})
	return result, normalizeDatabaseError(err)
}

func (store *Store) FailChunk(ctx context.Context, params FailChunkParams) (FailChunkResult, error) {
	if params.ChainID < 0 || params.ChunkID < 0 || params.Generation <= 0 {
		return FailChunkResult{}, fmt.Errorf("%w: invalid lease identity", ErrInvalidArgument)
	}
	if strings.TrimSpace(params.FailureClass) == "" {
		return FailChunkResult{}, fmt.Errorf("%w: failure class is required", ErrInvalidArgument)
	}

	result, err := withTransaction(ctx, store, func(queries *db.Queries) (FailChunkResult, error) {
		job, err := queries.LockJob(ctx, dbUUID(params.JobID))
		if err != nil {
			return FailChunkResult{}, fmt.Errorf("lock job: %w", err)
		}
		discovered, err := queries.GetChunks(ctx, db.GetChunksParams{
			JobID:    dbUUID(params.JobID),
			ChunkIds: []int64{params.ChunkID},
		})
		if err != nil {
			return FailChunkResult{}, fmt.Errorf("get failed chunk: %w", err)
		}
		if len(discovered) == 0 {
			return FailChunkResult{}, fmt.Errorf("%w: chunk", ErrNotFound)
		}

		requestKey := chainKey{rankID: params.RankID, chainID: params.ChainID}
		associations := map[chainKey]db.JobChainAssociation{}
		if discovered[0].State == db.ChunkStateLEASED {
			associations, err = lockChainAssociations(
				ctx,
				queries,
				params.JobID,
				relevantChainKeys(requestKey, discovered),
			)
			if err != nil {
				return FailChunkResult{}, err
			}
		}
		locked, err := queries.LockChunks(ctx, db.LockChunksParams{
			JobID:    dbUUID(params.JobID),
			ChunkIds: []int64{params.ChunkID},
		})
		if err != nil {
			return FailChunkResult{}, fmt.Errorf("lock failed chunk: %w", err)
		}
		if len(locked) == 0 {
			return FailChunkResult{}, fmt.Errorf("%w: chunk", ErrNotFound)
		}
		now, err := databaseTime(ctx, queries)
		if err != nil {
			return FailChunkResult{}, err
		}
		chunk := locked[0]
		if err := validateCurrentLease(job, chunk, requestKey, params.Generation, associations, now); err != nil {
			return FailChunkResult{}, err
		}

		failureClass := params.FailureClass
		failureMessage := params.Message
		if params.Retriable && chunk.RetryCount < job.MaxRetries {
			retryCount := chunk.RetryCount + 1
			notBefore := now.Add(time.Duration(job.RetryBackoffMs) * time.Millisecond)
			if _, err := queries.RetryChunk(ctx, db.RetryChunkParams{
				RetryCount:     retryCount,
				NotBefore:      dbTimestamp(notBefore),
				FailureClass:   &failureClass,
				FailureMessage: &failureMessage,
				DbTime:         dbTimestamp(now),
				JobID:          dbUUID(params.JobID),
				ChunkID:        params.ChunkID,
			}); err != nil {
				return FailChunkResult{}, fmt.Errorf("retry chunk: %w", err)
			}
			return FailChunkResult{
				JobState:  JobState(job.State),
				Retried:   true,
				NotBefore: &notBefore,
			}, nil
		}

		retryCount := chunk.RetryCount
		if params.Retriable {
			retryCount++
		}
		if _, err := queries.FailChunk(ctx, db.FailChunkParams{
			RetryCount:     retryCount,
			FailureClass:   &failureClass,
			FailureMessage: &failureMessage,
			DbTime:         dbTimestamp(now),
			JobID:          dbUUID(params.JobID),
			ChunkID:        params.ChunkID,
		}); err != nil {
			return FailChunkResult{}, fmt.Errorf("fail chunk: %w", err)
		}
		updatedJob, err := queries.RecordJobChunkFailed(ctx, db.RecordJobChunkFailedParams{
			DbTime: dbTimestamp(now),
			JobID:  dbUUID(params.JobID),
		})
		if err != nil {
			return FailChunkResult{}, fmt.Errorf("record failed chunk: %w", err)
		}
		return FailChunkResult{JobState: JobState(updatedJob.State)}, nil
	})
	return result, normalizeDatabaseError(err)
}

func relevantChainKeys(request chainKey, chunks []db.Chunk) []chainKey {
	keys := []chainKey{request}
	for _, chunk := range chunks {
		if key, ok := chainKeyFromChunk(chunk); ok {
			keys = append(keys, key)
		}
	}
	return keys
}

func chainKeyFromChunk(chunk db.Chunk) (chainKey, bool) {
	return chainKeyFromLeaseFields(chunk.CurrentRankID, chunk.CurrentChainID)
}

func chainKeyFromLeaseFields(rankID pgtype.UUID, chainID *int64) (chainKey, bool) {
	if !rankID.Valid || chainID == nil {
		return chainKey{}, false
	}
	return chainKey{rankID: ulid.ULID(rankID.Bytes), chainID: *chainID}, true
}

func leaseFromChunk(chunk db.Chunk) (Lease, error) {
	owner, ok := chainKeyFromChunk(chunk)
	if !ok || !chunk.LeaseExpiresAt.Valid {
		return Lease{}, fmt.Errorf("%w: assigned chunk has incomplete lease fields", ErrInvalidState)
	}
	return Lease{
		JobID:      ulidFromDB(chunk.JobID),
		ChunkID:    chunk.ChunkID,
		InputRef:   chunk.InputRef,
		RankID:     owner.rankID,
		ChainID:    owner.chainID,
		Generation: chunk.LeaseGeneration,
		ExpiresAt:  chunk.LeaseExpiresAt.Time,
		RetryCount: chunk.RetryCount,
	}, nil
}

func validateCurrentLease(
	job db.Job,
	chunk db.Chunk,
	request chainKey,
	generation int64,
	associations map[chainKey]db.JobChainAssociation,
	now time.Time,
) error {
	if job.State != db.JobStateRUNNING {
		return fmt.Errorf("%w: job is not running", ErrInvalidState)
	}
	if chunk.State != db.ChunkStateLEASED || chunk.LeaseGeneration != generation {
		return ErrStaleLease
	}
	owner, ok := chainKeyFromChunk(chunk)
	if !ok || owner != request {
		return ErrStaleLease
	}
	association, exists := associations[request]
	if !exists {
		return ErrStaleLease
	}
	if association.State != db.ChainStateACTIVE && association.State != db.ChainStateDRAINING {
		return ErrStaleLease
	}
	if !chunk.LeaseExpiresAt.Valid || !now.Before(chunk.LeaseExpiresAt.Time) {
		return ErrLeaseExpired
	}
	return nil
}

func completionMatches(chunk db.Chunk, params CompleteChunkParams) bool {
	return chunk.LeaseGeneration == params.Generation &&
		chunk.OutputUri != nil && *chunk.OutputUri == params.OutputURI &&
		chunk.OutputChecksum != nil && *chunk.OutputChecksum == params.Checksum &&
		chunk.OutputSizeBytes != nil && *chunk.OutputSizeBytes == params.OutputSize
}
