package postgres

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/tandemn-labs/chunk-manager/internal/postgres/db"
)

func (store *Store) CreateJob(ctx context.Context, params CreateJobParams) (Job, error) {
	if strings.TrimSpace(params.InputManifestRef) == "" {
		return Job{}, fmt.Errorf("%w: input manifest reference is required", ErrInvalidArgument)
	}
	if params.TotalChunkCount <= 0 {
		return Job{}, fmt.Errorf("%w: total chunk count must be positive", ErrInvalidArgument)
	}
	if params.MaxRetries < 0 {
		return Job{}, fmt.Errorf("%w: maximum retries cannot be negative", ErrInvalidArgument)
	}
	if params.MaxRetries == 2147483647 {
		return Job{}, fmt.Errorf("%w: maximum retries is too large", ErrInvalidArgument)
	}
	if params.RetryBackoffInitial < 0 || params.RetryBackoffMax < params.RetryBackoffInitial {
		return Job{}, fmt.Errorf("%w: invalid retry backoff", ErrInvalidArgument)
	}
	if params.LeaseDuration < time.Millisecond {
		return Job{}, fmt.Errorf("%w: lease duration must be at least one millisecond", ErrInvalidArgument)
	}

	result, err := withTransaction(ctx, store, func(queries *db.Queries) (Job, error) {
		now, err := databaseTime(ctx, queries)
		if err != nil {
			return Job{}, err
		}
		row, err := queries.CreateJob(ctx, db.CreateJobParams{
			JobID:                 dbUUID(params.JobID),
			InputManifestRef:      params.InputManifestRef,
			TotalChunkCount:       params.TotalChunkCount,
			MaxRetries:            params.MaxRetries,
			RetryBackoffInitialMs: params.RetryBackoffInitial.Milliseconds(),
			RetryBackoffMaxMs:     params.RetryBackoffMax.Milliseconds(),
			LeaseDurationMs:       params.LeaseDuration.Milliseconds(),
			DbTime:                dbTimestamp(now),
		})
		if err != nil {
			return Job{}, fmt.Errorf("create job: %w", err)
		}
		return jobFromDB(row), nil
	})
	return result, normalizeDatabaseError(err)
}

func (store *Store) GetJob(ctx context.Context, jobID ulid.ULID) (Job, error) {
	row, err := db.New(store.pool).GetJob(ctx, dbUUID(jobID))
	if err != nil {
		return Job{}, normalizeDatabaseError(fmt.Errorf("get job: %w", err))
	}
	return jobFromDB(row), nil
}

func (store *Store) RegisterChunks(
	ctx context.Context,
	jobID ulid.ULID,
	chunks []ChunkRegistration,
) (int, error) {
	if len(chunks) == 0 {
		return 0, fmt.Errorf("%w: at least one chunk is required", ErrInvalidArgument)
	}
	if len(chunks) > store.maxRegistrationChunks {
		return 0, fmt.Errorf(
			"%w: registration batch exceeds %d chunks",
			ErrInvalidArgument,
			store.maxRegistrationChunks,
		)
	}

	sortedChunks := append([]ChunkRegistration(nil), chunks...)
	sort.Slice(sortedChunks, func(left, right int) bool {
		return sortedChunks[left].ChunkID < sortedChunks[right].ChunkID
	})
	for index, chunk := range sortedChunks {
		if chunk.ChunkID < 0 {
			return 0, fmt.Errorf("%w: chunk ID cannot be negative", ErrInvalidArgument)
		}
		if strings.TrimSpace(chunk.InputRef) == "" {
			return 0, fmt.Errorf("%w: input reference is required", ErrInvalidArgument)
		}
		if index > 0 && sortedChunks[index-1].ChunkID == chunk.ChunkID {
			return 0, fmt.Errorf("%w: duplicate chunk ID %d", ErrInvalidArgument, chunk.ChunkID)
		}
	}

	registered, err := withTransaction(ctx, store, func(queries *db.Queries) (int, error) {
		job, err := queries.LockJob(ctx, dbUUID(jobID))
		if err != nil {
			return 0, fmt.Errorf("lock job: %w", err)
		}
		if job.State != db.JobStatePENDING {
			return 0, fmt.Errorf("%w: chunks can only be registered for a pending job", ErrInvalidState)
		}

		registeredCount, err := queries.CountJobChunks(ctx, dbUUID(jobID))
		if err != nil {
			return 0, fmt.Errorf("count registered chunks: %w", err)
		}
		if registeredCount+int64(len(sortedChunks)) > job.TotalChunkCount {
			return 0, fmt.Errorf("%w: registration exceeds declared chunk count", ErrConflict)
		}

		now, err := databaseTime(ctx, queries)
		if err != nil {
			return 0, err
		}
		chunkIDs := make([]int64, len(sortedChunks))
		inputRefs := make([]string, len(sortedChunks))
		for index, chunk := range sortedChunks {
			chunkIDs[index] = chunk.ChunkID
			inputRefs[index] = chunk.InputRef
		}
		inserted, err := queries.InsertChunks(ctx, db.InsertChunksParams{
			JobID:     dbUUID(jobID),
			DbTime:    dbTimestamp(now),
			InputRefs: inputRefs,
			ChunkIds:  chunkIDs,
		})
		if err != nil {
			return 0, fmt.Errorf("insert chunks: %w", err)
		}
		return len(inserted), nil
	})
	return registered, normalizeDatabaseError(err)
}

func (store *Store) FinalizeJobRegistration(ctx context.Context, jobID ulid.ULID) (Job, error) {
	result, err := withTransaction(ctx, store, func(queries *db.Queries) (Job, error) {
		job, err := queries.LockJob(ctx, dbUUID(jobID))
		if err != nil {
			return Job{}, fmt.Errorf("lock job: %w", err)
		}
		if job.State != db.JobStatePENDING {
			return Job{}, fmt.Errorf("%w: job is not pending", ErrInvalidState)
		}

		chunkCount, err := queries.CountJobChunks(ctx, dbUUID(jobID))
		if err != nil {
			return Job{}, fmt.Errorf("count registered chunks: %w", err)
		}
		if chunkCount != job.TotalChunkCount {
			return Job{}, fmt.Errorf(
				"%w: registered %d of %d chunks",
				ErrRegistrationIncomplete,
				chunkCount,
				job.TotalChunkCount,
			)
		}

		now, err := databaseTime(ctx, queries)
		if err != nil {
			return Job{}, err
		}
		updated, err := queries.FinalizeJobRegistration(ctx, db.FinalizeJobRegistrationParams{
			DbTime: dbTimestamp(now),
			JobID:  dbUUID(jobID),
		})
		if err != nil {
			return Job{}, fmt.Errorf("finalize job registration: %w", err)
		}
		return jobFromDB(updated), nil
	})
	return result, normalizeDatabaseError(err)
}

func (store *Store) FailJobRegistration(ctx context.Context, jobID ulid.ULID) (Job, error) {
	result, err := withTransaction(ctx, store, func(queries *db.Queries) (Job, error) {
		job, err := queries.LockJob(ctx, dbUUID(jobID))
		if err != nil {
			return Job{}, fmt.Errorf("lock job: %w", err)
		}
		if job.State != db.JobStatePENDING {
			return Job{}, fmt.Errorf("%w: job is not pending", ErrInvalidState)
		}
		now, err := databaseTime(ctx, queries)
		if err != nil {
			return Job{}, err
		}
		updated, err := queries.FailJobRegistration(ctx, db.FailJobRegistrationParams{
			DbTime: dbTimestamp(now),
			JobID:  dbUUID(jobID),
		})
		if err != nil {
			return Job{}, fmt.Errorf("fail job registration: %w", err)
		}
		return jobFromDB(updated), nil
	})
	return result, normalizeDatabaseError(err)
}

func (store *Store) CancelJob(ctx context.Context, jobID ulid.ULID) (Job, error) {
	result, err := withTransaction(ctx, store, func(queries *db.Queries) (Job, error) {
		job, err := queries.LockJob(ctx, dbUUID(jobID))
		if err != nil {
			return Job{}, fmt.Errorf("lock job: %w", err)
		}
		if job.State == db.JobStateCANCELLED {
			return jobFromDB(job), nil
		}
		if job.State != db.JobStatePENDING && job.State != db.JobStateRUNNING {
			return Job{}, fmt.Errorf("%w: terminal job cannot be cancelled", ErrInvalidState)
		}

		if _, err := queries.LockAllJobChainAssociations(ctx, dbUUID(jobID)); err != nil {
			return Job{}, fmt.Errorf("lock job chain associations: %w", err)
		}
		if _, err := queries.LockNonterminalChunks(ctx, dbUUID(jobID)); err != nil {
			return Job{}, fmt.Errorf("lock nonterminal chunks: %w", err)
		}
		now, err := databaseTime(ctx, queries)
		if err != nil {
			return Job{}, err
		}
		if _, err := queries.CancelNonterminalChunks(ctx, db.CancelNonterminalChunksParams{
			DbTime: dbTimestamp(now),
			JobID:  dbUUID(jobID),
		}); err != nil {
			return Job{}, fmt.Errorf("cancel chunks: %w", err)
		}
		updated, err := queries.CancelJob(ctx, db.CancelJobParams{
			DbTime: dbTimestamp(now),
			JobID:  dbUUID(jobID),
		})
		if err != nil {
			return Job{}, fmt.Errorf("cancel job: %w", err)
		}
		return jobFromDB(updated), nil
	})
	return result, normalizeDatabaseError(err)
}
