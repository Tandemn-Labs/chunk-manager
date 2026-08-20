package postgres

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/oklog/ulid/v2"

	"github.com/tandemn-labs/chunk-manager/internal/postgres/db"
)

func (store *Store) CreateJob(ctx context.Context, params CreateJobParams) (Job, error) {
	if params.TotalChunkCount <= 0 {
		return Job{}, fmt.Errorf("%w: total chunk count must be positive", ErrInvalidArgument)
	}
	if params.MaxRetries < 0 {
		return Job{}, fmt.Errorf("%w: maximum retries cannot be negative", ErrInvalidArgument)
	}
	if params.MaxRetries == 2147483647 {
		return Job{}, fmt.Errorf("%w: maximum retries is too large", ErrInvalidArgument)
	}
	if params.RetryBackoff < 0 {
		return Job{}, fmt.Errorf("%w: retry backoff cannot be negative", ErrInvalidArgument)
	}
	if params.LeaseDuration < time.Millisecond {
		return Job{}, fmt.Errorf("%w: lease duration must be at least one millisecond", ErrInvalidArgument)
	}
	if params.RetryBackoff%time.Millisecond != 0 || params.LeaseDuration%time.Millisecond != 0 {
		return Job{}, fmt.Errorf("%w: durations must use whole milliseconds", ErrInvalidArgument)
	}

	result, err := withTransaction(ctx, store, func(queries *db.Queries) (Job, error) {
		now, err := databaseTime(ctx, queries)
		if err != nil {
			return Job{}, err
		}
		row, err := queries.CreateJob(ctx, db.CreateJobParams{
			JobID:           dbUUID(params.JobID),
			TotalChunkCount: params.TotalChunkCount,
			MaxRetries:      params.MaxRetries,
			RetryBackoffMs:  params.RetryBackoff.Milliseconds(),
			LeaseDurationMs: params.LeaseDuration.Milliseconds(),
			DbTime:          dbTimestamp(now),
		})
		if errors.Is(err, pgx.ErrNoRows) {
			row, err = queries.LockJob(ctx, dbUUID(params.JobID))
			if err != nil {
				return Job{}, fmt.Errorf("read existing job: %w", err)
			}
			existing := jobFromDB(row)
			if createJobMatches(existing, params) {
				return existing, nil
			}
			return Job{}, fmt.Errorf("%w: job ID already exists with different settings", ErrConflict)
		}
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

	chunkIDs := make([]int64, len(sortedChunks))
	for index, chunk := range sortedChunks {
		chunkIDs[index] = chunk.ChunkID
	}

	registered, err := withTransaction(ctx, store, func(queries *db.Queries) (int, error) {
		job, err := queries.LockJob(ctx, dbUUID(jobID))
		if err != nil {
			return 0, fmt.Errorf("lock job: %w", err)
		}

		existingRows, err := queries.GetChunks(ctx, db.GetChunksParams{
			JobID:    dbUUID(jobID),
			ChunkIds: chunkIDs,
		})
		if err != nil {
			return 0, fmt.Errorf("get registered chunks: %w", err)
		}
		existingByID := make(map[int64]db.Chunk, len(existingRows))
		for _, row := range existingRows {
			existingByID[row.ChunkID] = row
		}
		missing := make([]ChunkRegistration, 0, len(sortedChunks)-len(existingRows))
		for _, chunk := range sortedChunks {
			existing, ok := existingByID[chunk.ChunkID]
			if !ok {
				missing = append(missing, chunk)
				continue
			}
			if existing.InputRef != chunk.InputRef {
				return 0, fmt.Errorf(
					"%w: chunk ID %d already has a different input reference",
					ErrConflict,
					chunk.ChunkID,
				)
			}
		}

		if job.State != db.JobStatePENDING {
			if len(missing) == 0 {
				return len(sortedChunks), nil
			}
			return 0, fmt.Errorf("%w: chunks can only be registered for a pending job", ErrInvalidState)
		}

		registeredCount, err := queries.CountJobChunks(ctx, dbUUID(jobID))
		if err != nil {
			return 0, fmt.Errorf("count registered chunks: %w", err)
		}
		if registeredCount+int64(len(missing)) > job.TotalChunkCount {
			return 0, fmt.Errorf("%w: registration exceeds declared chunk count", ErrConflict)
		}
		if len(missing) == 0 {
			return len(sortedChunks), nil
		}

		now, err := databaseTime(ctx, queries)
		if err != nil {
			return 0, err
		}
		missingChunkIDs := make([]int64, len(missing))
		inputRefs := make([]string, len(missing))
		for index, chunk := range missing {
			missingChunkIDs[index] = chunk.ChunkID
			inputRefs[index] = chunk.InputRef
		}
		inserted, err := queries.InsertChunks(ctx, db.InsertChunksParams{
			JobID:     dbUUID(jobID),
			DbTime:    dbTimestamp(now),
			InputRefs: inputRefs,
			ChunkIds:  missingChunkIDs,
		})
		if err != nil {
			return 0, fmt.Errorf("insert chunks: %w", err)
		}
		if len(inserted) != len(missing) {
			return 0, fmt.Errorf("insert chunks: expected %d rows, inserted %d", len(missing), len(inserted))
		}
		return len(sortedChunks), nil
	})
	return registered, normalizeDatabaseError(err)
}

func (store *Store) FinalizeJobRegistration(ctx context.Context, jobID ulid.ULID) (Job, error) {
	result, err := withTransaction(ctx, store, func(queries *db.Queries) (Job, error) {
		job, err := queries.LockJob(ctx, dbUUID(jobID))
		if err != nil {
			return Job{}, fmt.Errorf("lock job: %w", err)
		}
		if job.State != db.JobStatePENDING && job.RegistrationCompletedAt.Valid {
			return jobFromDB(job), nil
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

func createJobMatches(job Job, params CreateJobParams) bool {
	return job.ID == params.JobID &&
		job.TotalChunkCount == params.TotalChunkCount &&
		job.MaxRetries == params.MaxRetries &&
		job.RetryBackoff == params.RetryBackoff &&
		job.LeaseDuration == params.LeaseDuration
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
