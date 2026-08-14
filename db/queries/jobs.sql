-- name: CreateJob :one
INSERT INTO jobs (
    job_id,
    input_manifest_ref,
    total_chunk_count,
    max_retries,
    retry_backoff_initial_ms,
    retry_backoff_max_ms,
    lease_duration_ms,
    created_at,
    updated_at
) VALUES (
    sqlc.arg(job_id),
    sqlc.arg(input_manifest_ref),
    sqlc.arg(total_chunk_count),
    sqlc.arg(max_retries),
    sqlc.arg(retry_backoff_initial_ms),
    sqlc.arg(retry_backoff_max_ms),
    sqlc.arg(lease_duration_ms),
    sqlc.arg(db_time),
    sqlc.arg(db_time)
)
RETURNING *;

-- name: GetJob :one
SELECT *
FROM jobs
WHERE job_id = sqlc.arg(job_id);

-- name: LockJob :one
SELECT *
FROM jobs
WHERE job_id = sqlc.arg(job_id)
FOR UPDATE;

-- name: CountJobChunks :one
SELECT count(*)
FROM chunks
WHERE job_id = sqlc.arg(job_id);

-- name: FinalizeJobRegistration :one
UPDATE jobs
SET state = 'RUNNING',
    registration_completed_at = sqlc.arg(db_time),
    updated_at = sqlc.arg(db_time)
WHERE job_id = sqlc.arg(job_id)
  AND state = 'PENDING'
RETURNING *;

-- name: FailJobRegistration :one
UPDATE jobs
SET state = 'FAILED',
    terminal_at = sqlc.arg(db_time),
    updated_at = sqlc.arg(db_time)
WHERE job_id = sqlc.arg(job_id)
  AND state = 'PENDING'
RETURNING *;

-- name: RecordJobChunkSucceeded :one
UPDATE jobs
SET succeeded_chunk_count = succeeded_chunk_count + 1,
    state = CASE
        WHEN succeeded_chunk_count + failed_chunk_count + 1 = total_chunk_count
            THEN CASE
                WHEN failed_chunk_count = 0 THEN 'SUCCEEDED'::job_state
                ELSE 'FAILED'::job_state
            END
        ELSE state
    END,
    terminal_at = CASE
        WHEN succeeded_chunk_count + failed_chunk_count + 1 = total_chunk_count
            THEN sqlc.arg(db_time)
        ELSE terminal_at
    END,
    updated_at = sqlc.arg(db_time)
WHERE job_id = sqlc.arg(job_id)
  AND state = 'RUNNING'
RETURNING *;

-- name: RecordJobChunkFailed :one
UPDATE jobs
SET failed_chunk_count = failed_chunk_count + 1,
    state = CASE
        WHEN succeeded_chunk_count + failed_chunk_count + 1 = total_chunk_count
            THEN 'FAILED'::job_state
        ELSE state
    END,
    terminal_at = CASE
        WHEN succeeded_chunk_count + failed_chunk_count + 1 = total_chunk_count
            THEN sqlc.arg(db_time)
        ELSE terminal_at
    END,
    updated_at = sqlc.arg(db_time)
WHERE job_id = sqlc.arg(job_id)
  AND state = 'RUNNING'
RETURNING *;

-- name: CancelJob :one
UPDATE jobs
SET state = 'CANCELLED',
    terminal_at = sqlc.arg(db_time),
    updated_at = sqlc.arg(db_time)
WHERE job_id = sqlc.arg(job_id)
  AND state IN ('PENDING', 'RUNNING')
RETURNING *;
