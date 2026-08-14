-- name: InsertChunks :many
INSERT INTO chunks (
    job_id,
    chunk_id,
    input_ref,
    state,
    lease_generation,
    retry_count,
    not_before,
    created_at,
    updated_at
)
SELECT
    sqlc.arg(job_id),
    input.chunk_id,
    input.input_ref,
    'READY',
    0,
    0,
    sqlc.arg(db_time),
    sqlc.arg(db_time),
    sqlc.arg(db_time)
FROM (
    SELECT
        chunk.chunk_id,
        (sqlc.arg(input_refs)::text[])[chunk.ordinality] AS input_ref
    FROM unnest(sqlc.arg(chunk_ids)::bigint[])
        WITH ORDINALITY AS chunk (chunk_id, ordinality)
) AS input
ORDER BY input.chunk_id
RETURNING chunk_id;

-- name: FindClaimCandidates :many
SELECT
    chunks.*,
    CASE state
        WHEN 'READY' THEN not_before
        WHEN 'LEASED' THEN lease_expires_at
    END AS eligible_at
FROM chunks
WHERE job_id = sqlc.arg(job_id)
  AND (
      (state = 'READY' AND not_before <= sqlc.arg(discovery_time))
      OR (state = 'LEASED' AND lease_expires_at <= sqlc.arg(discovery_time))
  )
ORDER BY eligible_at, chunk_id
LIMIT sqlc.arg(candidate_limit);

-- name: GetChunks :many
SELECT *
FROM chunks
WHERE job_id = sqlc.arg(job_id)
  AND chunk_id = ANY(sqlc.arg(chunk_ids)::bigint[])
ORDER BY chunk_id;

-- name: LockChunks :many
SELECT *
FROM chunks
WHERE job_id = sqlc.arg(job_id)
  AND chunk_id = ANY(sqlc.arg(chunk_ids)::bigint[])
ORDER BY chunk_id
FOR UPDATE;

-- name: AssignChunk :one
UPDATE chunks
SET state = 'LEASED',
    current_rank_id = sqlc.arg(rank_id),
    current_chain_id = sqlc.arg(chain_id),
    lease_generation = lease_generation + 1,
    lease_expires_at = sqlc.arg(lease_expires_at),
    retry_count = sqlc.arg(retry_count),
    not_before = NULL,
    last_failure_class = CASE
        WHEN sqlc.arg(record_expiration)::boolean THEN 'LEASE_EXPIRED'
        ELSE last_failure_class
    END,
    last_failure_message = CASE
        WHEN sqlc.arg(record_expiration)::boolean THEN 'active chain lease expired'
        ELSE last_failure_message
    END,
    last_failure_at = CASE
        WHEN sqlc.arg(record_expiration)::boolean THEN sqlc.arg(db_time)
        ELSE last_failure_at
    END,
    updated_at = sqlc.arg(db_time)
WHERE job_id = sqlc.arg(job_id)
  AND chunk_id = sqlc.arg(chunk_id)
RETURNING *;

-- name: RenewMatchingLeases :many
WITH input AS (
    SELECT
        sqlc.arg(chunk_ids)::bigint[] AS chunk_ids,
        sqlc.arg(lease_generations)::bigint[] AS lease_generations
),
requested AS (
    SELECT
        chunk.chunk_id,
        input.lease_generations[chunk.ordinality] AS lease_generation
    FROM input,
         unnest(input.chunk_ids) WITH ORDINALITY AS chunk (chunk_id, ordinality)
)
UPDATE chunks AS chunk
SET lease_expires_at = sqlc.arg(lease_expires_at),
    updated_at = sqlc.arg(db_time)
FROM requested
WHERE chunk.job_id = sqlc.arg(job_id)
  AND chunk.chunk_id = requested.chunk_id
  AND chunk.state = 'LEASED'
  AND chunk.current_rank_id = sqlc.arg(rank_id)
  AND chunk.current_chain_id = sqlc.arg(chain_id)
  AND chunk.lease_generation = requested.lease_generation
RETURNING chunk.chunk_id, chunk.lease_generation, chunk.lease_expires_at;

-- name: SucceedChunk :one
UPDATE chunks
SET state = 'SUCCEEDED',
    current_rank_id = NULL,
    current_chain_id = NULL,
    lease_expires_at = NULL,
    output_uri = sqlc.arg(output_uri),
    output_checksum = sqlc.arg(output_checksum),
    output_size_bytes = sqlc.arg(output_size_bytes),
    terminal_at = sqlc.arg(db_time),
    updated_at = sqlc.arg(db_time)
WHERE job_id = sqlc.arg(job_id)
  AND chunk_id = sqlc.arg(chunk_id)
RETURNING *;

-- name: RetryChunk :one
UPDATE chunks
SET state = 'READY',
    current_rank_id = NULL,
    current_chain_id = NULL,
    lease_expires_at = NULL,
    retry_count = sqlc.arg(retry_count),
    not_before = sqlc.arg(not_before),
    last_failure_class = sqlc.arg(failure_class),
    last_failure_message = sqlc.narg(failure_message),
    last_failure_at = sqlc.arg(db_time),
    updated_at = sqlc.arg(db_time)
WHERE job_id = sqlc.arg(job_id)
  AND chunk_id = sqlc.arg(chunk_id)
RETURNING *;

-- name: FailChunk :one
UPDATE chunks
SET state = 'FAILED',
    current_rank_id = NULL,
    current_chain_id = NULL,
    lease_expires_at = NULL,
    retry_count = sqlc.arg(retry_count),
    not_before = NULL,
    last_failure_class = sqlc.arg(failure_class),
    last_failure_message = sqlc.narg(failure_message),
    last_failure_at = sqlc.arg(db_time),
    terminal_at = sqlc.arg(db_time),
    updated_at = sqlc.arg(db_time)
WHERE job_id = sqlc.arg(job_id)
  AND chunk_id = sqlc.arg(chunk_id)
RETURNING *;

-- name: LockNonterminalChunks :many
SELECT chunk_id
FROM chunks
WHERE job_id = sqlc.arg(job_id)
  AND state IN ('READY', 'LEASED')
ORDER BY chunk_id
FOR UPDATE;

-- name: CancelNonterminalChunks :execrows
UPDATE chunks
SET state = 'CANCELLED',
    current_rank_id = NULL,
    current_chain_id = NULL,
    lease_expires_at = NULL,
    not_before = NULL,
    terminal_at = sqlc.arg(db_time),
    updated_at = sqlc.arg(db_time)
WHERE job_id = sqlc.arg(job_id)
  AND state IN ('READY', 'LEASED');

-- name: FindExpiredDrainingChunkIDs :many
SELECT chunk_id
FROM chunks
WHERE job_id = sqlc.arg(job_id)
  AND state = 'LEASED'
  AND current_rank_id = sqlc.arg(rank_id)
  AND current_chain_id = sqlc.arg(chain_id)
  AND lease_expires_at <= sqlc.arg(discovery_time)
ORDER BY chunk_id
LIMIT sqlc.arg(candidate_limit);

-- name: RequeueExpiredDrainingChunks :many
UPDATE chunks
SET state = 'READY',
    current_rank_id = NULL,
    current_chain_id = NULL,
    lease_expires_at = NULL,
    not_before = sqlc.arg(db_time),
    updated_at = sqlc.arg(db_time)
WHERE job_id = sqlc.arg(job_id)
  AND chunk_id = ANY(sqlc.arg(chunk_ids)::bigint[])
  AND state = 'LEASED'
  AND current_rank_id = sqlc.arg(rank_id)
  AND current_chain_id = sqlc.arg(chain_id)
  AND lease_expires_at <= sqlc.arg(db_time)
RETURNING chunk_id;

-- name: ChainHasLeasedChunks :one
SELECT EXISTS (
    SELECT 1
    FROM chunks
    WHERE job_id = sqlc.arg(job_id)
      AND state = 'LEASED'
      AND current_rank_id = sqlc.arg(rank_id)
      AND current_chain_id = sqlc.arg(chain_id)
);
