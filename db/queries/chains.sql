-- name: CreateChainAssociation :one
INSERT INTO job_chain_associations (
    job_id,
    rank_id,
    chain_id,
    state,
    created_at
) VALUES (
    sqlc.arg(job_id),
    sqlc.arg(rank_id),
    sqlc.arg(chain_id),
    'ACTIVE',
    sqlc.arg(db_time)
)
RETURNING *;

-- name: LockChainAssociations :many
WITH input AS (
    SELECT
        sqlc.arg(rank_ids)::uuid[] AS rank_ids,
        sqlc.arg(chain_ids)::bigint[] AS chain_ids
),
requested AS (
    SELECT
        rank.rank_id,
        input.chain_ids[rank.ordinality] AS chain_id
    FROM input,
         unnest(input.rank_ids) WITH ORDINALITY AS rank (rank_id, ordinality)
)
SELECT association.*
FROM job_chain_associations AS association
JOIN requested USING (rank_id, chain_id)
WHERE association.job_id = sqlc.arg(job_id)
ORDER BY association.rank_id, association.chain_id
FOR UPDATE OF association;

-- name: LockAllJobChainAssociations :many
SELECT *
FROM job_chain_associations
WHERE job_id = sqlc.arg(job_id)
ORDER BY rank_id, chain_id
FOR UPDATE;

-- name: MarkChainDraining :one
UPDATE job_chain_associations
SET state = 'DRAINING',
    draining_at = sqlc.arg(db_time)
WHERE job_id = sqlc.arg(job_id)
  AND rank_id = sqlc.arg(rank_id)
  AND chain_id = sqlc.arg(chain_id)
  AND state = 'ACTIVE'
RETURNING *;

-- name: ListDrainingChainAssociations :many
SELECT *
FROM job_chain_associations
WHERE state = 'DRAINING'
  AND (
      NOT sqlc.arg(has_cursor)::boolean
      OR (job_id, rank_id, chain_id) > (
          sqlc.arg(after_job_id)::uuid,
          sqlc.arg(after_rank_id)::uuid,
          sqlc.arg(after_chain_id)::bigint
      )
  )
ORDER BY job_id, rank_id, chain_id
LIMIT sqlc.arg(result_limit);

-- name: DeleteDrainingChainAssociation :execrows
DELETE FROM job_chain_associations
WHERE job_id = sqlc.arg(job_id)
  AND rank_id = sqlc.arg(rank_id)
  AND chain_id = sqlc.arg(chain_id)
  AND state = 'DRAINING';
