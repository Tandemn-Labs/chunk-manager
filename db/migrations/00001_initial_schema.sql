-- +goose Up
CREATE TYPE job_state AS ENUM (
    'PENDING',
    'RUNNING',
    'SUCCEEDED',
    'FAILED',
    'CANCELLED'
);

CREATE TYPE chain_state AS ENUM (
    'ACTIVE',
    'DRAINING'
);

CREATE TYPE chunk_state AS ENUM (
    'READY',
    'LEASED',
    'SUCCEEDED',
    'FAILED',
    'CANCELLED'
);

CREATE TABLE jobs (
    job_id uuid PRIMARY KEY,
    state job_state NOT NULL DEFAULT 'PENDING',
    input_manifest_ref text NOT NULL CHECK (btrim(input_manifest_ref) <> ''),

    total_chunk_count bigint NOT NULL CHECK (total_chunk_count > 0),
    succeeded_chunk_count bigint NOT NULL DEFAULT 0 CHECK (succeeded_chunk_count >= 0),
    failed_chunk_count bigint NOT NULL DEFAULT 0 CHECK (failed_chunk_count >= 0),

    max_retries integer NOT NULL CHECK (max_retries >= 0 AND max_retries < 2147483647),
    retry_backoff_initial_ms bigint NOT NULL CHECK (retry_backoff_initial_ms >= 0),
    retry_backoff_max_ms bigint NOT NULL
        CHECK (retry_backoff_max_ms >= retry_backoff_initial_ms),
    lease_duration_ms bigint NOT NULL CHECK (lease_duration_ms > 0),

    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    registration_completed_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    terminal_at timestamptz,

    CONSTRAINT jobs_chunk_counts_valid CHECK (
        succeeded_chunk_count + failed_chunk_count <= total_chunk_count
    ),
    CONSTRAINT jobs_terminal_timestamp_valid CHECK (
        (state IN ('SUCCEEDED', 'FAILED', 'CANCELLED')) = (terminal_at IS NOT NULL)
    ),
    CONSTRAINT jobs_pending_valid CHECK (
        state <> 'PENDING'
        OR (
            registration_completed_at IS NULL
            AND succeeded_chunk_count = 0
            AND failed_chunk_count = 0
        )
    ),
    CONSTRAINT jobs_registration_valid CHECK (
        state NOT IN ('RUNNING', 'SUCCEEDED')
        OR registration_completed_at IS NOT NULL
    ),
    CONSTRAINT jobs_succeeded_valid CHECK (
        state <> 'SUCCEEDED'
        OR (
            succeeded_chunk_count = total_chunk_count
            AND failed_chunk_count = 0
        )
    ),
    CONSTRAINT jobs_failed_valid CHECK (
        state <> 'FAILED'
        OR registration_completed_at IS NULL
        OR (
            failed_chunk_count > 0
            AND succeeded_chunk_count + failed_chunk_count = total_chunk_count
        )
    )
);

CREATE TABLE job_chain_associations (
    job_id uuid NOT NULL REFERENCES jobs(job_id) ON DELETE RESTRICT,
    rank_id uuid NOT NULL,
    chain_id bigint NOT NULL CHECK (chain_id >= 0),
    state chain_state NOT NULL DEFAULT 'ACTIVE',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    draining_at timestamptz,

    PRIMARY KEY (job_id, rank_id, chain_id),
    CONSTRAINT chain_associations_state_valid CHECK (
        (state = 'ACTIVE' AND draining_at IS NULL)
        OR (state = 'DRAINING' AND draining_at IS NOT NULL)
    )
);

CREATE TABLE chunks (
    job_id uuid NOT NULL REFERENCES jobs(job_id) ON DELETE RESTRICT,
    chunk_id bigint NOT NULL CHECK (chunk_id >= 0),
    input_ref text NOT NULL CHECK (btrim(input_ref) <> ''),

    state chunk_state NOT NULL DEFAULT 'READY',
    current_rank_id uuid,
    current_chain_id bigint CHECK (current_chain_id >= 0),
    lease_generation bigint NOT NULL DEFAULT 0 CHECK (lease_generation >= 0),
    lease_expires_at timestamptz,

    retry_count integer NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    not_before timestamptz,

    output_uri text,
    output_checksum text,
    output_size_bytes bigint CHECK (output_size_bytes >= 0),

    last_failure_class text,
    last_failure_message text,
    last_failure_at timestamptz,

    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    terminal_at timestamptz,

    PRIMARY KEY (job_id, chunk_id),
    FOREIGN KEY (job_id, current_rank_id, current_chain_id)
        REFERENCES job_chain_associations(job_id, rank_id, chain_id)
        ON DELETE RESTRICT,

    CONSTRAINT chunks_lease_fields_valid CHECK (
        (
            state = 'LEASED'
            AND current_rank_id IS NOT NULL
            AND current_chain_id IS NOT NULL
            AND lease_expires_at IS NOT NULL
        )
        OR (
            state <> 'LEASED'
            AND current_rank_id IS NULL
            AND current_chain_id IS NULL
            AND lease_expires_at IS NULL
        )
    ),
    CONSTRAINT chunks_not_before_valid CHECK (
        (state = 'READY' AND not_before IS NOT NULL)
        OR (state <> 'READY' AND not_before IS NULL)
    ),
    CONSTRAINT chunks_output_valid CHECK (
        (
            state = 'SUCCEEDED'
            AND output_uri IS NOT NULL
            AND output_checksum IS NOT NULL
            AND output_size_bytes IS NOT NULL
        )
        OR (
            state <> 'SUCCEEDED'
            AND output_uri IS NULL
            AND output_checksum IS NULL
            AND output_size_bytes IS NULL
        )
    ),
    CONSTRAINT chunks_terminal_timestamp_valid CHECK (
        (state IN ('SUCCEEDED', 'FAILED', 'CANCELLED')) = (terminal_at IS NOT NULL)
    ),
    CONSTRAINT chunks_failure_fields_valid CHECK (
        (
            last_failure_class IS NULL
            AND last_failure_message IS NULL
            AND last_failure_at IS NULL
        )
        OR (
            last_failure_class IS NOT NULL
            AND last_failure_at IS NOT NULL
        )
    ),
    CONSTRAINT chunks_failed_valid CHECK (
        state <> 'FAILED' OR last_failure_class IS NOT NULL
    )
);

CREATE INDEX job_chain_associations_draining_idx
    ON job_chain_associations (job_id, rank_id, chain_id)
    WHERE state = 'DRAINING';

CREATE INDEX chunks_ready_due_idx
    ON chunks (job_id, not_before, chunk_id)
    WHERE state = 'READY';

CREATE INDEX chunks_expired_lease_idx
    ON chunks (job_id, lease_expires_at, chunk_id)
    INCLUDE (current_rank_id, current_chain_id, retry_count)
    WHERE state = 'LEASED';

CREATE INDEX chunks_chain_leases_idx
    ON chunks (
        job_id,
        current_rank_id,
        current_chain_id,
        lease_expires_at,
        chunk_id
    )
    WHERE state = 'LEASED';

-- +goose Down
DROP TABLE chunks;
DROP TABLE job_chain_associations;
DROP TABLE jobs;
DROP TYPE chunk_state;
DROP TYPE chain_state;
DROP TYPE job_state;
