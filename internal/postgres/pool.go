package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PoolConfig struct {
	DatabaseURL       string
	ApplicationName   string
	MaxConnections    int32
	MinConnections    int32
	MaxConnectionIdle time.Duration
	MaxConnectionAge  time.Duration
	HealthCheckPeriod time.Duration
}

func OpenPool(ctx context.Context, config PoolConfig) (*pgxpool.Pool, error) {
	if config.DatabaseURL == "" {
		return nil, fmt.Errorf("%w: database URL is required", ErrInvalidArgument)
	}

	poolConfig, err := pgxpool.ParseConfig(config.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database configuration: %w", err)
	}

	if config.ApplicationName != "" {
		poolConfig.ConnConfig.RuntimeParams["application_name"] = config.ApplicationName
	}
	if config.MaxConnections > 0 {
		poolConfig.MaxConns = config.MaxConnections
	}
	if config.MinConnections > 0 {
		poolConfig.MinConns = config.MinConnections
	}
	if config.MaxConnectionIdle > 0 {
		poolConfig.MaxConnIdleTime = config.MaxConnectionIdle
	}
	if config.MaxConnectionAge > 0 {
		poolConfig.MaxConnLifetime = config.MaxConnectionAge
	}
	if config.HealthCheckPeriod > 0 {
		poolConfig.HealthCheckPeriod = config.HealthCheckPeriod
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create PostgreSQL pool: %w", err)
	}

	if err := CheckWritable(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}

	return pool, nil
}

func CheckWritable(ctx context.Context, pool *pgxpool.Pool) error {
	var writable bool
	err := pool.QueryRow(ctx, `
		SELECT current_setting('transaction_read_only') = 'off'
		   AND NOT pg_is_in_recovery()
	`).Scan(&writable)
	if err != nil {
		return fmt.Errorf("%w: check PostgreSQL writer: %w", ErrUnavailable, err)
	}
	if !writable {
		return fmt.Errorf("%w: %w", ErrUnavailable, errors.New("PostgreSQL endpoint is not writable"))
	}
	return nil
}

func CheckReady(ctx context.Context, pool *pgxpool.Pool) error {
	if err := CheckWritable(ctx, pool); err != nil {
		return err
	}
	rows, err := pool.Query(ctx, `
		SELECT
			job.job_id,
			job.retry_backoff_ms,
			job.lease_duration_ms,
			association.rank_id,
			association.chain_id,
			chunk.chunk_id,
			chunk.input_ref,
			chunk.lease_generation
		FROM jobs AS job
		LEFT JOIN job_chain_associations AS association ON false
		LEFT JOIN chunks AS chunk ON false
		LIMIT 0
	`)
	if err != nil {
		return fmt.Errorf("check PostgreSQL schema: %w", err)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("check PostgreSQL schema: %w", err)
	}

	var privilegesAvailable bool
	err = pool.QueryRow(ctx, `
		SELECT
			has_table_privilege(current_user, 'jobs', 'SELECT, INSERT, UPDATE')
			AND has_table_privilege(
				current_user,
				'job_chain_associations',
				'SELECT, INSERT, UPDATE, DELETE'
			)
			AND has_table_privilege(current_user, 'chunks', 'SELECT, INSERT, UPDATE')
	`).Scan(&privilegesAvailable)
	if err != nil {
		return fmt.Errorf("check PostgreSQL privileges: %w", err)
	}
	if !privilegesAvailable {
		return errors.New("PostgreSQL role lacks required chunk-manager table privileges")
	}
	return nil
}
