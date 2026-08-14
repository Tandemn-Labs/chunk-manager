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

	var writable bool
	err = pool.QueryRow(ctx, `
		SELECT current_setting('transaction_read_only') = 'off'
		   AND NOT pg_is_in_recovery()
	`).Scan(&writable)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("check PostgreSQL writer: %w", err)
	}
	if !writable {
		pool.Close()
		return nil, errors.New("PostgreSQL endpoint is not writable")
	}

	return pool, nil
}
