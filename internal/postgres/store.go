package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oklog/ulid/v2"

	"github.com/tandemn-labs/chunk-manager/internal/postgres/db"
)

const (
	defaultMaxClaimChunks        = int32(256)
	defaultMaxRenewLeases        = 1024
	defaultMaxRegistrationChunks = 4096
	defaultReconcileBatchSize    = int32(256)
)

type StoreConfig struct {
	LockTimeout           time.Duration
	MaxClaimChunks        int32
	MaxRenewLeases        int
	MaxRegistrationChunks int
	ReconcileBatchSize    int32
}

type Store struct {
	pool                  *pgxpool.Pool
	lockTimeout           time.Duration
	maxClaimChunks        int32
	maxRenewLeases        int
	maxRegistrationChunks int
	reconcileBatchSize    int32
}

func NewStore(pool *pgxpool.Pool, config StoreConfig) (*Store, error) {
	if pool == nil {
		return nil, fmt.Errorf("%w: PostgreSQL pool is required", ErrInvalidArgument)
	}
	if config.LockTimeout < 0 {
		return nil, fmt.Errorf("%w: lock timeout cannot be negative", ErrInvalidArgument)
	}

	if config.MaxClaimChunks == 0 {
		config.MaxClaimChunks = defaultMaxClaimChunks
	}
	if config.MaxRenewLeases == 0 {
		config.MaxRenewLeases = defaultMaxRenewLeases
	}
	if config.MaxRegistrationChunks == 0 {
		config.MaxRegistrationChunks = defaultMaxRegistrationChunks
	}
	if config.ReconcileBatchSize == 0 {
		config.ReconcileBatchSize = defaultReconcileBatchSize
	}
	if config.MaxClaimChunks < 0 || config.MaxRenewLeases < 0 ||
		config.MaxRegistrationChunks < 0 || config.ReconcileBatchSize < 0 {
		return nil, fmt.Errorf("%w: store limits must be positive", ErrInvalidArgument)
	}

	return &Store{
		pool:                  pool,
		lockTimeout:           config.LockTimeout,
		maxClaimChunks:        config.MaxClaimChunks,
		maxRenewLeases:        config.MaxRenewLeases,
		maxRegistrationChunks: config.MaxRegistrationChunks,
		reconcileBatchSize:    config.ReconcileBatchSize,
	}, nil
}

func withTransaction[T any](
	ctx context.Context,
	store *Store,
	operation func(*db.Queries) (T, error),
) (result T, err error) {
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return result, fmt.Errorf("begin transaction: %w", err)
	}

	// If commit is successful, rollback reports error which is ignored. I.e. a no-op
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	// Sets timeout for lock acquisition, not for query runtime
	// The `true` is so that the setting only applies to the current transaction
	if store.lockTimeout > 0 {
		milliseconds := max(store.lockTimeout.Milliseconds(), 1)
		_, err = tx.Exec(
			ctx,
			"SELECT set_config('lock_timeout', $1, true)",
			strconv.FormatInt(milliseconds, 10)+"ms",
		)
		if err != nil {
			return result, fmt.Errorf("set transaction lock timeout: %w", err)
		}
	}

	// Applies the operation using the `pgx.Tx` object  (a connection returned from the pool)
	// The `operation` parameter is a function that takes in a `Queries` object
	// I.e. The `operation` function will use that object to do all the queries
	// associated with the TX
	result, err = operation(db.New(tx))
	if err != nil {
		return result, err
	}
	if err := tx.Commit(ctx); err != nil {
		return result, fmt.Errorf("commit transaction: %w", err)
	}

	return result, nil
}

func databaseTime(ctx context.Context, queries *db.Queries) (time.Time, error) {
	value, err := queries.DatabaseTime(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("read database time: %w", err)
	}
	if !value.Valid {
		return time.Time{}, errors.New("PostgreSQL returned a null clock timestamp")
	}
	return value.Time, nil
}

func dbTimestamp(value time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: value, Valid: true}
}

func dbUUID(value ulid.ULID) uuid.UUID {
	return uuid.UUID(value)
}

func nullableDBUUID(value ulid.ULID) pgtype.UUID {
	return pgtype.UUID{Bytes: [16]byte(value), Valid: true}
}

func ulidFromDB(value uuid.UUID) ulid.ULID {
	return ulid.ULID(value)
}

func optionalTime(value pgtype.Timestamptz) *time.Time {
	if !value.Valid {
		return nil
	}
	result := value.Time
	return &result
}

func jobFromDB(value db.Job) Job {
	return Job{
		ID:                      ulidFromDB(value.JobID),
		State:                   JobState(value.State),
		TotalChunkCount:         value.TotalChunkCount,
		SucceededChunkCount:     value.SucceededChunkCount,
		FailedChunkCount:        value.FailedChunkCount,
		MaxRetries:              value.MaxRetries,
		RetryBackoff:            time.Duration(value.RetryBackoffMs) * time.Millisecond,
		LeaseDuration:           time.Duration(value.LeaseDurationMs) * time.Millisecond,
		CreatedAt:               value.CreatedAt.Time,
		RegistrationCompletedAt: optionalTime(value.RegistrationCompletedAt),
		UpdatedAt:               value.UpdatedAt.Time,
		TerminalAt:              optionalTime(value.TerminalAt),
	}
}

func chainFromDB(value db.JobChainAssociation) ChainAssociation {
	return ChainAssociation{
		JobID:      ulidFromDB(value.JobID),
		RankID:     ulidFromDB(value.RankID),
		ChainID:    value.ChainID,
		State:      ChainState(value.State),
		CreatedAt:  value.CreatedAt.Time,
		DrainingAt: optionalTime(value.DrainingAt),
	}
}

type chainKey struct {
	rankID  ulid.ULID
	chainID int64
}

func lockChainAssociations(
	ctx context.Context,
	queries *db.Queries,
	jobID ulid.ULID,
	identities []chainKey,
) (map[chainKey]db.JobChainAssociation, error) {
	identities = sortedUniqueChainKeys(identities)
	if len(identities) == 0 {
		return map[chainKey]db.JobChainAssociation{}, nil
	}

	rankIDs := make([]uuid.UUID, len(identities))
	chainIDs := make([]int64, len(identities))
	for index, identity := range identities {
		rankIDs[index] = dbUUID(identity.rankID)
		chainIDs[index] = identity.chainID
	}

	rows, err := queries.LockChainAssociations(ctx, db.LockChainAssociationsParams{
		JobID:    dbUUID(jobID),
		RankIds:  rankIDs,
		ChainIds: chainIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("lock chain associations: %w", err)
	}

	locked := make(map[chainKey]db.JobChainAssociation, len(rows))
	for _, row := range rows {
		key := chainKey{rankID: ulidFromDB(row.RankID), chainID: row.ChainID}
		locked[key] = row
	}
	return locked, nil
}

func sortedUniqueChainKeys(values []chainKey) []chainKey {
	result := append([]chainKey(nil), values...)

	// Sorts by rank_id then chain_id
	sort.Slice(result, func(left, right int) bool {
		comparison := bytes.Compare(result[left].rankID[:], result[right].rankID[:])
		if comparison != 0 {
			return comparison < 0
		}
		return result[left].chainID < result[right].chainID
	})

	// Removes duplicates since identical rows are now adjacent
	writeIndex := 0
	for _, value := range result {
		if writeIndex > 0 && result[writeIndex-1] == value {
			continue
		}
		result[writeIndex] = value
		writeIndex++
	}
	return result[:writeIndex]
}

func normalizeDatabaseError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: %w", ErrNotFound, err)
	}

	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		switch postgresError.Code {
		case "23503", "23505":
			return fmt.Errorf("%w (%s): %w", ErrConflict, postgresError.ConstraintName, err)
		case "22023", "23502", "23514":
			return fmt.Errorf("%w (%s): %w", ErrInvalidArgument, postgresError.ConstraintName, err)
		}
	}
	return err
}
