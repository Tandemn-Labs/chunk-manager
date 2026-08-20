# Chunk Manager

Chunk Manager coordinates chunks of a batched inference job across workers in
multiple Kubernetes clusters. It manages coordination metadata only; workers
read inputs and write outputs directly to object storage.

The service exposes separate gRPC APIs for planners and workers. PostgreSQL is
the source of truth and service processes are stateless.

## Design documents

- [Chunk coordination protocol](docs/chunk-coordination-protocol.md)
- [Python lifecycle example](examples/python/README.md)

## PostgreSQL

PostgreSQL is the authority for jobs, chain associations, chunk state, leases,
retries, and committed output metadata. Coordination operations must connect to
a writable primary.

The initial schema is in `db/migrations`. Apply it with the embedded Goose
migration command:

```sh
export DATABASE_URL='postgresql://user@host/chunk_manager'
go run ./cmd/dbmigrate up
```

The command also supports `down`, `status`, and `version`.

SQL queries are in `db/queries`; generated `pgx/v5` code is committed under
`internal/postgres/db`. Regenerate it with the pinned sqlc version:

```sh
go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.29.0 generate
```

The handwritten store in `internal/postgres` owns transaction boundaries, lock
ordering, database-time checks, and state-machine validation. Placement must
guarantee that `(job_id, rank_id, chain_id)` identities are never reused after
a draining association is deleted.

Lossy database restore is not safe for resumable jobs because it can roll lease
generations backward. Until the protocol includes a recovery epoch, all workers
must be fenced and nonterminal jobs recreated after such a restore.

## Protobuf

The versioned API is in `proto/tandemn/chunkmanager/v1`. Generated Go code is
committed under `gen/go`. Lint and regenerate it with the local Buf CLI:

```sh
buf lint
buf generate
```

## Running the service

Apply migrations, then start the gRPC server:

```sh
export DATABASE_URL='postgresql:///chunkManagement?host=/run/postgresql'
go run ./cmd/dbmigrate up
go run ./cmd/chunk-manager
```

The gRPC listener defaults to `:9090`. The standard gRPC health service publishes
`liveness` and `readiness` service names. Reflection is enabled for development
and troubleshooting.

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `DATABASE_URL` | required | PostgreSQL connection string for a writable primary. |
| `GRPC_LISTEN_ADDR` | `:9090` | TCP address for the planner and worker gRPC APIs, health service, and reflection. |
| `POSTGRES_MAX_CONNECTIONS` | pgx default | Maximum number of connections in the PostgreSQL pool. |
| `STORE_LOCK_TIMEOUT` | disabled | Maximum time a store transaction waits to acquire a PostgreSQL lock. |
| `RECONCILE_INTERVAL` | `30s` | Interval between draining-chain reconciliation sweeps. |
| `RECONCILE_OPERATION_TIMEOUT` | `10s` | Timeout for each draining-chain reconciliation operation. |
| `HEALTH_CHECK_INTERVAL` | `5s` | Interval and per-check timeout for PostgreSQL readiness checks. |
| `SHUTDOWN_TIMEOUT` | `15s` | Overall deadline for graceful service shutdown. |
| `LOG_LEVEL` | `INFO` | Minimum severity emitted by the structured logger. |

The initial service is plaintext and has no authentication.

## Testing

Integration tests use an existing local PostgreSQL server. Create the dedicated
database once, then run the suite:

```sh
createdb chunk_manager_test
go test ./...
```

Set `TEST_DATABASE_URL` to use another local database. The integration harness
refuses remote hosts and database names that do not end in `_test`; it creates
and drops an isolated schema for each test process.
