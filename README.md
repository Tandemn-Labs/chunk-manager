# Chunk Manager

Chunk Manager coordinates chunks of a batched inference job across workers in
multiple Kubernetes clusters. It manages coordination metadata only; workers
read inputs and write outputs directly to object storage.

The service is currently under development.

## Design documents

- [Chunk coordination protocol](docs/chunk-coordination-protocol.md)

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
