# Chunk Coordination Protocol

Status: Draft

This document defines the correctness contract for the chunk manager.

## 1. Overview

The chunk manager coordinates chunks of a batched inference job across chains.
It stores coordination metadata only. Chains read inputs and write outputs
directly to object storage.

PostgreSQL is the authority for jobs, chain associations, chunks, leases,
retries, and committed outputs. API processes are stateless.

## 2. Terminology

### Job

A batched inference request and its chunks. Multiple chains may work on one job.

### Chunk

The smallest independently leased and retried unit of a job. Its input is
immutable.

### Rank

A group of chains with the same model and runtime configuration. A job may have
multiple ranks. Job and rank IDs are ULIDs.

### Chain

One complete instance of an LLM model, potentially distributed across multiple
machines and GPUs. For example, Llama 70B deployed on two machines with four
GPUs each using PP=2 and TP=4 is one chain.

The placement hierarchy is:

```text
job_id -> rank_id -> chain_id
```

`chain_id` is an integer scoped to its rank. The composite
`(job_id, rank_id, chain_id)` uniquely identifies a chain. The chunk manager
only checks whether this composite identity is associated with the job; it does
not validate configuration.

The composite chain identity identifies one lifecycle and is not reused after
removal.

### Lease

Time-bounded permission for a chain to process a chunk. The lease consists of:

```text
(job_id, chunk_id, current_rank_id, current_chain_id, lease_generation, lease_expires_at)
```

A lease is valid when:

- The chunk is `LEASED`.
- The rank, chain, and generation match the chunk row.
- The job is `RUNNING`.
- The chain association is `ACTIVE` or `DRAINING`.
- Database time is earlier than `lease_expires_at`.

There is no maximum total runtime. A chain may renew a healthy lease
indefinitely. It must stop renewing work that is no longer running locally.

### Lease generation

`lease_generation` is the per-chunk fencing value. It starts at zero and is
incremented whenever a new assignment is made. It is retained in every chunk
state and never decreases.

Reopening an expired lease for the same chain does not increment the generation.
A new claim does increment it, even when the same chain receives the chunk
again.

## 3. Guarantees

The service guarantees:

- At most one valid lease for a chunk at a time.
- At most one committed output for a chunk.
- An old lease generation cannot modify a replaced chunk.
- A terminal chunk never returns to a nonterminal state.
- A `DRAINING` chain receives no new chunks.

The service does not guarantee that only one chain is physically computing a
chunk. A partitioned chain may continue after its lease expires while a
replacement starts. Only the current generation can commit.

All state transitions are transactional. Database wall-clock time must be
sampled after waiting for relevant locks. Operations lock records in this order:
job, chain associations ordered by `(rank_id, chain_id)`, then chunks ordered by
chunk ID.
After locking, operations recheck job state, both chain associations, chunk
state, generation, and expiration. PostgreSQL `clock_timestamp()` is used for
post-lock time checks.

When PostgreSQL is unavailable, claims, renewals, completions, and failures stop.
Chains may continue local computation and recover their unchanged leases after
the database returns.

## 4. Records

### Job

Required fields:

- `job_id`
- State and timestamps
- Total, succeeded, and failed chunk counts
- Maximum retries and retry backoff policy
- Input manifest reference

### Chain association

Required fields:

- `job_id`
- `rank_id`
- `chain_id`
- State: `ACTIVE` or `DRAINING`

| State | Claim new chunks | Operate existing leases |
| --- | --- | --- |
| `ACTIVE` | Yes | Yes |
| `DRAINING` | No | Yes |

The database enforces uniqueness for `(job_id, rank_id, chain_id)` and
`(job_id, chunk_id)`.

### Chunk

Required fields:

- `job_id` and `chunk_id`
- Input reference
- State
- Current rank and chain IDs when leased
- Lease generation and expiration
- Retry count and `not_before`
- Committed output metadata
- Last failure information

No attempt-history, events, or request-deduplication table is required.

## 5. State Machines

### Job states

| State | Meaning |
| --- | --- |
| `PENDING` | Chunks are being registered. |
| `RUNNING` | Active chains may claim chunks. |
| `SUCCEEDED` | Every chunk succeeded. |
| `FAILED` | Registration failed, or all chunks are terminal and at least one failed. |
| `CANCELLED` | The job was cancelled. |

A chunk failure does not stop the job. Remaining chunks continue until every
chunk is terminal. The final job state is `SUCCEEDED` if all chunks succeeded
and `FAILED` otherwise.

`PENDING` becomes `RUNNING` only after chunk registration and the total count
are committed. No chunks may be added afterward. Terminal job states have no
outgoing transitions.

### Chunk states

| State | Meaning |
| --- | --- |
| `READY` | Unleased; claimable at or after `not_before`. |
| `LEASED` | Assigned to a chain; the lease may be expired. |
| `SUCCEEDED` | Output committed. |
| `FAILED` | Non-retriable failure or retry limit reached. |
| `CANCELLED` | The job was cancelled. |

Allowed transitions:

```text
READY  -> LEASED
LEASED -> LEASED      renewal, reopening, or replacement
LEASED -> READY       retriable failure or draining-chain cleanup
LEASED -> SUCCEEDED
LEASED -> FAILED
READY/LEASED -> CANCELLED
```

When a chunk leaves `LEASED`, its current rank, chain, and expiration are
cleared. Its lease generation is retained.

## 6. API Semantics

### Claim chunks

```text
ClaimChunks(job_id, rank_id, chain_id, max_chunks)
```

One request may claim up to `max_chunks`. Chains must not request more chunks
than they can execute concurrently.

A chunk is claimable when:

- It is `READY` and `not_before` has passed.
- It is `LEASED` and its expiration has passed.

The claim transaction verifies the job and chain association, locks eligible
chunks, increments each `lease_generation`, assigns the chain, and sets
`lease_expires_at` from database time.

Replacing an expired lease from an `ACTIVE` chain consumes retry budget.
Replacing an expired lease from a `DRAINING` chain does not, because planned
termination caused the abandonment.

If an expired active-chain lease has exhausted its retry budget, the chunk
becomes `FAILED` instead of being assigned. The same transaction updates job
counters and marks the job `FAILED` if every chunk is now terminal. Other chunks
continue normally.

Claim requests are not deduplicated. If a response times out after the database
commits, those unknown leases remain unavailable until they expire. The chain
may issue a new claim, and any late response from the closed request is ignored.
This may delay work and consume retry budget, which is an accepted tradeoff.

### Renew leases

```text
RenewLeases(
  job_id,
  rank_id,
  chain_id,
  leases: [{chunk_id, lease_generation}, ...]
)
```

Each heartbeat explicitly lists the leases still running locally. The server
does not infer and renew every lease belonging to the chain.

For each matching lease, the server sets a new expiration from database time.
Renewal is allowed for `ACTIVE` and `DRAINING` chains.

If the lease expired but the chain and generation are still current, renewal
may reopen the same generation. If a replacement claim and reopening race, the
transaction that updates the chunk first wins.

There is no hard runtime limit. Continued successful heartbeats keep the lease
valid.

### Complete chunk

```text
CompleteChunk(
  job_id,
  rank_id,
  chain_id,
  chunk_id,
  lease_generation,
  output_uri,
  checksum,
  size
)
```

Completion succeeds only for the current, unexpired lease. It marks the chunk
`SUCCEEDED`, records its output, and updates job counters in one transaction.

If all chunks are terminal, the transaction marks the job `SUCCEEDED` when all
succeeded or `FAILED` when any failed.

Submitting the same generation and output after a successful completion returns
the existing success. This uses the chunk's terminal state, not a request record.

### Fail chunk

```text
FailChunk(
  job_id,
  rank_id,
  chain_id,
  chunk_id,
  lease_generation,
  failure_class,
  message
)
```

Failure is accepted only for the current, unexpired lease.

- A retriable failure increments `retry_count`, sets `not_before`, and returns
  the chunk to `READY` when budget remains.
- A non-retriable failure or exhausted retry budget makes the chunk `FAILED`.

A terminal chunk failure updates job counters but does not stop other chunks.
If every chunk is now terminal, the same transaction marks the job `FAILED`.

There is no release operation. A claimed chunk succeeds, fails, or becomes
available after its lease expires.

## 7. Retry Policy

Each job defines `max_retries` and backoff settings. `retry_count` starts at
zero.

Retry count increments when:

- A chain reports a retriable failure.
- A new claim replaces an expired lease from an `ACTIVE` chain.

Retry count does not increment when:

- The same chain reopens its expired lease.
- An expired `DRAINING` lease is replaced or cleaned up.
- The job is cancelled.

When `retry_count` reaches `max_retries`, the next retryable failure or expired
active-chain replacement makes the chunk `FAILED`.

Retry backoff is stored as `READY.not_before`. Transient infrastructure,
storage, or engine errors are normally retriable. Invalid input is normally
terminal.

## 8. Chain Draining

When placement removes a chain from a job:

1. Its association changes from `ACTIVE` to `DRAINING`.
2. The chunk manager rejects new claims from that chain.
3. The chain continues heartbeating and finishing its current chunks.
4. The Kubernetes operator force-terminates it after the external shutdown
   grace period.
5. Remaining leases expire after heartbeats stop.

The chunk manager does not terminate chains and has no additional termination
state.

A reconciliation loop cleans up each `DRAINING` chain:

1. Leave the association while the chain has any valid lease.
2. Move its expired `LEASED` chunks to `READY`, retaining their generations and
   without incrementing retry count.
3. Delete the association when no chunk remains leased to the chain.

An active chain may claim an expired draining-chain lease before cleanup. A
late reopening heartbeat and cleanup serialize on the chunk row; only one wins.

Association deletion is the final cleanup step and never invalidates a live
lease.

## 9. Output Publication

Chains write immutable generation-specific outputs:

```text
jobs/{job_id}/chunks/{chunk_id}/generations/{lease_generation}/output
```

Only output metadata committed by `CompleteChunk` is authoritative. An output
uploaded by a stale generation remains unreferenced and may be garbage-collected.

## 10. Required Failure Behavior

| Scenario | Result |
| --- | --- |
| Active chain exits | Its leases expire and are retried within policy. |
| Draining chain is terminated | Its expired leases are reassigned without retry charge. |
| Partitioned chain returns before replacement | It may reopen the same generation. |
| Replacement wins first | The old generation is stale. |
| Claim response is lost | Unknown leases expire; a later claim may assign other chunks. |
| Database is unavailable | Coordination stops until PostgreSQL returns. |
| Output upload succeeds but completion fails | The unreferenced output may be garbage-collected. |
| Job is cancelled | No state-changing lease operation is accepted; replay of an already committed identical completion may return its existing success. |
