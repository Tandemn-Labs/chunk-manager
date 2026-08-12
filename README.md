# Chunk Manager

Chunk Manager coordinates chunks of a batched inference job across workers in
multiple Kubernetes clusters. It manages coordination metadata only; workers
read inputs and write outputs directly to object storage.

The service is currently in the design phase.

## Design documents

- [Chunk coordination protocol](docs/chunk-coordination-protocol.md)
