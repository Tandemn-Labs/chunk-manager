package api

import (
	"context"

	"github.com/oklog/ulid/v2"

	"github.com/tandemn-labs/chunk-manager/internal/postgres"
)

type Store interface {
	CreateJob(context.Context, postgres.CreateJobParams) (postgres.Job, error)
	GetJob(context.Context, ulid.ULID) (postgres.Job, error)
	RegisterChunks(context.Context, ulid.ULID, []postgres.ChunkRegistration) (int, error)
	FinalizeJobRegistration(context.Context, ulid.ULID) (postgres.Job, error)
	CancelJob(context.Context, ulid.ULID) (postgres.Job, error)
	AddChainAssociation(context.Context, ulid.ULID, postgres.ChainIdentity) (postgres.ChainAssociation, error)
	DrainChainAssociation(context.Context, ulid.ULID, postgres.ChainIdentity) (postgres.ChainAssociation, error)
	ClaimChunks(context.Context, postgres.ClaimChunksParams) (postgres.ClaimChunksResult, error)
	RenewLeases(context.Context, postgres.RenewLeasesParams) (postgres.RenewLeasesResult, error)
	CompleteChunk(context.Context, postgres.CompleteChunkParams) (postgres.CompleteChunkResult, error)
	FailChunk(context.Context, postgres.FailChunkParams) (postgres.FailChunkResult, error)
}
