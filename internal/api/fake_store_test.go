package api

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tandemn-labs/chunk-manager/internal/postgres"
)

const (
	testJobID  = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testRankID = "01ARZ3NDEKTSV4RRFFQ69G5FAW"
)

type fakeStore struct {
	createJobFn               func(context.Context, postgres.CreateJobParams) (postgres.Job, error)
	getJobFn                  func(context.Context, ulid.ULID) (postgres.Job, error)
	registerChunksFn          func(context.Context, ulid.ULID, []postgres.ChunkRegistration) (int, error)
	finalizeJobRegistrationFn func(context.Context, ulid.ULID) (postgres.Job, error)
	cancelJobFn               func(context.Context, ulid.ULID) (postgres.Job, error)
	addChainAssociationFn     func(context.Context, ulid.ULID, postgres.ChainIdentity) (postgres.ChainAssociation, error)
	drainChainAssociationFn   func(context.Context, ulid.ULID, postgres.ChainIdentity) (postgres.ChainAssociation, error)
	claimChunksFn             func(context.Context, postgres.ClaimChunksParams) (postgres.ClaimChunksResult, error)
	renewLeasesFn             func(context.Context, postgres.RenewLeasesParams) (postgres.RenewLeasesResult, error)
	completeChunkFn           func(context.Context, postgres.CompleteChunkParams) (postgres.CompleteChunkResult, error)
	failChunkFn               func(context.Context, postgres.FailChunkParams) (postgres.FailChunkResult, error)
}

var _ Store = (*fakeStore)(nil)

func (store *fakeStore) CreateJob(
	ctx context.Context,
	params postgres.CreateJobParams,
) (postgres.Job, error) {
	return store.createJobFn(ctx, params)
}

func (store *fakeStore) GetJob(ctx context.Context, jobID ulid.ULID) (postgres.Job, error) {
	return store.getJobFn(ctx, jobID)
}

func (store *fakeStore) RegisterChunks(
	ctx context.Context,
	jobID ulid.ULID,
	chunks []postgres.ChunkRegistration,
) (int, error) {
	return store.registerChunksFn(ctx, jobID, chunks)
}

func (store *fakeStore) FinalizeJobRegistration(
	ctx context.Context,
	jobID ulid.ULID,
) (postgres.Job, error) {
	return store.finalizeJobRegistrationFn(ctx, jobID)
}

func (store *fakeStore) CancelJob(ctx context.Context, jobID ulid.ULID) (postgres.Job, error) {
	return store.cancelJobFn(ctx, jobID)
}

func (store *fakeStore) AddChainAssociation(
	ctx context.Context,
	jobID ulid.ULID,
	identity postgres.ChainIdentity,
) (postgres.ChainAssociation, error) {
	return store.addChainAssociationFn(ctx, jobID, identity)
}

func (store *fakeStore) DrainChainAssociation(
	ctx context.Context,
	jobID ulid.ULID,
	identity postgres.ChainIdentity,
) (postgres.ChainAssociation, error) {
	return store.drainChainAssociationFn(ctx, jobID, identity)
}

func (store *fakeStore) ClaimChunks(
	ctx context.Context,
	params postgres.ClaimChunksParams,
) (postgres.ClaimChunksResult, error) {
	return store.claimChunksFn(ctx, params)
}

func (store *fakeStore) RenewLeases(
	ctx context.Context,
	params postgres.RenewLeasesParams,
) (postgres.RenewLeasesResult, error) {
	return store.renewLeasesFn(ctx, params)
}

func (store *fakeStore) CompleteChunk(
	ctx context.Context,
	params postgres.CompleteChunkParams,
) (postgres.CompleteChunkResult, error) {
	return store.completeChunkFn(ctx, params)
}

func (store *fakeStore) FailChunk(
	ctx context.Context,
	params postgres.FailChunkParams,
) (postgres.FailChunkResult, error) {
	return store.failChunkFn(ctx, params)
}

func assertRPCError(t *testing.T, err error, wantCode codes.Code, wantReason string) {
	t.Helper()
	if err == nil {
		t.Fatal("RPC returned no error")
	}

	gotStatus := status.Convert(err)
	if gotStatus.Code() != wantCode {
		t.Errorf("code = %s, want %s", gotStatus.Code(), wantCode)
	}
	for _, detail := range gotStatus.Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if !ok {
			continue
		}
		if info.GetReason() != wantReason {
			t.Errorf("ErrorInfo reason = %q, want %q", info.GetReason(), wantReason)
		}
		if info.GetDomain() != errorDomain {
			t.Errorf("ErrorInfo domain = %q, want %q", info.GetDomain(), errorDomain)
		}
		return
	}
	t.Fatal("RPC error has no ErrorInfo detail")
}
