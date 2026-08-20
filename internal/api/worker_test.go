package api

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	chunkmanagerv1 "github.com/tandemn-labs/chunk-manager/gen/go/tandemn/chunkmanager/v1"
	"github.com/tandemn-labs/chunk-manager/internal/postgres"
)

func TestClaimChunksMapsRunningLeasesAndDatabaseTime(t *testing.T) {
	jobID := ulid.MustParse(testJobID)
	rankID := ulid.MustParse(testRankID)
	expiresAt := time.Date(2026, time.August, 19, 10, 1, 0, 0, time.UTC)
	databaseTime := expiresAt.Add(-30 * time.Second)
	var gotParams postgres.ClaimChunksParams
	store := &fakeStore{
		claimChunksFn: func(_ context.Context, params postgres.ClaimChunksParams) (postgres.ClaimChunksResult, error) {
			gotParams = params
			return postgres.ClaimChunksResult{
				JobState: postgres.JobStateRunning,
				Leases: []postgres.Lease{{
					ChunkID:    9,
					InputRef:   "s3://inputs/9",
					Generation: 3,
					ExpiresAt:  expiresAt,
					RetryCount: 2,
				}},
				DatabaseTime: databaseTime,
			}, nil
		},
	}

	response, err := NewWorkerServer(store).ClaimChunks(context.Background(), &chunkmanagerv1.ClaimChunksRequest{
		Chain:     testChainIdentity(),
		MaxChunks: 5,
	})
	if err != nil {
		t.Fatalf("ClaimChunks returned an error: %v", err)
	}
	wantParams := postgres.ClaimChunksParams{JobID: jobID, RankID: rankID, ChainID: 7, MaxChunks: 5}
	if !reflect.DeepEqual(gotParams, wantParams) {
		t.Errorf("ClaimChunks params = %+v, want %+v", gotParams, wantParams)
	}
	wantResponse := &chunkmanagerv1.ClaimChunksResponse{
		JobState: chunkmanagerv1.JobState_JOB_STATE_RUNNING,
		Leases: []*chunkmanagerv1.ChunkLease{{
			ChunkId:    9,
			InputRef:   "s3://inputs/9",
			Generation: 3,
			ExpiresAt:  timestamppb.New(expiresAt),
			RetryCount: 2,
		}},
		DatabaseTime: timestamppb.New(databaseTime),
	}
	if !proto.Equal(response, wantResponse) {
		t.Errorf("ClaimChunks response = %v, want %v", response, wantResponse)
	}
}

func TestClaimChunksMapsTerminalStateWithNoLeases(t *testing.T) {
	databaseTime := time.Date(2026, time.August, 19, 10, 2, 0, 0, time.UTC)
	store := &fakeStore{
		claimChunksFn: func(context.Context, postgres.ClaimChunksParams) (postgres.ClaimChunksResult, error) {
			return postgres.ClaimChunksResult{
				JobState:     postgres.JobStateSucceeded,
				DatabaseTime: databaseTime,
			}, nil
		},
	}

	response, err := NewWorkerServer(store).ClaimChunks(context.Background(), &chunkmanagerv1.ClaimChunksRequest{
		Chain:     testChainIdentity(),
		MaxChunks: 5,
	})
	if err != nil {
		t.Fatalf("ClaimChunks returned an error: %v", err)
	}
	if response.GetJobState() != chunkmanagerv1.JobState_JOB_STATE_SUCCEEDED {
		t.Errorf("job state = %s, want %s", response.GetJobState(), chunkmanagerv1.JobState_JOB_STATE_SUCCEEDED)
	}
	if len(response.GetLeases()) != 0 {
		t.Errorf("leases = %v, want empty", response.GetLeases())
	}
	if !response.GetDatabaseTime().AsTime().Equal(databaseTime) {
		t.Errorf("database time = %v, want %v", response.GetDatabaseTime().AsTime(), databaseTime)
	}
}

func TestRenewLeasesMapsRequestAndResponse(t *testing.T) {
	jobID := ulid.MustParse(testJobID)
	rankID := ulid.MustParse(testRankID)
	expiresAt := time.Date(2026, time.August, 19, 10, 3, 0, 0, time.UTC)
	databaseTime := expiresAt.Add(-time.Second)
	var gotParams postgres.RenewLeasesParams
	store := &fakeStore{
		renewLeasesFn: func(_ context.Context, params postgres.RenewLeasesParams) (postgres.RenewLeasesResult, error) {
			gotParams = params
			return postgres.RenewLeasesResult{
				Renewed:      []postgres.RenewedLease{{ChunkID: 10, Generation: 4, ExpiresAt: expiresAt}},
				Stale:        []postgres.LeaseReference{{ChunkID: 11, Generation: 2}},
				DatabaseTime: databaseTime,
			}, nil
		},
	}
	request := &chunkmanagerv1.RenewLeasesRequest{
		Chain: testChainIdentity(),
		Leases: []*chunkmanagerv1.LeaseReference{
			{ChunkId: 10, Generation: 4},
			{ChunkId: 11, Generation: 2},
		},
	}

	response, err := NewWorkerServer(store).RenewLeases(context.Background(), request)
	if err != nil {
		t.Fatalf("RenewLeases returned an error: %v", err)
	}
	wantParams := postgres.RenewLeasesParams{
		JobID:   jobID,
		RankID:  rankID,
		ChainID: 7,
		Leases: []postgres.LeaseReference{
			{ChunkID: 10, Generation: 4},
			{ChunkID: 11, Generation: 2},
		},
	}
	if !reflect.DeepEqual(gotParams, wantParams) {
		t.Errorf("RenewLeases params = %+v, want %+v", gotParams, wantParams)
	}
	wantResponse := &chunkmanagerv1.RenewLeasesResponse{
		Renewed: []*chunkmanagerv1.RenewedLease{{
			Lease:     &chunkmanagerv1.LeaseReference{ChunkId: 10, Generation: 4},
			ExpiresAt: timestamppb.New(expiresAt),
		}},
		Stale:        []*chunkmanagerv1.LeaseReference{{ChunkId: 11, Generation: 2}},
		DatabaseTime: timestamppb.New(databaseTime),
	}
	if !proto.Equal(response, wantResponse) {
		t.Errorf("RenewLeases response = %v, want %v", response, wantResponse)
	}
}

func TestCompleteChunkMapsRequestAndResponse(t *testing.T) {
	jobID := ulid.MustParse(testJobID)
	rankID := ulid.MustParse(testRankID)
	var gotParams postgres.CompleteChunkParams
	store := &fakeStore{
		completeChunkFn: func(_ context.Context, params postgres.CompleteChunkParams) (postgres.CompleteChunkResult, error) {
			gotParams = params
			return postgres.CompleteChunkResult{JobState: postgres.JobStateSucceeded, Replay: true}, nil
		},
	}

	response, err := NewWorkerServer(store).CompleteChunk(context.Background(), &chunkmanagerv1.CompleteChunkRequest{
		Chain:           testChainIdentity(),
		Lease:           &chunkmanagerv1.LeaseReference{ChunkId: 12, Generation: 5},
		OutputUri:       "s3://outputs/12",
		Checksum:        "sha256:abc",
		OutputSizeBytes: 4096,
	})
	if err != nil {
		t.Fatalf("CompleteChunk returned an error: %v", err)
	}
	wantParams := postgres.CompleteChunkParams{
		JobID:      jobID,
		RankID:     rankID,
		ChainID:    7,
		ChunkID:    12,
		Generation: 5,
		OutputURI:  "s3://outputs/12",
		Checksum:   "sha256:abc",
		OutputSize: 4096,
	}
	if !reflect.DeepEqual(gotParams, wantParams) {
		t.Errorf("CompleteChunk params = %+v, want %+v", gotParams, wantParams)
	}
	wantResponse := &chunkmanagerv1.CompleteChunkResponse{
		JobState: chunkmanagerv1.JobState_JOB_STATE_SUCCEEDED,
		Replayed: true,
	}
	if !proto.Equal(response, wantResponse) {
		t.Errorf("CompleteChunk response = %v, want %v", response, wantResponse)
	}
}

func TestFailChunkMapsRequestAndResponse(t *testing.T) {
	jobID := ulid.MustParse(testJobID)
	rankID := ulid.MustParse(testRankID)
	var gotParams postgres.FailChunkParams
	store := &fakeStore{
		failChunkFn: func(_ context.Context, params postgres.FailChunkParams) (postgres.FailChunkResult, error) {
			gotParams = params
			return postgres.FailChunkResult{JobState: postgres.JobStateRunning}, nil
		},
	}

	response, err := NewWorkerServer(store).FailChunk(context.Background(), &chunkmanagerv1.FailChunkRequest{
		Chain:        testChainIdentity(),
		Lease:        &chunkmanagerv1.LeaseReference{ChunkId: 13, Generation: 6},
		FailureClass: "transient",
		Message:      "worker overloaded",
		Retriable:    true,
	})
	if err != nil {
		t.Fatalf("FailChunk returned an error: %v", err)
	}
	wantParams := postgres.FailChunkParams{
		JobID:        jobID,
		RankID:       rankID,
		ChainID:      7,
		ChunkID:      13,
		Generation:   6,
		FailureClass: "transient",
		Message:      "worker overloaded",
		Retriable:    true,
	}
	if !reflect.DeepEqual(gotParams, wantParams) {
		t.Errorf("FailChunk params = %+v, want %+v", gotParams, wantParams)
	}
	wantResponse := &chunkmanagerv1.FailChunkResponse{JobState: chunkmanagerv1.JobState_JOB_STATE_RUNNING}
	if !proto.Equal(response, wantResponse) {
		t.Errorf("FailChunk response = %v, want %v", response, wantResponse)
	}
}

func TestStoreErrorsMapToRPCStatus(t *testing.T) {
	tests := []struct {
		name       string
		storeErr   error
		wantCode   codes.Code
		wantReason string
	}{
		{name: "stale lease", storeErr: postgres.ErrStaleLease, wantCode: codes.FailedPrecondition, wantReason: "STALE_LEASE"},
		{name: "conflict", storeErr: postgres.ErrConflict, wantCode: codes.AlreadyExists, wantReason: "CONFLICT"},
		{name: "unavailable", storeErr: postgres.ErrUnavailable, wantCode: codes.Unavailable, wantReason: "POSTGRESQL_UNAVAILABLE"},
		{name: "not found", storeErr: postgres.ErrNotFound, wantCode: codes.NotFound, wantReason: "NOT_FOUND"},
		{name: "invalid state", storeErr: postgres.ErrInvalidState, wantCode: codes.FailedPrecondition, wantReason: "INVALID_STATE"},
		{name: "aborted", storeErr: postgres.ErrAborted, wantCode: codes.Aborted, wantReason: "TRANSACTION_ABORTED"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &fakeStore{
				completeChunkFn: func(context.Context, postgres.CompleteChunkParams) (postgres.CompleteChunkResult, error) {
					return postgres.CompleteChunkResult{}, fmt.Errorf("store operation: %w", test.storeErr)
				},
			}
			_, err := NewWorkerServer(store).CompleteChunk(context.Background(), &chunkmanagerv1.CompleteChunkRequest{
				Chain: testChainIdentity(),
				Lease: &chunkmanagerv1.LeaseReference{ChunkId: 1, Generation: 1},
			})
			assertRPCError(t, err, test.wantCode, test.wantReason)
		})
	}
}

func testChainIdentity() *chunkmanagerv1.ChainIdentity {
	return &chunkmanagerv1.ChainIdentity{JobId: testJobID, RankId: testRankID, ChainId: 7}
}
