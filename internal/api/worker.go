package api

import (
	"context"

	"google.golang.org/protobuf/types/known/timestamppb"

	chunkmanagerv1 "github.com/tandemn-labs/chunk-manager/gen/go/tandemn/chunkmanager/v1"
	"github.com/tandemn-labs/chunk-manager/internal/postgres"
)

type WorkerServer struct {
	chunkmanagerv1.UnimplementedWorkerServiceServer
	store Store
}

func NewWorkerServer(store Store) *WorkerServer {
	return &WorkerServer{store: store}
}

func (server *WorkerServer) ClaimChunks(
	ctx context.Context,
	request *chunkmanagerv1.ClaimChunksRequest,
) (*chunkmanagerv1.ClaimChunksResponse, error) {
	jobID, identity, err := parseChainIdentity(request.GetChain())
	if err != nil {
		return nil, rpcError(err)
	}
	result, err := server.store.ClaimChunks(ctx, postgres.ClaimChunksParams{
		JobID:     jobID,
		RankID:    identity.RankID,
		ChainID:   identity.ChainID,
		MaxChunks: request.GetMaxChunks(),
	})
	if err != nil {
		return nil, rpcError(err)
	}
	state, err := jobStateToProto(result.JobState)
	if err != nil {
		return nil, rpcError(err)
	}
	leases := make([]*chunkmanagerv1.ChunkLease, len(result.Leases))
	for index, lease := range result.Leases {
		leases[index] = &chunkmanagerv1.ChunkLease{
			ChunkId:    lease.ChunkID,
			InputRef:   lease.InputRef,
			Generation: lease.Generation,
			ExpiresAt:  timestamppb.New(lease.ExpiresAt),
			RetryCount: lease.RetryCount,
		}
	}
	return &chunkmanagerv1.ClaimChunksResponse{
		JobState:     state,
		Leases:       leases,
		DatabaseTime: timestamppb.New(result.DatabaseTime),
	}, nil
}

func (server *WorkerServer) RenewLeases(
	ctx context.Context,
	request *chunkmanagerv1.RenewLeasesRequest,
) (*chunkmanagerv1.RenewLeasesResponse, error) {
	jobID, identity, err := parseChainIdentity(request.GetChain())
	if err != nil {
		return nil, rpcError(err)
	}
	leases := make([]postgres.LeaseReference, len(request.GetLeases()))
	for index, lease := range request.GetLeases() {
		leases[index], err = parseLeaseReference(lease)
		if err != nil {
			return nil, rpcError(err)
		}
	}
	result, err := server.store.RenewLeases(ctx, postgres.RenewLeasesParams{
		JobID:   jobID,
		RankID:  identity.RankID,
		ChainID: identity.ChainID,
		Leases:  leases,
	})
	if err != nil {
		return nil, rpcError(err)
	}
	renewed := make([]*chunkmanagerv1.RenewedLease, len(result.Renewed))
	for index, lease := range result.Renewed {
		renewed[index] = &chunkmanagerv1.RenewedLease{
			Lease: &chunkmanagerv1.LeaseReference{
				ChunkId:    lease.ChunkID,
				Generation: lease.Generation,
			},
			ExpiresAt: timestamppb.New(lease.ExpiresAt),
		}
	}
	stale := make([]*chunkmanagerv1.LeaseReference, len(result.Stale))
	for index, lease := range result.Stale {
		stale[index] = &chunkmanagerv1.LeaseReference{
			ChunkId:    lease.ChunkID,
			Generation: lease.Generation,
		}
	}
	return &chunkmanagerv1.RenewLeasesResponse{
		Renewed:      renewed,
		Stale:        stale,
		DatabaseTime: timestamppb.New(result.DatabaseTime),
	}, nil
}

func (server *WorkerServer) CompleteChunk(
	ctx context.Context,
	request *chunkmanagerv1.CompleteChunkRequest,
) (*chunkmanagerv1.CompleteChunkResponse, error) {
	jobID, identity, err := parseChainIdentity(request.GetChain())
	if err != nil {
		return nil, rpcError(err)
	}
	lease, err := parseLeaseReference(request.GetLease())
	if err != nil {
		return nil, rpcError(err)
	}
	result, err := server.store.CompleteChunk(ctx, postgres.CompleteChunkParams{
		JobID:      jobID,
		RankID:     identity.RankID,
		ChainID:    identity.ChainID,
		ChunkID:    lease.ChunkID,
		Generation: lease.Generation,
		OutputURI:  request.GetOutputUri(),
		Checksum:   request.GetChecksum(),
		OutputSize: request.GetOutputSizeBytes(),
	})
	if err != nil {
		return nil, rpcError(err)
	}
	state, err := jobStateToProto(result.JobState)
	if err != nil {
		return nil, rpcError(err)
	}
	return &chunkmanagerv1.CompleteChunkResponse{JobState: state, Replayed: result.Replay}, nil
}

func (server *WorkerServer) FailChunk(
	ctx context.Context,
	request *chunkmanagerv1.FailChunkRequest,
) (*chunkmanagerv1.FailChunkResponse, error) {
	jobID, identity, err := parseChainIdentity(request.GetChain())
	if err != nil {
		return nil, rpcError(err)
	}
	lease, err := parseLeaseReference(request.GetLease())
	if err != nil {
		return nil, rpcError(err)
	}
	result, err := server.store.FailChunk(ctx, postgres.FailChunkParams{
		JobID:        jobID,
		RankID:       identity.RankID,
		ChainID:      identity.ChainID,
		ChunkID:      lease.ChunkID,
		Generation:   lease.Generation,
		FailureClass: request.GetFailureClass(),
		Message:      request.GetMessage(),
		Retriable:    request.GetRetriable(),
	})
	if err != nil {
		return nil, rpcError(err)
	}
	state, err := jobStateToProto(result.JobState)
	if err != nil {
		return nil, rpcError(err)
	}
	return &chunkmanagerv1.FailChunkResponse{JobState: state}, nil
}
