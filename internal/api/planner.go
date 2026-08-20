package api

import (
	"context"

	chunkmanagerv1 "github.com/tandemn-labs/chunk-manager/gen/go/tandemn/chunkmanager/v1"
	"github.com/tandemn-labs/chunk-manager/internal/postgres"
)

type PlannerServer struct {
	chunkmanagerv1.UnimplementedPlannerServiceServer
	store Store
}

func NewPlannerServer(store Store) *PlannerServer {
	return &PlannerServer{store: store}
}

func (server *PlannerServer) CreateJob(
	ctx context.Context,
	request *chunkmanagerv1.CreateJobRequest,
) (*chunkmanagerv1.CreateJobResponse, error) {
	jobID, err := parseULID(request.GetJobId(), "job_id")
	if err != nil {
		return nil, rpcError(err)
	}
	retryBackoff, err := parseDuration(request.GetRetryBackoff(), "retry_backoff")
	if err != nil {
		return nil, rpcError(err)
	}
	leaseDuration, err := parseDuration(request.GetLeaseDuration(), "lease_duration")
	if err != nil {
		return nil, rpcError(err)
	}

	job, err := server.store.CreateJob(ctx, postgres.CreateJobParams{
		JobID:           jobID,
		TotalChunkCount: request.GetTotalChunkCount(),
		MaxRetries:      request.GetMaxRetries(),
		RetryBackoff:    retryBackoff,
		LeaseDuration:   leaseDuration,
	})
	if err != nil {
		return nil, rpcError(err)
	}
	response, err := jobToProto(job)
	if err != nil {
		return nil, rpcError(err)
	}
	return &chunkmanagerv1.CreateJobResponse{Job: response}, nil
}

func (server *PlannerServer) GetJob(
	ctx context.Context,
	request *chunkmanagerv1.GetJobRequest,
) (*chunkmanagerv1.GetJobResponse, error) {
	jobID, err := parseULID(request.GetJobId(), "job_id")
	if err != nil {
		return nil, rpcError(err)
	}
	job, err := server.store.GetJob(ctx, jobID)
	if err != nil {
		return nil, rpcError(err)
	}
	response, err := jobToProto(job)
	if err != nil {
		return nil, rpcError(err)
	}
	return &chunkmanagerv1.GetJobResponse{Job: response}, nil
}

func (server *PlannerServer) RegisterChunks(
	ctx context.Context,
	request *chunkmanagerv1.RegisterChunksRequest,
) (*chunkmanagerv1.RegisterChunksResponse, error) {
	jobID, err := parseULID(request.GetJobId(), "job_id")
	if err != nil {
		return nil, rpcError(err)
	}
	chunks := make([]postgres.ChunkRegistration, len(request.GetChunks()))
	for index, chunk := range request.GetChunks() {
		if chunk == nil {
			return nil, rpcError(postgres.ErrInvalidArgument)
		}
		chunks[index] = postgres.ChunkRegistration{
			ChunkID:  chunk.GetChunkId(),
			InputRef: chunk.GetInputRef(),
		}
	}
	registered, err := server.store.RegisterChunks(ctx, jobID, chunks)
	if err != nil {
		return nil, rpcError(err)
	}
	return &chunkmanagerv1.RegisterChunksResponse{RegisteredCount: int32(registered)}, nil
}

func (server *PlannerServer) FinalizeJobRegistration(
	ctx context.Context,
	request *chunkmanagerv1.FinalizeJobRegistrationRequest,
) (*chunkmanagerv1.FinalizeJobRegistrationResponse, error) {
	jobID, err := parseULID(request.GetJobId(), "job_id")
	if err != nil {
		return nil, rpcError(err)
	}
	job, err := server.store.FinalizeJobRegistration(ctx, jobID)
	if err != nil {
		return nil, rpcError(err)
	}
	response, err := jobToProto(job)
	if err != nil {
		return nil, rpcError(err)
	}
	return &chunkmanagerv1.FinalizeJobRegistrationResponse{Job: response}, nil
}

func (server *PlannerServer) CancelJob(
	ctx context.Context,
	request *chunkmanagerv1.CancelJobRequest,
) (*chunkmanagerv1.CancelJobResponse, error) {
	jobID, err := parseULID(request.GetJobId(), "job_id")
	if err != nil {
		return nil, rpcError(err)
	}
	job, err := server.store.CancelJob(ctx, jobID)
	if err != nil {
		return nil, rpcError(err)
	}
	response, err := jobToProto(job)
	if err != nil {
		return nil, rpcError(err)
	}
	return &chunkmanagerv1.CancelJobResponse{Job: response}, nil
}

func (server *PlannerServer) AddChainAssociation(
	ctx context.Context,
	request *chunkmanagerv1.AddChainAssociationRequest,
) (*chunkmanagerv1.AddChainAssociationResponse, error) {
	jobID, identity, err := parseChainIdentity(request.GetChain())
	if err != nil {
		return nil, rpcError(err)
	}
	association, err := server.store.AddChainAssociation(ctx, jobID, identity)
	if err != nil {
		return nil, rpcError(err)
	}
	response, err := associationToProto(association)
	if err != nil {
		return nil, rpcError(err)
	}
	return &chunkmanagerv1.AddChainAssociationResponse{Association: response}, nil
}

func (server *PlannerServer) DrainChainAssociation(
	ctx context.Context,
	request *chunkmanagerv1.DrainChainAssociationRequest,
) (*chunkmanagerv1.DrainChainAssociationResponse, error) {
	jobID, identity, err := parseChainIdentity(request.GetChain())
	if err != nil {
		return nil, rpcError(err)
	}
	association, err := server.store.DrainChainAssociation(ctx, jobID, identity)
	if err != nil {
		return nil, rpcError(err)
	}
	response, err := associationToProto(association)
	if err != nil {
		return nil, rpcError(err)
	}
	return &chunkmanagerv1.DrainChainAssociationResponse{Association: response}, nil
}
