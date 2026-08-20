package api

import (
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	chunkmanagerv1 "github.com/tandemn-labs/chunk-manager/gen/go/tandemn/chunkmanager/v1"
	"github.com/tandemn-labs/chunk-manager/internal/postgres"
)

func parseULID(value string, field string) (ulid.ULID, error) {
	id, err := ulid.ParseStrict(value)
	if err != nil || id.String() != value {
		return ulid.ULID{}, fmt.Errorf("%w: %s must be a canonical ULID", postgres.ErrInvalidArgument, field)
	}
	return id, nil
}

func parseDuration(value *durationpb.Duration, field string) (time.Duration, error) {
	if value == nil {
		return 0, fmt.Errorf("%w: %s is required", postgres.ErrInvalidArgument, field)
	}
	if err := value.CheckValid(); err != nil {
		return 0, fmt.Errorf("%w: invalid %s: %v", postgres.ErrInvalidArgument, field, err)
	}
	duration := value.AsDuration()
	roundTrip := durationpb.New(duration)
	if roundTrip.GetSeconds() != value.GetSeconds() || roundTrip.GetNanos() != value.GetNanos() {
		return 0, fmt.Errorf("%w: %s is outside the supported range", postgres.ErrInvalidArgument, field)
	}
	if duration%time.Millisecond != 0 {
		return 0, fmt.Errorf("%w: %s must use whole milliseconds", postgres.ErrInvalidArgument, field)
	}
	return duration, nil
}

func parseChainIdentity(value *chunkmanagerv1.ChainIdentity) (ulid.ULID, postgres.ChainIdentity, error) {
	if value == nil {
		return ulid.ULID{}, postgres.ChainIdentity{}, fmt.Errorf(
			"%w: chain identity is required",
			postgres.ErrInvalidArgument,
		)
	}
	jobID, err := parseULID(value.GetJobId(), "job_id")
	if err != nil {
		return ulid.ULID{}, postgres.ChainIdentity{}, err
	}
	rankID, err := parseULID(value.GetRankId(), "rank_id")
	if err != nil {
		return ulid.ULID{}, postgres.ChainIdentity{}, err
	}
	if value.GetChainId() < 0 {
		return ulid.ULID{}, postgres.ChainIdentity{}, fmt.Errorf(
			"%w: chain_id cannot be negative",
			postgres.ErrInvalidArgument,
		)
	}
	return jobID, postgres.ChainIdentity{RankID: rankID, ChainID: value.GetChainId()}, nil
}

func parseLeaseReference(value *chunkmanagerv1.LeaseReference) (postgres.LeaseReference, error) {
	if value == nil {
		return postgres.LeaseReference{}, fmt.Errorf(
			"%w: lease reference is required",
			postgres.ErrInvalidArgument,
		)
	}
	if value.GetChunkId() < 0 || value.GetGeneration() <= 0 {
		return postgres.LeaseReference{}, fmt.Errorf(
			"%w: invalid lease reference",
			postgres.ErrInvalidArgument,
		)
	}
	return postgres.LeaseReference{
		ChunkID:    value.GetChunkId(),
		Generation: value.GetGeneration(),
	}, nil
}

func jobToProto(job postgres.Job) (*chunkmanagerv1.Job, error) {
	state, err := jobStateToProto(job.State)
	if err != nil {
		return nil, err
	}
	return &chunkmanagerv1.Job{
		JobId:                   job.ID.String(),
		State:                   state,
		TotalChunkCount:         job.TotalChunkCount,
		SucceededChunkCount:     job.SucceededChunkCount,
		FailedChunkCount:        job.FailedChunkCount,
		MaxRetries:              job.MaxRetries,
		RetryBackoff:            durationpb.New(job.RetryBackoff),
		LeaseDuration:           durationpb.New(job.LeaseDuration),
		CreatedAt:               timestamppb.New(job.CreatedAt),
		RegistrationCompletedAt: optionalTimestamp(job.RegistrationCompletedAt),
		UpdatedAt:               timestamppb.New(job.UpdatedAt),
		TerminalAt:              optionalTimestamp(job.TerminalAt),
	}, nil
}

func associationToProto(association postgres.ChainAssociation) (*chunkmanagerv1.ChainAssociation, error) {
	state, err := chainStateToProto(association.State)
	if err != nil {
		return nil, err
	}
	return &chunkmanagerv1.ChainAssociation{
		Identity: &chunkmanagerv1.ChainIdentity{
			JobId:   association.JobID.String(),
			RankId:  association.RankID.String(),
			ChainId: association.ChainID,
		},
		State:      state,
		CreatedAt:  timestamppb.New(association.CreatedAt),
		DrainingAt: optionalTimestamp(association.DrainingAt),
	}, nil
}

func jobStateToProto(state postgres.JobState) (chunkmanagerv1.JobState, error) {
	switch state {
	case postgres.JobStatePending:
		return chunkmanagerv1.JobState_JOB_STATE_PENDING, nil
	case postgres.JobStateRunning:
		return chunkmanagerv1.JobState_JOB_STATE_RUNNING, nil
	case postgres.JobStateSucceeded:
		return chunkmanagerv1.JobState_JOB_STATE_SUCCEEDED, nil
	case postgres.JobStateFailed:
		return chunkmanagerv1.JobState_JOB_STATE_FAILED, nil
	case postgres.JobStateCancelled:
		return chunkmanagerv1.JobState_JOB_STATE_CANCELLED, nil
	default:
		return chunkmanagerv1.JobState_JOB_STATE_UNSPECIFIED, fmt.Errorf("unknown job state %q", state)
	}
}

func chainStateToProto(state postgres.ChainState) (chunkmanagerv1.ChainState, error) {
	switch state {
	case postgres.ChainStateActive:
		return chunkmanagerv1.ChainState_CHAIN_STATE_ACTIVE, nil
	case postgres.ChainStateDraining:
		return chunkmanagerv1.ChainState_CHAIN_STATE_DRAINING, nil
	default:
		return chunkmanagerv1.ChainState_CHAIN_STATE_UNSPECIFIED, fmt.Errorf("unknown chain state %q", state)
	}
}

func optionalTimestamp(value *time.Time) *timestamppb.Timestamp {
	if value == nil {
		return nil
	}
	return timestamppb.New(*value)
}
