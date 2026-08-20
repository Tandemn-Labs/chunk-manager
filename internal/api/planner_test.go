package api

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	chunkmanagerv1 "github.com/tandemn-labs/chunk-manager/gen/go/tandemn/chunkmanager/v1"
	"github.com/tandemn-labs/chunk-manager/internal/postgres"
)

func TestGetJobRejectsInvalidOrNoncanonicalULID(t *testing.T) {
	tests := []struct {
		name  string
		jobID string
	}{
		{name: "invalid", jobID: "not-a-ulid"},
		{name: "noncanonical", jobID: strings.ToLower(testJobID)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			storeCalled := false
			store := &fakeStore{
				getJobFn: func(context.Context, ulid.ULID) (postgres.Job, error) {
					storeCalled = true
					return postgres.Job{}, nil
				},
			}

			response, err := NewPlannerServer(store).GetJob(context.Background(), &chunkmanagerv1.GetJobRequest{
				JobId: test.jobID,
			})
			if response != nil {
				t.Errorf("response = %v, want nil", response)
			}
			assertRPCError(t, err, codes.InvalidArgument, "INVALID_ARGUMENT")
			if storeCalled {
				t.Error("store was called")
			}
		})
	}
}

func TestCreateJobMapsRequestAndResponse(t *testing.T) {
	jobID := ulid.MustParse(testJobID)
	createdAt := time.Date(2026, time.August, 19, 10, 0, 0, 123_000_000, time.UTC)
	registrationCompletedAt := createdAt.Add(time.Second)
	updatedAt := createdAt.Add(2 * time.Minute)
	terminalAt := updatedAt.Add(time.Second)
	returnedJob := postgres.Job{
		ID:                      jobID,
		State:                   postgres.JobStateFailed,
		TotalChunkCount:         12,
		SucceededChunkCount:     10,
		FailedChunkCount:        2,
		MaxRetries:              4,
		RetryBackoff:            1500 * time.Millisecond,
		LeaseDuration:           45 * time.Second,
		CreatedAt:               createdAt,
		RegistrationCompletedAt: &registrationCompletedAt,
		UpdatedAt:               updatedAt,
		TerminalAt:              &terminalAt,
	}
	var gotParams postgres.CreateJobParams
	store := &fakeStore{
		createJobFn: func(_ context.Context, params postgres.CreateJobParams) (postgres.Job, error) {
			gotParams = params
			return returnedJob, nil
		},
	}

	response, err := NewPlannerServer(store).CreateJob(context.Background(), &chunkmanagerv1.CreateJobRequest{
		JobId:           testJobID,
		TotalChunkCount: 12,
		MaxRetries:      4,
		RetryBackoff:    durationpb.New(1500 * time.Millisecond),
		LeaseDuration:   durationpb.New(45 * time.Second),
	})
	if err != nil {
		t.Fatalf("CreateJob returned an error: %v", err)
	}
	wantParams := postgres.CreateJobParams{
		JobID:           jobID,
		TotalChunkCount: 12,
		MaxRetries:      4,
		RetryBackoff:    1500 * time.Millisecond,
		LeaseDuration:   45 * time.Second,
	}
	if !reflect.DeepEqual(gotParams, wantParams) {
		t.Errorf("CreateJob params = %+v, want %+v", gotParams, wantParams)
	}
	wantResponse := &chunkmanagerv1.CreateJobResponse{Job: &chunkmanagerv1.Job{
		JobId:                   testJobID,
		State:                   chunkmanagerv1.JobState_JOB_STATE_FAILED,
		TotalChunkCount:         12,
		SucceededChunkCount:     10,
		FailedChunkCount:        2,
		MaxRetries:              4,
		RetryBackoff:            durationpb.New(1500 * time.Millisecond),
		LeaseDuration:           durationpb.New(45 * time.Second),
		CreatedAt:               timestamppb.New(createdAt),
		RegistrationCompletedAt: timestamppb.New(registrationCompletedAt),
		UpdatedAt:               timestamppb.New(updatedAt),
		TerminalAt:              timestamppb.New(terminalAt),
	}}
	if !proto.Equal(response, wantResponse) {
		t.Errorf("CreateJob response = %v, want %v", response, wantResponse)
	}
}
