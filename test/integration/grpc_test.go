package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/oklog/ulid/v2"
	"github.com/pressly/goose/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/tandemn-labs/chunk-manager/db/migrations"
	chunkmanagerv1 "github.com/tandemn-labs/chunk-manager/gen/go/tandemn/chunkmanager/v1"
	"github.com/tandemn-labs/chunk-manager/internal/api"
	"github.com/tandemn-labs/chunk-manager/internal/postgres"
	"github.com/tandemn-labs/chunk-manager/internal/reconcile"
)

const defaultTestDatabaseURL = "postgresql:///chunk_manager_test?host=/run/postgresql"

var testDatabaseURL string

func TestMain(m *testing.M) {
	baseDatabaseURL := os.Getenv("TEST_DATABASE_URL")
	if baseDatabaseURL == "" {
		baseDatabaseURL = defaultTestDatabaseURL
	}
	var cleanup func() error
	var err error
	testDatabaseURL, cleanup, err = prepareTestDatabase(context.Background(), baseDatabaseURL)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "prepare integration database: %v\n", err)
		os.Exit(1)
	}
	code := m.Run()
	if err := cleanup(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "clean up integration database: %v\n", err)
		code = 1
	}
	os.Exit(code)
}

func TestPlannerAndWorkerLifecycle(t *testing.T) {
	harness := newHarness(t)
	jobID := ulid.Make().String()
	rankID := ulid.Make().String()
	chain := &chunkmanagerv1.ChainIdentity{JobId: jobID, RankId: rankID, ChainId: 0}

	createRequest := &chunkmanagerv1.CreateJobRequest{
		JobId:           jobID,
		TotalChunkCount: 2,
		MaxRetries:      1,
		RetryBackoff:    durationpb.New(5 * time.Millisecond),
		LeaseDuration:   durationpb.New(time.Second),
	}
	created, err := harness.planner.CreateJob(harness.ctx, createRequest)
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if created.GetJob().GetState() != chunkmanagerv1.JobState_JOB_STATE_PENDING {
		t.Fatalf("created job state = %s, want PENDING", created.GetJob().GetState())
	}
	if _, err := harness.planner.CreateJob(harness.ctx, createRequest); err != nil {
		t.Fatalf("idempotent CreateJob: %v", err)
	}

	registerRequest := &chunkmanagerv1.RegisterChunksRequest{
		JobId: jobID,
		Chunks: []*chunkmanagerv1.ChunkRegistration{
			{ChunkId: 0, InputRef: "s3://inputs/0"},
			{ChunkId: 1, InputRef: "s3://inputs/1"},
		},
	}
	registered, err := harness.planner.RegisterChunks(harness.ctx, registerRequest)
	if err != nil {
		t.Fatalf("RegisterChunks: %v", err)
	}
	if registered.GetRegisteredCount() != 2 {
		t.Fatalf("registered count = %d, want 2", registered.GetRegisteredCount())
	}
	if _, err := harness.planner.RegisterChunks(harness.ctx, registerRequest); err != nil {
		t.Fatalf("idempotent RegisterChunks: %v", err)
	}

	if _, err := harness.planner.AddChainAssociation(harness.ctx, &chunkmanagerv1.AddChainAssociationRequest{
		Chain: chain,
	}); err != nil {
		t.Fatalf("AddChainAssociation: %v", err)
	}
	if _, err := harness.planner.AddChainAssociation(harness.ctx, &chunkmanagerv1.AddChainAssociationRequest{
		Chain: chain,
	}); err != nil {
		t.Fatalf("idempotent AddChainAssociation: %v", err)
	}

	finalized, err := harness.planner.FinalizeJobRegistration(
		harness.ctx,
		&chunkmanagerv1.FinalizeJobRegistrationRequest{JobId: jobID},
	)
	if err != nil {
		t.Fatalf("FinalizeJobRegistration: %v", err)
	}
	if finalized.GetJob().GetState() != chunkmanagerv1.JobState_JOB_STATE_RUNNING {
		t.Fatalf("finalized job state = %s, want RUNNING", finalized.GetJob().GetState())
	}
	if _, err := harness.planner.FinalizeJobRegistration(
		harness.ctx,
		&chunkmanagerv1.FinalizeJobRegistrationRequest{JobId: jobID},
	); err != nil {
		t.Fatalf("idempotent FinalizeJobRegistration: %v", err)
	}
	if _, err := harness.planner.RegisterChunks(harness.ctx, registerRequest); err != nil {
		t.Fatalf("registration replay after finalization: %v", err)
	}

	claimed, err := harness.worker.ClaimChunks(harness.ctx, &chunkmanagerv1.ClaimChunksRequest{
		Chain:     chain,
		MaxChunks: 2,
	})
	if err != nil {
		t.Fatalf("ClaimChunks: %v", err)
	}
	if claimed.GetJobState() != chunkmanagerv1.JobState_JOB_STATE_RUNNING || len(claimed.GetLeases()) != 2 {
		t.Fatalf("claim response = %+v, want RUNNING with two leases", claimed)
	}
	if claimed.GetDatabaseTime() == nil {
		t.Fatal("claim response has no database time")
	}

	first := claimed.GetLeases()[0]
	second := claimed.GetLeases()[1]
	renewed, err := harness.worker.RenewLeases(harness.ctx, &chunkmanagerv1.RenewLeasesRequest{
		Chain: chain,
		Leases: []*chunkmanagerv1.LeaseReference{
			{ChunkId: first.GetChunkId(), Generation: first.GetGeneration()},
			{ChunkId: 9999, Generation: 1},
		},
	})
	if err != nil {
		t.Fatalf("RenewLeases: %v", err)
	}
	if len(renewed.GetRenewed()) != 1 || len(renewed.GetStale()) != 1 || renewed.GetDatabaseTime() == nil {
		t.Fatalf("renew response = %+v, want one renewed and one stale", renewed)
	}

	completion := &chunkmanagerv1.CompleteChunkRequest{
		Chain:           chain,
		Lease:           &chunkmanagerv1.LeaseReference{ChunkId: first.GetChunkId(), Generation: first.GetGeneration()},
		OutputUri:       "s3://outputs/first",
		Checksum:        "sha256:first",
		OutputSizeBytes: 128,
	}
	completed, err := harness.worker.CompleteChunk(harness.ctx, completion)
	if err != nil {
		t.Fatalf("CompleteChunk: %v", err)
	}
	if completed.GetReplayed() {
		t.Fatal("first completion was marked as a replay")
	}
	replayed, err := harness.worker.CompleteChunk(harness.ctx, completion)
	if err != nil {
		t.Fatalf("completion replay: %v", err)
	}
	if !replayed.GetReplayed() {
		t.Fatal("second completion was not marked as a replay")
	}

	failed, err := harness.worker.FailChunk(harness.ctx, &chunkmanagerv1.FailChunkRequest{
		Chain:        chain,
		Lease:        &chunkmanagerv1.LeaseReference{ChunkId: second.GetChunkId(), Generation: second.GetGeneration()},
		FailureClass: "INVALID_INPUT",
		Message:      "bad record",
		Retriable:    false,
	})
	if err != nil {
		t.Fatalf("FailChunk: %v", err)
	}
	if failed.GetJobState() != chunkmanagerv1.JobState_JOB_STATE_FAILED || failed.GetRetried() {
		t.Fatalf("failure response = %+v, want terminal job failure", failed)
	}

	terminalClaim, err := harness.worker.ClaimChunks(harness.ctx, &chunkmanagerv1.ClaimChunksRequest{
		Chain:     chain,
		MaxChunks: 1,
	})
	if err != nil {
		t.Fatalf("terminal ClaimChunks: %v", err)
	}
	if terminalClaim.GetJobState() != chunkmanagerv1.JobState_JOB_STATE_FAILED || len(terminalClaim.GetLeases()) != 0 {
		t.Fatalf("terminal claim = %+v, want FAILED with no leases", terminalClaim)
	}

	job, err := harness.planner.GetJob(harness.ctx, &chunkmanagerv1.GetJobRequest{JobId: jobID})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if job.GetJob().GetSucceededChunkCount() != 1 || job.GetJob().GetFailedChunkCount() != 1 {
		t.Fatalf("job counters = succeeded %d, failed %d", job.GetJob().GetSucceededChunkCount(), job.GetJob().GetFailedChunkCount())
	}
}

func TestConcurrentClaimsDoNotDuplicateChunks(t *testing.T) {
	harness := newHarness(t)
	jobID := ulid.Make().String()
	rankID := ulid.Make().String()
	chains := []*chunkmanagerv1.ChainIdentity{
		{JobId: jobID, RankId: rankID, ChainId: 0},
		{JobId: jobID, RankId: rankID, ChainId: 1},
	}
	createRunningJob(t, harness, jobID, chains, 10, time.Second)

	var wait sync.WaitGroup
	results := make(chan *chunkmanagerv1.ClaimChunksResponse, len(chains))
	errors := make(chan error, len(chains))
	for _, chain := range chains {
		wait.Add(1)
		go func(chain *chunkmanagerv1.ChainIdentity) {
			defer wait.Done()
			response, err := harness.worker.ClaimChunks(harness.ctx, &chunkmanagerv1.ClaimChunksRequest{
				Chain:     chain,
				MaxChunks: 10,
			})
			if err != nil {
				errors <- err
				return
			}
			results <- response
		}(chain)
	}
	wait.Wait()
	close(results)
	close(errors)
	for err := range errors {
		t.Errorf("concurrent ClaimChunks: %v", err)
	}

	seen := make(map[int64]struct{})
	for result := range results {
		for _, lease := range result.GetLeases() {
			if _, exists := seen[lease.GetChunkId()]; exists {
				t.Errorf("chunk %d was leased more than once", lease.GetChunkId())
			}
			seen[lease.GetChunkId()] = struct{}{}
		}
	}
	if len(seen) != 10 {
		t.Fatalf("claimed %d unique chunks, want 10", len(seen))
	}
}

func TestDrainingReconciliationRequeuesWithoutRetryCharge(t *testing.T) {
	harness := newHarness(t)
	jobID := ulid.Make().String()
	rankID := ulid.Make().String()
	drainingChain := &chunkmanagerv1.ChainIdentity{JobId: jobID, RankId: rankID, ChainId: 0}
	replacementChain := &chunkmanagerv1.ChainIdentity{JobId: jobID, RankId: rankID, ChainId: 1}
	createRunningJob(t, harness, jobID, []*chunkmanagerv1.ChainIdentity{drainingChain, replacementChain}, 1, 10*time.Second)

	claimed, err := harness.worker.ClaimChunks(harness.ctx, &chunkmanagerv1.ClaimChunksRequest{
		Chain:     drainingChain,
		MaxChunks: 1,
	})
	if err != nil || len(claimed.GetLeases()) != 1 {
		t.Fatalf("initial ClaimChunks = %+v, %v", claimed, err)
	}
	if _, err := harness.planner.DrainChainAssociation(harness.ctx, &chunkmanagerv1.DrainChainAssociationRequest{
		Chain: drainingChain,
	}); err != nil {
		t.Fatalf("DrainChainAssociation: %v", err)
	}

	runner, err := reconcile.NewRunner(harness.store, slog.New(slog.NewTextHandler(io.Discard, nil)), reconcile.Config{
		Interval:         time.Hour,
		PageSize:         10,
		OperationTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	beforeExpiry, err := runner.Sweep(harness.ctx)
	if err != nil {
		t.Fatalf("pre-expiry Sweep: %v", err)
	}
	if beforeExpiry.Deleted != 0 || beforeExpiry.Requeued != 0 {
		t.Fatalf("pre-expiry sweep = %+v, want no cleanup", beforeExpiry)
	}

	jobUUID := uuid.UUID(ulid.MustParse(jobID))
	if _, err := harness.pool.Exec(
		harness.ctx,
		"UPDATE chunks SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE job_id = $1",
		jobUUID,
	); err != nil {
		t.Fatalf("expire draining lease: %v", err)
	}
	afterExpiry, err := runner.Sweep(harness.ctx)
	if err != nil {
		t.Fatalf("post-expiry Sweep: %v", err)
	}
	if afterExpiry.Deleted != 1 || afterExpiry.Requeued != 1 {
		t.Fatalf("post-expiry sweep = %+v, want one requeue and deletion", afterExpiry)
	}

	replacement, err := harness.worker.ClaimChunks(harness.ctx, &chunkmanagerv1.ClaimChunksRequest{
		Chain:     replacementChain,
		MaxChunks: 1,
	})
	if err != nil || len(replacement.GetLeases()) != 1 {
		t.Fatalf("replacement ClaimChunks = %+v, %v", replacement, err)
	}
	lease := replacement.GetLeases()[0]
	if lease.GetGeneration() != 2 || lease.GetRetryCount() != 0 {
		t.Fatalf("replacement lease = %+v, want generation 2 and retry count 0", lease)
	}
}

func TestRetriableFailureBackoffAndGenerationFencing(t *testing.T) {
	harness := newHarness(t)
	jobID := ulid.Make().String()
	rankID := ulid.Make().String()
	chain := &chunkmanagerv1.ChainIdentity{JobId: jobID, RankId: rankID, ChainId: 0}
	createRunningJobWithPolicy(t, harness, jobID, []*chunkmanagerv1.ChainIdentity{chain}, 1, 1, 10*time.Second, time.Second)

	firstClaim, err := harness.worker.ClaimChunks(harness.ctx, &chunkmanagerv1.ClaimChunksRequest{
		Chain:     chain,
		MaxChunks: 1,
	})
	if err != nil || len(firstClaim.GetLeases()) != 1 {
		t.Fatalf("first ClaimChunks = %+v, %v", firstClaim, err)
	}
	firstLease := firstClaim.GetLeases()[0]
	failed, err := harness.worker.FailChunk(harness.ctx, &chunkmanagerv1.FailChunkRequest{
		Chain:        chain,
		Lease:        &chunkmanagerv1.LeaseReference{ChunkId: firstLease.GetChunkId(), Generation: firstLease.GetGeneration()},
		FailureClass: "TRANSIENT_STORAGE",
		Message:      "temporary read failure",
		Retriable:    true,
	})
	if err != nil {
		t.Fatalf("retriable FailChunk: %v", err)
	}
	if !failed.GetRetried() || failed.GetNotBefore() == nil {
		t.Fatalf("failure response = %+v, want a scheduled retry", failed)
	}

	tooEarly, err := harness.worker.ClaimChunks(harness.ctx, &chunkmanagerv1.ClaimChunksRequest{
		Chain:     chain,
		MaxChunks: 1,
	})
	if err != nil {
		t.Fatalf("early ClaimChunks: %v", err)
	}
	if tooEarly.GetJobState() != chunkmanagerv1.JobState_JOB_STATE_RUNNING || len(tooEarly.GetLeases()) != 0 {
		t.Fatalf("early claim = %+v, want RUNNING with no eligible leases", tooEarly)
	}

	jobUUID := uuid.UUID(ulid.MustParse(jobID))
	if _, err := harness.pool.Exec(
		harness.ctx,
		"UPDATE chunks SET not_before = clock_timestamp() - interval '1 second' WHERE job_id = $1",
		jobUUID,
	); err != nil {
		t.Fatalf("make retry due: %v", err)
	}
	secondClaim, err := harness.worker.ClaimChunks(harness.ctx, &chunkmanagerv1.ClaimChunksRequest{
		Chain:     chain,
		MaxChunks: 1,
	})
	if err != nil || len(secondClaim.GetLeases()) != 1 {
		t.Fatalf("second ClaimChunks = %+v, %v", secondClaim, err)
	}
	secondLease := secondClaim.GetLeases()[0]
	if secondLease.GetGeneration() != firstLease.GetGeneration()+1 || secondLease.GetRetryCount() != 1 {
		t.Fatalf("second lease = %+v, want next generation and retry count 1", secondLease)
	}

	_, err = harness.worker.CompleteChunk(harness.ctx, &chunkmanagerv1.CompleteChunkRequest{
		Chain:           chain,
		Lease:           &chunkmanagerv1.LeaseReference{ChunkId: firstLease.GetChunkId(), Generation: firstLease.GetGeneration()},
		OutputUri:       "s3://outputs/stale",
		Checksum:        "sha256:stale",
		OutputSizeBytes: 1,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale completion code = %s, want FailedPrecondition: %v", status.Code(err), err)
	}

	exhausted, err := harness.worker.FailChunk(harness.ctx, &chunkmanagerv1.FailChunkRequest{
		Chain:        chain,
		Lease:        &chunkmanagerv1.LeaseReference{ChunkId: secondLease.GetChunkId(), Generation: secondLease.GetGeneration()},
		FailureClass: "TRANSIENT_STORAGE",
		Message:      "failed again",
		Retriable:    true,
	})
	if err != nil {
		t.Fatalf("exhausting FailChunk: %v", err)
	}
	if exhausted.GetRetried() || exhausted.GetJobState() != chunkmanagerv1.JobState_JOB_STATE_FAILED {
		t.Fatalf("exhausted failure = %+v, want terminal FAILED", exhausted)
	}
}

func TestCancelledJobReturnsTerminalClaimWithoutAssociation(t *testing.T) {
	harness := newHarness(t)
	jobID := ulid.Make().String()
	if _, err := harness.planner.CreateJob(harness.ctx, &chunkmanagerv1.CreateJobRequest{
		JobId:           jobID,
		TotalChunkCount: 1,
		RetryBackoff:    durationpb.New(0),
		LeaseDuration:   durationpb.New(time.Second),
	}); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	registration := &chunkmanagerv1.RegisterChunksRequest{
		JobId: jobID,
		Chunks: []*chunkmanagerv1.ChunkRegistration{{
			ChunkId:  0,
			InputRef: "s3://inputs/cancelled",
		}},
	}
	if _, err := harness.planner.RegisterChunks(harness.ctx, registration); err != nil {
		t.Fatalf("RegisterChunks: %v", err)
	}
	cancelled, err := harness.planner.CancelJob(harness.ctx, &chunkmanagerv1.CancelJobRequest{JobId: jobID})
	if err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	if cancelled.GetJob().GetState() != chunkmanagerv1.JobState_JOB_STATE_CANCELLED {
		t.Fatalf("cancelled state = %s, want CANCELLED", cancelled.GetJob().GetState())
	}
	if _, err := harness.planner.RegisterChunks(harness.ctx, registration); err != nil {
		t.Fatalf("registration replay after cancellation: %v", err)
	}

	claim, err := harness.worker.ClaimChunks(harness.ctx, &chunkmanagerv1.ClaimChunksRequest{
		Chain: &chunkmanagerv1.ChainIdentity{
			JobId:   jobID,
			RankId:  ulid.Make().String(),
			ChainId: 0,
		},
		MaxChunks: 1,
	})
	if err != nil {
		t.Fatalf("terminal ClaimChunks: %v", err)
	}
	if claim.GetJobState() != chunkmanagerv1.JobState_JOB_STATE_CANCELLED || len(claim.GetLeases()) != 0 {
		t.Fatalf("cancelled claim = %+v, want CANCELLED with no leases", claim)
	}
}

func TestCreateJobConflictUsesAlreadyExists(t *testing.T) {
	harness := newHarness(t)
	jobID := ulid.Make().String()
	request := &chunkmanagerv1.CreateJobRequest{
		JobId:           jobID,
		TotalChunkCount: 1,
		RetryBackoff:    durationpb.New(0),
		LeaseDuration:   durationpb.New(time.Second),
	}
	if _, err := harness.planner.CreateJob(harness.ctx, request); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	request.TotalChunkCount = 2
	_, err := harness.planner.CreateJob(harness.ctx, request)
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("conflicting CreateJob code = %s, want AlreadyExists: %v", status.Code(err), err)
	}
}

type harness struct {
	ctx     context.Context
	pool    *pgxpool.Pool
	store   *postgres.Store
	planner chunkmanagerv1.PlannerServiceClient
	worker  chunkmanagerv1.WorkerServiceClient
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	pool, err := postgres.OpenPool(ctx, postgres.PoolConfig{
		DatabaseURL:     testDatabaseURL,
		ApplicationName: "chunk-manager-integration-test",
	})
	if err != nil {
		t.Fatalf("OpenPool: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := postgres.CheckReady(ctx, pool); err != nil {
		t.Fatalf("CheckReady: %v", err)
	}
	store, err := postgres.NewStore(pool, postgres.StoreConfig{})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	chunkmanagerv1.RegisterPlannerServiceServer(server, api.NewPlannerServer(store))
	chunkmanagerv1.RegisterWorkerServiceServer(server, api.NewWorkerServer(store))
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(server.Stop)

	connection, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial gRPC server: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })

	return &harness{
		ctx:     ctx,
		pool:    pool,
		store:   store,
		planner: chunkmanagerv1.NewPlannerServiceClient(connection),
		worker:  chunkmanagerv1.NewWorkerServiceClient(connection),
	}
}

func createRunningJob(
	t *testing.T,
	harness *harness,
	jobID string,
	chains []*chunkmanagerv1.ChainIdentity,
	chunkCount int,
	leaseDuration time.Duration,
) {
	t.Helper()
	createRunningJobWithPolicy(t, harness, jobID, chains, chunkCount, 1, 0, leaseDuration)
}

func createRunningJobWithPolicy(
	t *testing.T,
	harness *harness,
	jobID string,
	chains []*chunkmanagerv1.ChainIdentity,
	chunkCount int,
	maxRetries int32,
	retryBackoff time.Duration,
	leaseDuration time.Duration,
) {
	t.Helper()
	_, err := harness.planner.CreateJob(harness.ctx, &chunkmanagerv1.CreateJobRequest{
		JobId:           jobID,
		TotalChunkCount: int64(chunkCount),
		MaxRetries:      maxRetries,
		RetryBackoff:    durationpb.New(retryBackoff),
		LeaseDuration:   durationpb.New(leaseDuration),
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	chunks := make([]*chunkmanagerv1.ChunkRegistration, chunkCount)
	for index := range chunks {
		chunks[index] = &chunkmanagerv1.ChunkRegistration{
			ChunkId:  int64(index),
			InputRef: fmt.Sprintf("s3://inputs/%d", index),
		}
	}
	if _, err := harness.planner.RegisterChunks(harness.ctx, &chunkmanagerv1.RegisterChunksRequest{
		JobId:  jobID,
		Chunks: chunks,
	}); err != nil {
		t.Fatalf("RegisterChunks: %v", err)
	}
	for _, chain := range chains {
		if _, err := harness.planner.AddChainAssociation(harness.ctx, &chunkmanagerv1.AddChainAssociationRequest{
			Chain: chain,
		}); err != nil {
			t.Fatalf("AddChainAssociation: %v", err)
		}
	}
	if _, err := harness.planner.FinalizeJobRegistration(
		harness.ctx,
		&chunkmanagerv1.FinalizeJobRegistrationRequest{JobId: jobID},
	); err != nil {
		t.Fatalf("FinalizeJobRegistration: %v", err)
	}
}

func prepareTestDatabase(ctx context.Context, databaseURL string) (string, func() error, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return "", nil, fmt.Errorf("parse TEST_DATABASE_URL: %w", err)
	}
	if !strings.HasSuffix(config.ConnConfig.Database, "_test") {
		return "", nil, fmt.Errorf("database %q must end in _test", config.ConnConfig.Database)
	}
	if !isLocalHost(config.ConnConfig.Host) {
		return "", nil, fmt.Errorf("database host %q is not local", config.ConnConfig.Host)
	}
	for _, fallback := range config.ConnConfig.Fallbacks {
		if !isLocalHost(fallback.Host) {
			return "", nil, fmt.Errorf("database fallback host %q is not local", fallback.Host)
		}
	}

	database, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return "", nil, fmt.Errorf("open database: %w", err)
	}
	defer database.Close()
	if err := database.PingContext(ctx); err != nil {
		return "", nil, fmt.Errorf("connect to database: %w", err)
	}

	schemaName := "chunk_manager_test_" + strings.ToLower(ulid.Make().String())
	schemaIdentifier := pgx.Identifier{schemaName}.Sanitize()
	if _, err := database.ExecContext(ctx, "CREATE SCHEMA "+schemaIdentifier); err != nil {
		return "", nil, fmt.Errorf("create isolated schema: %w", err)
	}
	cleanup := func() error {
		cleanupDatabase, err := sql.Open("pgx", databaseURL)
		if err != nil {
			return fmt.Errorf("open cleanup database: %w", err)
		}
		defer cleanupDatabase.Close()
		if _, err := cleanupDatabase.ExecContext(context.Background(), "DROP SCHEMA "+schemaIdentifier+" CASCADE"); err != nil {
			return fmt.Errorf("drop isolated schema: %w", err)
		}
		return nil
	}

	parsedURL, err := url.Parse(databaseURL)
	if err != nil {
		_ = cleanup()
		return "", nil, fmt.Errorf("parse database URL: %w", err)
	}
	query := parsedURL.Query()
	query.Set("search_path", schemaName)
	parsedURL.RawQuery = query.Encode()
	isolatedURL := parsedURL.String()

	migrationDatabase, err := sql.Open("pgx", isolatedURL)
	if err != nil {
		_ = cleanup()
		return "", nil, fmt.Errorf("open migration database: %w", err)
	}
	defer migrationDatabase.Close()
	goose.SetBaseFS(migrations.Files)
	if err := goose.SetDialect("postgres"); err != nil {
		_ = cleanup()
		return "", nil, fmt.Errorf("set migration dialect: %w", err)
	}
	if err := goose.UpContext(ctx, migrationDatabase, "."); err != nil {
		_ = cleanup()
		return "", nil, fmt.Errorf("apply migrations: %w", err)
	}
	return isolatedURL, cleanup, nil
}

func isLocalHost(host string) bool {
	return host == "localhost" || host == "127.0.0.1" || host == "::1" || strings.HasPrefix(host, "/")
}
