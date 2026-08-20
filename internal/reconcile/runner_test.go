package reconcile

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/tandemn-labs/chunk-manager/internal/postgres"
)

type fakeStore struct {
	listFn      func(context.Context, *postgres.ChainAssociationCursor, int32) ([]postgres.ChainAssociation, error)
	reconcileFn func(context.Context, ulid.ULID, postgres.ChainIdentity) (postgres.ReconcileDrainingResult, error)
}

func (store *fakeStore) ListDrainingChainAssociations(
	ctx context.Context,
	cursor *postgres.ChainAssociationCursor,
	limit int32,
) ([]postgres.ChainAssociation, error) {
	if store.listFn == nil {
		return nil, nil
	}
	return store.listFn(ctx, cursor, limit)
}

func (store *fakeStore) ReconcileDrainingAssociation(
	ctx context.Context,
	jobID ulid.ULID,
	identity postgres.ChainIdentity,
) (postgres.ReconcileDrainingResult, error) {
	if store.reconcileFn == nil {
		return postgres.ReconcileDrainingResult{}, nil
	}
	return store.reconcileFn(ctx, jobID, identity)
}

func TestNewRunnerAppliesDefaults(t *testing.T) {
	runner, err := NewRunner(&fakeStore{}, nil, Config{})
	if err != nil {
		t.Fatalf("NewRunner returned an error: %v", err)
	}

	if runner.interval != defaultInterval {
		t.Errorf("interval = %v, want %v", runner.interval, defaultInterval)
	}
	if runner.pageSize != defaultPageSize {
		t.Errorf("page size = %d, want %d", runner.pageSize, defaultPageSize)
	}
	if runner.operationTimeout != defaultOperationTimeout {
		t.Errorf("operation timeout = %v, want %v", runner.operationTimeout, defaultOperationTimeout)
	}
	if runner.logger == nil {
		t.Error("logger is nil")
	}
}

func TestNewRunnerRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name   string
		store  Store
		config Config
	}{
		{name: "missing store"},
		{name: "negative interval", store: &fakeStore{}, config: Config{Interval: -time.Second}},
		{name: "negative page size", store: &fakeStore{}, config: Config{PageSize: -1}},
		{name: "negative operation timeout", store: &fakeStore{}, config: Config{OperationTimeout: -time.Second}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewRunner(test.store, discardLogger(), test.config); err == nil {
				t.Fatal("NewRunner returned no error")
			}
		})
	}
}

func TestSweepKeysetPagesAcrossDeletedAssociations(t *testing.T) {
	jobID := testULID(1)
	rankID := testULID(2)
	associations := []postgres.ChainAssociation{
		{JobID: jobID, RankID: rankID, ChainID: 1, State: postgres.ChainStateDraining},
		{JobID: jobID, RankID: rankID, ChainID: 2, State: postgres.ChainStateDraining},
		{JobID: jobID, RankID: rankID, ChainID: 3, State: postgres.ChainStateDraining},
	}
	remaining := map[int64]bool{1: true, 2: true, 3: true}
	var cursors []*postgres.ChainAssociationCursor

	store := &fakeStore{}
	store.listFn = func(
		_ context.Context,
		cursor *postgres.ChainAssociationCursor,
		limit int32,
	) ([]postgres.ChainAssociation, error) {
		if limit != 2 {
			t.Errorf("limit = %d, want 2", limit)
		}
		if cursor == nil {
			cursors = append(cursors, nil)
		} else {
			cursorCopy := *cursor
			cursors = append(cursors, &cursorCopy)
		}

		page := make([]postgres.ChainAssociation, 0, limit)
		for _, association := range associations {
			if !remaining[association.ChainID] {
				continue
			}
			if cursor != nil && association.ChainID <= cursor.ChainID {
				continue
			}
			page = append(page, association)
			if len(page) == int(limit) {
				break
			}
		}
		return page, nil
	}
	store.reconcileFn = func(
		_ context.Context,
		gotJobID ulid.ULID,
		identity postgres.ChainIdentity,
	) (postgres.ReconcileDrainingResult, error) {
		if gotJobID != jobID {
			t.Errorf("job ID = %s, want %s", gotJobID, jobID)
		}
		if identity.RankID != rankID {
			t.Errorf("rank ID = %s, want %s", identity.RankID, rankID)
		}
		delete(remaining, identity.ChainID)
		return postgres.ReconcileDrainingResult{
			RequeuedChunkIDs:   []int64{identity.ChainID},
			AssociationDeleted: true,
		}, nil
	}

	runner, err := NewRunner(store, discardLogger(), Config{
		Interval:         time.Minute,
		PageSize:         2,
		OperationTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewRunner returned an error: %v", err)
	}

	result, err := runner.Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep returned an error: %v", err)
	}
	wantResult := SweepResult{Examined: 3, Requeued: 3, Deleted: 3}
	if result != wantResult {
		t.Errorf("result = %+v, want %+v", result, wantResult)
	}
	if len(cursors) != 2 {
		t.Fatalf("list calls = %d, want 2", len(cursors))
	}
	if cursors[0] != nil {
		t.Errorf("first cursor = %+v, want nil", cursors[0])
	}
	wantCursor := postgres.ChainAssociationCursor{JobID: jobID, RankID: rankID, ChainID: 2}
	if cursors[1] == nil || *cursors[1] != wantCursor {
		t.Errorf("second cursor = %+v, want %+v", cursors[1], wantCursor)
	}
}

func TestSweepContinuesAfterAssociationError(t *testing.T) {
	jobID := testULID(1)
	rankID := testULID(2)
	associations := []postgres.ChainAssociation{
		{JobID: jobID, RankID: rankID, ChainID: 1},
		{JobID: jobID, RankID: rankID, ChainID: 2},
		{JobID: jobID, RankID: rankID, ChainID: 3},
	}
	operationTimeout := 2 * time.Second
	reconcileErr := errors.New("reconcile failed")
	var calls []int64
	var operationContexts []context.Context
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))

	store := &fakeStore{
		listFn: func(
			_ context.Context,
			_ *postgres.ChainAssociationCursor,
			_ int32,
		) ([]postgres.ChainAssociation, error) {
			return associations, nil
		},
		reconcileFn: func(
			ctx context.Context,
			_ ulid.ULID,
			identity postgres.ChainIdentity,
		) (postgres.ReconcileDrainingResult, error) {
			deadline, ok := ctx.Deadline()
			if !ok {
				t.Error("operation context has no deadline")
			} else if remaining := time.Until(deadline); remaining <= 0 || remaining > operationTimeout {
				t.Errorf("operation deadline remaining = %v, want within %v", remaining, operationTimeout)
			}
			calls = append(calls, identity.ChainID)
			operationContexts = append(operationContexts, ctx)
			switch identity.ChainID {
			case 1:
				return postgres.ReconcileDrainingResult{
					RequeuedChunkIDs:   []int64{10, 11},
					AssociationDeleted: true,
				}, nil
			case 2:
				return postgres.ReconcileDrainingResult{}, reconcileErr
			default:
				return postgres.ReconcileDrainingResult{RequeuedChunkIDs: []int64{12}}, nil
			}
		},
	}
	runner, err := NewRunner(store, logger, Config{
		Interval:         time.Minute,
		PageSize:         10,
		OperationTimeout: operationTimeout,
	})
	if err != nil {
		t.Fatalf("NewRunner returned an error: %v", err)
	}

	result, err := runner.Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep returned an error: %v", err)
	}
	wantResult := SweepResult{Examined: 3, Requeued: 3, Deleted: 1, Errors: 1}
	if result != wantResult {
		t.Errorf("result = %+v, want %+v", result, wantResult)
	}
	wantCalls := []int64{1, 2, 3}
	if len(calls) != len(wantCalls) {
		t.Fatalf("reconcile calls = %v, want %v", calls, wantCalls)
	}
	for index := range wantCalls {
		if calls[index] != wantCalls[index] {
			t.Errorf("reconcile calls = %v, want %v", calls, wantCalls)
			break
		}
	}
	for _, operationCtx := range operationContexts {
		select {
		case <-operationCtx.Done():
		default:
			t.Error("operation context was not canceled")
		}
	}
	if output := logs.String(); !strings.Contains(output, "failed to reconcile draining chain association") ||
		!strings.Contains(output, reconcileErr.Error()) {
		t.Errorf("error log = %q", output)
	}
}

func TestSweepReturnsPageErrorWithPartialResult(t *testing.T) {
	association := postgres.ChainAssociation{
		JobID:   testULID(1),
		RankID:  testULID(2),
		ChainID: 3,
	}
	pageErr := errors.New("list failed")
	listCalls := 0
	store := &fakeStore{
		listFn: func(
			_ context.Context,
			cursor *postgres.ChainAssociationCursor,
			_ int32,
		) ([]postgres.ChainAssociation, error) {
			listCalls++
			if listCalls == 1 {
				return []postgres.ChainAssociation{association}, nil
			}
			wantCursor := postgres.ChainAssociationCursor{
				JobID:   association.JobID,
				RankID:  association.RankID,
				ChainID: association.ChainID,
			}
			if cursor == nil || *cursor != wantCursor {
				t.Errorf("second cursor = %+v, want %+v", cursor, wantCursor)
			}
			return nil, pageErr
		},
		reconcileFn: func(
			_ context.Context,
			_ ulid.ULID,
			_ postgres.ChainIdentity,
		) (postgres.ReconcileDrainingResult, error) {
			return postgres.ReconcileDrainingResult{
				RequeuedChunkIDs:   []int64{1, 2},
				AssociationDeleted: true,
			}, nil
		},
	}
	runner, err := NewRunner(store, discardLogger(), Config{
		Interval:         time.Minute,
		PageSize:         1,
		OperationTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewRunner returned an error: %v", err)
	}

	result, err := runner.Sweep(context.Background())
	if !errors.Is(err, pageErr) {
		t.Fatalf("Sweep error = %v, want %v", err, pageErr)
	}
	wantResult := SweepResult{Examined: 1, Requeued: 2, Deleted: 1}
	if result != wantResult {
		t.Errorf("result = %+v, want %+v", result, wantResult)
	}
}

func TestRunSweepsImmediately(t *testing.T) {
	listCalls := make(chan struct{}, 1)
	store := &fakeStore{
		listFn: func(
			_ context.Context,
			_ *postgres.ChainAssociationCursor,
			_ int32,
		) ([]postgres.ChainAssociation, error) {
			listCalls <- struct{}{}
			return nil, nil
		},
	}
	runner, err := NewRunner(store, discardLogger(), Config{
		Interval:         time.Hour,
		PageSize:         1,
		OperationTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewRunner returned an error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runner.Run(ctx)
	}()

	select {
	case <-listCalls:
	case <-time.After(time.Second):
		t.Fatal("Run did not perform an immediate sweep")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after cancellation")
	}
}

func TestRunSweepsPeriodically(t *testing.T) {
	listCalls := make(chan struct{}, 4)
	store := &fakeStore{
		listFn: func(
			_ context.Context,
			_ *postgres.ChainAssociationCursor,
			_ int32,
		) ([]postgres.ChainAssociation, error) {
			select {
			case listCalls <- struct{}{}:
			default:
			}
			return nil, nil
		},
	}
	runner, err := NewRunner(store, discardLogger(), Config{
		Interval:         10 * time.Millisecond,
		PageSize:         1,
		OperationTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewRunner returned an error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runner.Run(ctx)
	}()

	for call := 0; call < 2; call++ {
		select {
		case <-listCalls:
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for sweep %d", call+1)
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after cancellation")
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testULID(lastByte byte) ulid.ULID {
	var id ulid.ULID
	id[len(id)-1] = lastByte
	return id
}
