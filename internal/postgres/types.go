package postgres

import (
	"time"

	"github.com/oklog/ulid/v2"
)

type JobState string

const (
	JobStatePending   JobState = "PENDING"
	JobStateRunning   JobState = "RUNNING"
	JobStateSucceeded JobState = "SUCCEEDED"
	JobStateFailed    JobState = "FAILED"
	JobStateCancelled JobState = "CANCELLED"
)

type ChainState string

const (
	ChainStateActive   ChainState = "ACTIVE"
	ChainStateDraining ChainState = "DRAINING"
)

type ChunkState string

const (
	ChunkStateReady     ChunkState = "READY"
	ChunkStateLeased    ChunkState = "LEASED"
	ChunkStateSucceeded ChunkState = "SUCCEEDED"
	ChunkStateFailed    ChunkState = "FAILED"
	ChunkStateCancelled ChunkState = "CANCELLED"
)

type Job struct {
	ID                      ulid.ULID
	State                   JobState
	TotalChunkCount         int64
	SucceededChunkCount     int64
	FailedChunkCount        int64
	MaxRetries              int32
	RetryBackoff            time.Duration
	LeaseDuration           time.Duration
	CreatedAt               time.Time
	RegistrationCompletedAt *time.Time
	UpdatedAt               time.Time
	TerminalAt              *time.Time
}

type ChainAssociation struct {
	JobID      ulid.ULID
	RankID     ulid.ULID
	ChainID    int64
	State      ChainState
	CreatedAt  time.Time
	DrainingAt *time.Time
}

type ChainIdentity struct {
	RankID  ulid.ULID
	ChainID int64
}

type ChainAssociationCursor struct {
	JobID   ulid.ULID
	RankID  ulid.ULID
	ChainID int64
}

type ChunkRegistration struct {
	ChunkID  int64
	InputRef string
}

type CreateJobParams struct {
	JobID           ulid.ULID
	TotalChunkCount int64
	MaxRetries      int32
	RetryBackoff    time.Duration
	LeaseDuration   time.Duration
}

type ClaimChunksParams struct {
	JobID     ulid.ULID
	RankID    ulid.ULID
	ChainID   int64
	MaxChunks int32
}

type Lease struct {
	JobID      ulid.ULID
	ChunkID    int64
	InputRef   string
	RankID     ulid.ULID
	ChainID    int64
	Generation int64
	ExpiresAt  time.Time
	RetryCount int32
}

type LeaseReference struct {
	ChunkID    int64
	Generation int64
}

type RenewLeasesParams struct {
	JobID   ulid.ULID
	RankID  ulid.ULID
	ChainID int64
	Leases  []LeaseReference
}

type RenewedLease struct {
	ChunkID    int64
	Generation int64
	ExpiresAt  time.Time
}

type RenewLeasesResult struct {
	Renewed []RenewedLease
	Stale   []LeaseReference
}

type CompleteChunkParams struct {
	JobID      ulid.ULID
	RankID     ulid.ULID
	ChainID    int64
	ChunkID    int64
	Generation int64
	OutputURI  string
	Checksum   string
	OutputSize int64
}

type CompleteChunkResult struct {
	JobState JobState
	Replay   bool
}

type FailChunkParams struct {
	JobID        ulid.ULID
	RankID       ulid.ULID
	ChainID      int64
	ChunkID      int64
	Generation   int64
	FailureClass string
	Message      string
	Retriable    bool
}

type FailChunkResult struct {
	JobState  JobState
	Retried   bool
	NotBefore *time.Time
}

type ReconcileDrainingResult struct {
	RequeuedChunkIDs   []int64
	AssociationDeleted bool
}
