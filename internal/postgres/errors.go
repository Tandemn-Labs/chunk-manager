package postgres

import "errors"

var (
	ErrNotFound               = errors.New("not found")
	ErrConflict               = errors.New("conflict")
	ErrInvalidArgument        = errors.New("invalid argument")
	ErrInvalidState           = errors.New("invalid state")
	ErrRegistrationIncomplete = errors.New("job registration incomplete")
	ErrChainNotActive         = errors.New("chain is not active")
	ErrStaleLease             = errors.New("stale lease")
	ErrLeaseExpired           = errors.New("lease expired")
)
