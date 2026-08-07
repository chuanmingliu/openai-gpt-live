package handoff

import "errors"

var (
	ErrInvalidRequest      = errors.New("invalid handoff request")
	ErrNotFound            = errors.New("handoff not found")
	ErrUncommittedTurn     = errors.New("source turn is not committed")
	ErrIdempotencyConflict = errors.New("handoff idempotency conflict")
	ErrInvalidTransition   = errors.New("invalid handoff state transition")
	ErrTerminalState       = errors.New("handoff is terminal")
	ErrRevisionConflict    = errors.New("handoff revision conflict")
	ErrSteeringBlocked     = errors.New("handoff steering is blocked")
)
