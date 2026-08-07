// Package postgres implements the durable handoff repository with pgx.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	maxIdentityBytes = 256
	maxEventPageSize = 1000
	// eventAppendAdvisoryLock serializes sequence allocation through commit.
	// This deliberately trades event-write throughput for commit-ordered replay.
	eventAppendAdvisoryLock int64 = 0x4750544c495645
)

type Repository struct {
	pool      *pgxpool.Pool
	clock     clock.Clock
	failpoint func(string) error
}

var _ handoff.Repository = (*Repository)(nil)

func NewRepository(pool *pgxpool.Pool, repositoryClock clock.Clock) (*Repository, error) {
	if pool == nil {
		return nil, errors.New("postgres repository pool is required")
	}
	if nilInterface(repositoryClock) {
		return nil, errors.New("postgres repository clock is required")
	}
	return &Repository{pool: pool, clock: repositoryClock}, nil
}

func (r *Repository) after(step string) error {
	if r.failpoint != nil {
		return r.failpoint(step)
	}
	return nil
}

// lockEventAppend must be the first lock acquired by a transaction that may
// append a framework event. PostgreSQL sequences are allocation ordered, not
// commit ordered; holding this transaction-scoped lock through commit makes a
// published seq prefix complete for all current and future event writers.
func (r *Repository) lockEventAppend(ctx context.Context, tx pgx.Tx) error {
	if err := r.after("before_event_lock"); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, eventAppendAdvisoryLock); err != nil {
		return err
	}
	return r.after("event_lock")
}

func validateContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", handoff.ErrInvalidRequest)
	}
	return ctx.Err()
}

func validateScope(scope handoff.Scope) error {
	if err := bounded("tenant id", scope.TenantID, maxIdentityBytes, true); err != nil {
		return err
	}
	if err := bounded("user id", scope.UserID, maxIdentityBytes, true); err != nil {
		return err
	}
	return bounded("conversation id", string(scope.ConversationID), maxIdentityBytes, true)
}

func bounded(name, value string, limit int, required bool) error {
	if (required && strings.TrimSpace(value) == "") || !utf8.ValidString(value) || len(value) > limit {
		return fmt.Errorf("%w: %s is invalid", handoff.ErrInvalidRequest, name)
	}
	return nil
}

func mapWriteError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505":
			return fmt.Errorf("%w: conflicting durable identity", handoff.ErrIdempotencyConflict)
		case "23503":
			return handoff.ErrNotFound
		case "23514", "22001", "22003", "22P02":
			return fmt.Errorf("%w: database rejected bounded value", handoff.ErrInvalidRequest)
		}
	}
	return err
}

func rollback(tx pgx.Tx) {
	_ = tx.Rollback(context.Background())
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
