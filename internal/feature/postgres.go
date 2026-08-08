package feature

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	featureNotificationChannel = "live_runtime_feature_changed"
	featureTextBytes           = 256
)

type PostgresStore struct {
	pool           *pgxpool.Pool
	clock          clock.Clock
	reconnectDelay time.Duration
	closeTimeout   time.Duration
	wakeups        chan struct{}
	listenerHook   func(uint32)
	commit         func(context.Context, pgx.Tx) error
	mutationHook   func(string)
}

func NewPostgres(pool *pgxpool.Pool, source clock.Clock, reconnectDelay time.Duration, closeTimeout ...time.Duration) (*PostgresStore, error) {
	if pool == nil || source == nil {
		return nil, errors.New("feature postgres dependencies are required")
	}
	if reconnectDelay < time.Millisecond || reconnectDelay > time.Minute {
		return nil, errors.New("feature listener reconnect delay is out of bounds")
	}
	boundedClose := defaultNotificationShutdownTimeout
	if len(closeTimeout) > 1 || (len(closeTimeout) == 1 && (closeTimeout[0] <= 0 || closeTimeout[0] > time.Minute)) {
		return nil, errors.New("feature listener close timeout is out of bounds")
	}
	if len(closeTimeout) == 1 {
		boundedClose = closeTimeout[0]
	}
	return &PostgresStore{pool: pool, clock: source, reconnectDelay: reconnectDelay, closeTimeout: boundedClose, wakeups: make(chan struct{}, 1)}, nil
}

func (s *PostgresStore) Wakeups() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.wakeups
}

func (s *PostgresStore) Load(ctx context.Context) (Snapshot, error) {
	if s == nil || s.pool == nil || ctx == nil {
		return Snapshot{}, errors.New("feature store is not configured")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return Snapshot{}, errors.New("load feature authority")
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	snapshot, err := loadSnapshot(ctx, tx)
	if err != nil {
		return Snapshot{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Snapshot{}, errors.New("load feature authority")
	}
	return snapshot, nil
}

type rowQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func loadSnapshot(ctx context.Context, query rowQuerier) (Snapshot, error) {
	var version int64
	if err := query.QueryRow(ctx, `SELECT version FROM runtime_feature_authority WHERE singleton=true`).Scan(&version); err != nil || version <= 0 {
		return Snapshot{}, errors.New("runtime feature authority is corrupt")
	}
	rows, err := query.Query(ctx, `SELECT name, enabled, version FROM runtime_feature_flags ORDER BY name`)
	if err != nil {
		return Snapshot{}, errors.New("runtime feature authority is corrupt")
	}
	defer rows.Close()
	values := make(map[string]bool, 2)
	for rows.Next() {
		var name string
		var enabled bool
		var rowVersion int64
		if rows.Scan(&name, &enabled, &rowVersion) != nil || !known(name) || rowVersion <= 0 || rowVersion > version {
			return Snapshot{}, errors.New("runtime feature authority is corrupt")
		}
		if _, duplicate := values[name]; duplicate {
			return Snapshot{}, errors.New("runtime feature authority is corrupt")
		}
		values[name] = enabled
	}
	if rows.Err() != nil || !validAuthorityValues(values) {
		return Snapshot{}, errors.New("runtime feature authority is corrupt")
	}
	return Snapshot{Version: uint64(version), Values: cloneValues(values)}, nil
}

func (s *PostgresStore) Set(ctx context.Context, name string, enabled bool, actor, reason string) (Snapshot, error) {
	if s == nil || s.pool == nil || ctx == nil {
		return Snapshot{}, errors.New("feature store is not configured")
	}
	if !known(name) || invalidBoundedText(actor) || invalidBoundedText(reason) {
		return Snapshot{}, errors.New("runtime feature mutation is invalid")
	}
	// The singleton row lock is the writer serialization point. Read committed
	// avoids serialization-abort retries while preserving one total version order.
	if s.mutationHook != nil {
		s.mutationHook("begin")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Snapshot{}, errors.New("mutate runtime feature")
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var previous int64
	if err := tx.QueryRow(ctx, `SELECT version FROM runtime_feature_authority WHERE singleton=true FOR UPDATE`).Scan(&previous); err != nil || previous <= 0 {
		return Snapshot{}, errors.New("runtime feature authority is corrupt")
	}
	next := previous + 1
	command, err := tx.Exec(ctx, `UPDATE runtime_feature_authority SET version=$1, updated_at=CURRENT_TIMESTAMP WHERE singleton=true`, next)
	if err != nil || command.RowsAffected() != 1 {
		return Snapshot{}, errors.New("mutate runtime feature")
	}
	command, err = tx.Exec(ctx, `UPDATE runtime_feature_flags SET enabled=$1, version=$2, updated_at=CURRENT_TIMESTAMP WHERE name=$3`, enabled, next, name)
	if err != nil || command.RowsAffected() != 1 {
		return Snapshot{}, errors.New("runtime feature authority is corrupt")
	}
	if _, err = tx.Exec(ctx, `INSERT INTO runtime_feature_audit(version,name,enabled,actor,reason) VALUES($1,$2,$3,$4,$5)`, next, name, enabled, actor, reason); err != nil {
		return Snapshot{}, errors.New("mutate runtime feature")
	}
	payload, _ := json.Marshal(struct {
		Version int64  `json:"version"`
		Name    string `json:"name"`
	}{next, name})
	if _, err = tx.Exec(ctx, `SELECT pg_notify($1,$2)`, featureNotificationChannel, string(payload)); err != nil {
		return Snapshot{}, errors.New("mutate runtime feature")
	}
	snapshot, err := loadSnapshot(ctx, tx)
	if err != nil {
		return Snapshot{}, err
	}
	commit := tx.Commit
	if s.commit != nil {
		commit = func(commitCtx context.Context) error { return s.commit(commitCtx, tx) }
	}
	if err = commit(ctx); err == nil {
		return snapshot, nil
	}
	// Commit outcomes can be ambiguous after a connection failure. Reconcile
	// the exact content-free audit identity; never retry. A later serialized
	// writer may already have advanced authority, so current version/value are
	// not evidence that this exact commit failed.
	reconcileCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	var audit bool
	checkErr := s.pool.QueryRow(reconcileCtx, `SELECT EXISTS(SELECT 1 FROM runtime_feature_audit WHERE version=$1 AND name=$2 AND enabled=$3 AND actor=$4 AND reason=$5)`, next, name, enabled, actor, reason).Scan(&audit)
	if checkErr != nil || !audit {
		return Snapshot{}, errors.New("runtime feature mutation outcome is unknown")
	}
	return snapshot, nil
}

func invalidBoundedText(value string) bool {
	return strings.TrimSpace(value) == "" || len(value) > featureTextBytes || !utf8.ValidString(value)
}

func (s *PostgresStore) RunNotifications(ctx context.Context) error {
	if s == nil || s.pool == nil || ctx == nil {
		return errors.New("feature listener is not configured")
	}
	for {
		config := s.pool.Config().ConnConfig.Copy()
		conn, err := pgx.ConnectConfig(ctx, config)
		if err == nil {
			_, err = conn.Exec(ctx, `LISTEN `+featureNotificationChannel)
		}
		if err == nil {
			if s.listenerHook != nil {
				s.listenerHook(conn.PgConn().PID())
			}
			err = s.consumeNotifications(ctx, conn)
		}
		if conn != nil {
			closeCtx, cancel := context.WithTimeout(context.Background(), s.closeTimeout)
			_ = conn.Close(closeCtx)
			cancel()
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		ticker := s.clock.NewTicker(s.reconnectDelay)
		select {
		case <-ctx.Done():
			ticker.Stop()
			return ctx.Err()
		case <-ticker.C():
			ticker.Stop()
		}
	}
}

func (s *PostgresStore) consumeNotifications(ctx context.Context, conn *pgx.Conn) error {
	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		var hint struct {
			Version uint64 `json:"version"`
			Name    string `json:"name"`
		}
		if json.Unmarshal([]byte(notification.Payload), &hint) != nil || hint.Version == 0 || !known(hint.Name) {
			continue
		}
		select {
		case s.wakeups <- struct{}{}:
		default:
		}
	}
}

var _ Store = (*PostgresStore)(nil)
