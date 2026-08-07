package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ListenerConfig struct {
	ReconnectDelay time.Duration
	MaxJitter      time.Duration
}

type Listener struct {
	pool            *pgxpool.Pool
	clock           clock.Clock
	jitter          clock.JitterSource
	config          ListenerConfig
	onConnected     func(uint32)
	onNotification  func()
	beforeReconnect func()
}

func NewListener(pool *pgxpool.Pool, listenerClock clock.Clock, jitter clock.JitterSource, config ListenerConfig) (*Listener, error) {
	if pool == nil || nilInterface(listenerClock) || nilInterface(jitter) {
		return nil, errors.New("listener dependencies are required")
	}
	if !validDelayBounds(config.ReconnectDelay, config.MaxJitter) {
		return nil, errors.New("listener bounds are invalid")
	}
	return &Listener{pool: pool, clock: listenerClock, jitter: jitter, config: config}, nil
}

func (l *Listener) Run(ctx context.Context, wakeups chan<- handoff.EventWakeup) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if wakeups == nil {
		return errors.New("listener output channel is required")
	}
	for {
		config := l.pool.Config().ConnConfig.Copy()
		conn, err := pgx.ConnectConfig(ctx, config)
		if err == nil {
			_, err = conn.Exec(ctx, `LISTEN `+notificationChannel)
		}
		if err == nil {
			if l.onConnected != nil {
				l.onConnected(conn.PgConn().PID())
			}
			err = l.consume(ctx, conn, wakeups)
		}
		if conn != nil {
			_ = conn.Close(context.Background())
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if l.beforeReconnect != nil {
			l.beforeReconnect()
		}
		if waitErr := waitWithClock(ctx, l.clock, l.config.ReconnectDelay+jitterDuration(l.jitter, l.config.MaxJitter)); waitErr != nil {
			return waitErr
		}
	}
}

func (l *Listener) consume(ctx context.Context, conn *pgx.Conn, wakeups chan<- handoff.EventWakeup) error {
	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		wakeup, ok := parseWakeup([]byte(notification.Payload))
		if ok {
			select {
			case wakeups <- wakeup:
			default:
			}
		}
		if l.onNotification != nil {
			l.onNotification()
		}
	}
}

func parseWakeup(raw []byte) (handoff.EventWakeup, bool) {
	fields, ok := decodeFlatObject(raw)
	if !ok || len(fields) != 3 {
		return handoff.EventWakeup{}, false
	}
	for _, key := range []string{"tenant_id", "conversation_id", "highest_seq"} {
		if _, ok := fields[key]; !ok {
			return handoff.EventWakeup{}, false
		}
	}
	var tenant, conversation string
	var seq int64
	if json.Unmarshal(fields["tenant_id"], &tenant) != nil || bounded("tenant", tenant, maxIdentityBytes, true) != nil ||
		json.Unmarshal(fields["conversation_id"], &conversation) != nil || bounded("conversation", conversation, maxIdentityBytes, true) != nil ||
		json.Unmarshal(fields["highest_seq"], &seq) != nil || seq <= 0 {
		return handoff.EventWakeup{}, false
	}
	return handoff.EventWakeup{TenantID: tenant, ConversationID: identity.ConversationID(conversation), HighestSeq: seq}, true
}
