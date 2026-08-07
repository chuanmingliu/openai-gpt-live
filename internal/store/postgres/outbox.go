package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const notificationChannel = "gpt_live_handoff_events"

type OutboxPublisherConfig struct {
	BatchSize    int
	PollInterval time.Duration
	MaxJitter    time.Duration
}

type OutboxPublisher struct {
	pool        *pgxpool.Pool
	clock       clock.Clock
	jitter      clock.JitterSource
	config      OutboxPublisherConfig
	hook        func(context.Context, string) error
	lagObserver func(time.Duration)
	tracer      trace.Tracer
}

func (p *OutboxPublisher) SetTracer(tracer trace.Tracer) {
	if p != nil {
		p.tracer = tracer
	}
}

// SetLagObserver installs a bounded aggregate metric hook. It receives only a
// duration and never durable payloads or identifiers.
func (p *OutboxPublisher) SetLagObserver(observer func(time.Duration)) {
	if p != nil {
		p.lagObserver = observer
	}
}

func NewOutboxPublisher(pool *pgxpool.Pool, publisherClock clock.Clock, jitter clock.JitterSource, config OutboxPublisherConfig) (*OutboxPublisher, error) {
	if pool == nil || nilInterface(publisherClock) || nilInterface(jitter) {
		return nil, errors.New("outbox publisher dependencies are required")
	}
	if config.BatchSize <= 0 || config.BatchSize > 1000 || !validDelayBounds(config.PollInterval, config.MaxJitter) {
		return nil, errors.New("outbox publisher bounds are invalid")
	}
	return &OutboxPublisher{pool: pool, clock: publisherClock, jitter: jitter, config: config}, nil
}

func (p *OutboxPublisher) Run(ctx context.Context) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	for {
		published, err := p.publishBatch(ctx)
		if err == nil && published == p.config.BatchSize {
			continue
		}
		if err := waitWithClock(ctx, p.clock, p.config.PollInterval+jitterDuration(p.jitter, p.config.MaxJitter)); err != nil {
			return err
		}
	}
}

type outboxRow struct {
	ID        int64
	Topic     string
	Payload   []byte
	CreatedAt time.Time
}

func (p *OutboxPublisher) publishBatch(ctx context.Context) (published int, runErr error) {
	var span trace.Span
	if p != nil && p.tracer != nil {
		ctx, span = p.tracer.Start(ctx, "store.outbox.publish_batch")
		defer func() {
			result := "ok"
			if runErr != nil {
				result = "error"
			}
			span.SetAttributes(attribute.String("result", result))
			span.End()
		}()
	}
	if err := validateContext(ctx); err != nil {
		return 0, err
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer rollback(tx)
	rows, err := tx.Query(ctx, `SELECT id,topic,payload::text,created_at FROM outbox
		WHERE published_at IS NULL ORDER BY attempts,created_at,id FOR UPDATE SKIP LOCKED LIMIT $1`, p.config.BatchSize)
	if err != nil {
		return 0, err
	}
	claimed := make([]outboxRow, 0, p.config.BatchSize)
	for rows.Next() {
		var row outboxRow
		if err := rows.Scan(&row.ID, &row.Topic, &row.Payload, &row.CreatedAt); err != nil {
			rows.Close()
			return 0, err
		}
		row.Payload = append([]byte(nil), row.Payload...)
		claimed = append(claimed, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(claimed) > 0 && p.hook != nil {
		if err := p.hook(ctx, "claimed"); err != nil {
			return 0, err
		}
	}
	now := p.clock.Now().UTC()
	published = 0
	for _, row := range claimed {
		if p.lagObserver != nil {
			lag := now.Sub(row.CreatedAt)
			if lag < 0 {
				lag = 0
			}
			p.lagObserver(lag)
		}
		if !validOutboxPayload(row.Topic, row.Payload) {
			_, err := tx.Exec(ctx, `UPDATE outbox
				SET attempts=CASE WHEN attempts < 2147483647 THEN attempts+1 ELSE attempts END,
				    last_error='invalid outbox payload' WHERE id=$1`, row.ID)
			if err != nil {
				return 0, err
			}
			continue
		}
		if _, err := tx.Exec(ctx, `SELECT pg_notify($1,$2)`, notificationChannel, string(row.Payload)); err != nil {
			return 0, err
		}
		if p.hook != nil {
			if err := p.hook(ctx, "notified"); err != nil {
				return 0, err
			}
		}
		tag, err := tx.Exec(ctx, `UPDATE outbox SET published_at=$2,
			attempts=CASE WHEN attempts < 2147483647 THEN attempts+1 ELSE attempts END,last_error=NULL
			WHERE id=$1 AND published_at IS NULL`, row.ID, now)
		if err != nil {
			return 0, err
		}
		if tag.RowsAffected() != 1 {
			return 0, errors.New("outbox claim was lost")
		}
		published++
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return published, nil
}

func validOutboxPayload(topic string, raw []byte) bool {
	fields, ok := decodeFlatObject(raw)
	if !ok {
		return false
	}
	var required []string
	switch topic {
	case "handoff.dispatch":
		required = []string{"tenant_id", "conversation_id", "handoff_id", "event_seq"}
	case "handoff.event":
		required = []string{"tenant_id", "conversation_id", "highest_seq"}
	default:
		return false
	}
	if len(fields) != len(required) {
		return false
	}
	for _, key := range required {
		if _, ok := fields[key]; !ok {
			return false
		}
	}
	for _, key := range []string{"tenant_id", "conversation_id"} {
		var value string
		if json.Unmarshal(fields[key], &value) != nil || bounded(key, value, maxIdentityBytes, true) != nil {
			return false
		}
	}
	if topic == "handoff.dispatch" {
		var handoffID string
		var seq int64
		return json.Unmarshal(fields["handoff_id"], &handoffID) == nil && bounded("handoff id", handoffID, maxIdentityBytes, true) == nil &&
			json.Unmarshal(fields["event_seq"], &seq) == nil && seq > 0
	}
	var highest int64
	return json.Unmarshal(fields["highest_seq"], &highest) == nil && highest > 0
}

func decodeFlatObject(raw []byte) (map[string]json.RawMessage, bool) {
	fields := make(map[string]json.RawMessage)
	decoder := json.NewDecoder(bytes.NewReader(raw))
	first, err := decoder.Token()
	if err != nil || first != json.Delim('{') {
		return nil, false
	}
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, false
		}
		key, ok := keyToken.(string)
		if !ok {
			return nil, false
		}
		if _, duplicate := fields[key]; duplicate {
			return nil, false
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, false
		}
		fields[key] = append(json.RawMessage(nil), value...)
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') {
		return nil, false
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, false
	}
	return fields, true
}

func validDelayBounds(base, maxJitter time.Duration) bool {
	const maxDuration = time.Duration(1<<63 - 1)
	return base > 0 && maxJitter >= 0 && maxJitter <= maxDuration-base
}

func waitWithClock(ctx context.Context, c clock.Clock, delay time.Duration) error {
	if delay <= 0 {
		return fmt.Errorf("invalid retry delay")
	}
	ticker := c.NewTicker(delay)
	defer ticker.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ticker.C():
		return nil
	}
}

func jitterDuration(source clock.JitterSource, max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	value := source.Duration(max)
	if value < 0 {
		return 0
	}
	if value > max {
		return max
	}
	return value
}
