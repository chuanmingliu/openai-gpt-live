package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
)

func (r *Repository) ReadEvents(ctx context.Context, scope handoff.Scope, afterSeq int64, limit int) ([]handoff.Event, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	if err := validateScope(scope); err != nil {
		return nil, err
	}
	if afterSeq < 0 || limit <= 0 || limit > maxEventPageSize {
		return nil, fmt.Errorf("%w: invalid event page bounds", handoff.ErrInvalidRequest)
	}
	rows, err := r.pool.Query(ctx, `
		SELECT e.seq,e.event_id,e.handoff_id,e.tenant_id,e.conversation_id,e.kind,e.state,e.attempt,e.payload::text,e.created_at
		FROM handoff_events e
		JOIN handoffs h ON h.tenant_id=e.tenant_id AND h.conversation_id=e.conversation_id AND h.id=e.handoff_id
		WHERE e.tenant_id=$1 AND e.conversation_id=$2 AND h.user_id=$3 AND e.seq>$4
		ORDER BY e.seq ASC LIMIT $5`, scope.TenantID, scope.ConversationID, scope.UserID, afterSeq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]handoff.Event, 0)
	for rows.Next() {
		var event handoff.Event
		var state string
		var attempt int64
		var payload []byte
		if err := rows.Scan(&event.Seq, &event.EventID, &event.HandoffID, &event.TenantID, &event.ConversationID,
			&event.Kind, &state, &attempt, &payload, &event.CreatedAt); err != nil {
			return nil, err
		}
		if event.Seq < 0 || attempt < 0 || attempt > math.MaxUint32 {
			return nil, errors.New("postgres: event numeric value out of range")
		}
		if !json.Valid(payload) {
			return nil, errors.New("postgres: invalid event payload")
		}
		event.State, event.Attempt = handoff.State(state), uint32(attempt)
		event.Payload = append(json.RawMessage(nil), payload...)
		event.CreatedAt = event.CreatedAt.UTC()
		out = append(out, event)
	}
	return out, rows.Err()
}

var _ handoff.EventReader = (*Repository)(nil)
