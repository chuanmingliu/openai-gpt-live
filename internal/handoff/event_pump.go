package handoff

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
)

const (
	eventPumpPageSize          = 64
	maximumEventSubscriptions  = 10_000
	maximumPresentationPayload = 64 << 10
	maximumPumpIdentityBytes   = 256
)

var ErrNoLiveSession = errors.New("no live session")

type PresentationSink interface {
	OfferHandoffEvent(context.Context, Scope, Event) error
}

type DeliveryCursorStore interface {
	LoadDeliveryCursor(context.Context, string, Scope) (int64, error)
	AdvanceDeliveryCursor(context.Context, string, Scope, int64) error
}

type EventSubscription struct {
	ConsumerID string
	Scope      Scope
}

// ReplayRegistrar installs a one-shot per-live-session replay overlay. The
// durable global cursor remains monotonic; original events are offered again
// from afterSeq until the overlay catches the stored cursor.
type ReplayRegistrar interface {
	RegisterReplay(EventSubscription, int64) error
}

type EventPump struct {
	Reader        EventReader
	Cursors       DeliveryCursorStore
	Notifications <-chan EventWakeup
	Sink          PresentationSink
	Clock         clock.Clock
	PollInterval  time.Duration

	mu            sync.Mutex
	subscriptions map[EventSubscription]struct{}
	replays       map[EventSubscription]int64
	changed       chan struct{}
	running       bool
}

func (p *EventPump) RegisterReplay(subscription EventSubscription, afterSeq int64) error {
	if afterSeq < 0 {
		return errors.New("event pump replay cursor is negative")
	}
	if err := p.Register(subscription); err != nil {
		return err
	}
	p.mu.Lock()
	if p.replays == nil {
		p.replays = make(map[EventSubscription]int64)
	}
	if current, exists := p.replays[subscription]; !exists || afterSeq < current {
		p.replays[subscription] = afterSeq
	}
	changed := p.changed
	p.mu.Unlock()
	select {
	case changed <- struct{}{}:
	default:
	}
	return nil
}

func (p *EventPump) Register(subscription EventSubscription) error {
	if p == nil {
		return errors.New("event pump is nil")
	}
	if err := validateEventSubscription(subscription); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.subscriptions == nil {
		p.subscriptions = make(map[EventSubscription]struct{})
	}
	if p.changed == nil {
		p.changed = make(chan struct{}, 1)
	}
	if _, exists := p.subscriptions[subscription]; exists {
		return nil
	}
	if len(p.subscriptions) >= maximumEventSubscriptions {
		return errors.New("event pump subscription limit exceeded")
	}
	p.subscriptions[subscription] = struct{}{}
	select {
	case p.changed <- struct{}{}:
	default:
	}
	return nil
}

func (p *EventPump) Unregister(subscription EventSubscription) {
	if p == nil {
		return
	}
	p.mu.Lock()
	delete(p.subscriptions, subscription)
	delete(p.replays, subscription)
	p.mu.Unlock()
}

func (p *EventPump) Run(ctx context.Context) error {
	if p == nil {
		return errors.New("event pump is nil")
	}
	if ctx == nil {
		return errors.New("event pump context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if eventPumpNil(p.Reader) || eventPumpNil(p.Cursors) || eventPumpNil(p.Sink) || eventPumpNil(p.Clock) {
		return errors.New("event pump dependencies are required")
	}
	if p.PollInterval < time.Millisecond || p.PollInterval > 24*time.Hour {
		return errors.New("event pump poll interval is out of bounds")
	}
	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return errors.New("event pump is already running")
	}
	p.running = true
	if p.subscriptions == nil {
		p.subscriptions = make(map[EventSubscription]struct{})
	}
	if p.changed == nil {
		p.changed = make(chan struct{}, 1)
	}
	changed := p.changed
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		p.running = false
		p.mu.Unlock()
	}()

	ticker := p.Clock.NewTicker(p.PollInterval)
	defer ticker.Stop()
	// Registrations made before Run are covered by the initial complete poll;
	// discard their coalesced hint so they cannot trigger a redundant replay.
	select {
	case <-changed:
	default:
	}
	p.deliver(ctx, nil)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C():
			p.deliver(ctx, nil)
		case <-changed:
			p.deliver(ctx, nil)
		case notification, ok := <-p.Notifications:
			if !ok {
				p.Notifications = nil
				continue
			}
			p.deliver(ctx, &notification)
		}
	}
}

func (p *EventPump) deliver(ctx context.Context, notification *EventWakeup) {
	for _, subscription := range p.subscriptionSnapshot(notification) {
		if ctx.Err() != nil {
			return
		}
		_ = p.deliverSubscription(ctx, subscription)
	}
}

func (p *EventPump) subscriptionSnapshot(notification *EventWakeup) []EventSubscription {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]EventSubscription, 0, len(p.subscriptions))
	for subscription := range p.subscriptions {
		if notification != nil && (subscription.Scope.TenantID != notification.TenantID ||
			subscription.Scope.ConversationID != notification.ConversationID) {
			continue
		}
		out = append(out, subscription)
	}
	return out
}

func (p *EventPump) deliverSubscription(ctx context.Context, subscription EventSubscription) error {
	storedCursor, err := p.Cursors.LoadDeliveryCursor(ctx, subscription.ConsumerID, subscription.Scope)
	if err != nil {
		return err
	}
	if storedCursor < 0 {
		return errors.New("event pump cursor is negative")
	}
	cursor := storedCursor
	p.mu.Lock()
	if replay, ok := p.replays[subscription]; ok && replay < cursor {
		cursor = replay
	} else if ok {
		delete(p.replays, subscription)
	}
	p.mu.Unlock()
	for {
		events, err := p.Reader.ReadEvents(ctx, subscription.Scope, cursor, eventPumpPageSize)
		if err != nil {
			return err
		}
		if len(events) > eventPumpPageSize {
			return errors.New("event reader exceeded requested page size")
		}
		if err := validateEventPage(subscription.Scope, cursor, events); err != nil {
			return err
		}
		for index := 0; index < len(events); {
			end := index + 1
			if ordinaryProgress(events[index]) {
				for end < len(events) && ordinaryProgress(events[end]) && events[end].HandoffID == events[index].HandoffID {
					end++
				}
			}
			if err := p.Sink.OfferHandoffEvent(ctx, subscription.Scope, CloneEvent(events[end-1])); err != nil {
				return err
			}
			for acknowledged := index; acknowledged < end; acknowledged++ {
				if events[acknowledged].Seq > storedCursor {
					if err := p.Cursors.AdvanceDeliveryCursor(ctx, subscription.ConsumerID, subscription.Scope, events[acknowledged].Seq); err != nil {
						return err
					}
				}
				cursor = events[acknowledged].Seq
				p.mu.Lock()
				if replay, ok := p.replays[subscription]; ok && replay <= cursor {
					if cursor >= storedCursor {
						delete(p.replays, subscription)
					} else {
						p.replays[subscription] = cursor
					}
				}
				p.mu.Unlock()
			}
			index = end
		}
		if len(events) < eventPumpPageSize {
			return nil
		}
	}
}

func validateEventSubscription(subscription EventSubscription) error {
	for name, value := range map[string]string{
		"consumer ID":     subscription.ConsumerID,
		"tenant ID":       subscription.Scope.TenantID,
		"user ID":         subscription.Scope.UserID,
		"conversation ID": string(subscription.Scope.ConversationID),
	} {
		if strings.TrimSpace(value) == "" || len(value) > maximumPumpIdentityBytes || !utf8.ValidString(value) {
			return fmt.Errorf("event pump %s is invalid", name)
		}
	}
	return nil
}

func validateEventPage(scope Scope, cursor int64, events []Event) error {
	previous := cursor
	for _, event := range events {
		trimmed := bytes.TrimSpace(event.Payload)
		if event.Seq <= previous || strings.TrimSpace(event.EventID) == "" || len(event.EventID) > maximumPumpIdentityBytes ||
			strings.TrimSpace(string(event.HandoffID)) == "" || len(event.HandoffID) > maximumPumpIdentityBytes ||
			event.TenantID != scope.TenantID || event.ConversationID != scope.ConversationID ||
			strings.TrimSpace(event.Kind) == "" || len(event.Kind) > maximumPumpIdentityBytes || strings.TrimSpace(string(event.State)) == "" ||
			len(event.Payload) == 0 || len(event.Payload) > maximumPresentationPayload || !json.Valid(event.Payload) ||
			len(trimmed) == 0 || trimmed[0] != '{' || !eventPayloadWithinDepth(trimmed, 32) {
			return errors.New("event reader returned an invalid event")
		}
		previous = event.Seq
	}
	return nil
}

func eventPayloadWithinDepth(raw []byte, maximum int) bool {
	depth := 0
	inString := false
	escaped := false
	for _, char := range raw {
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if char == '\\' {
				escaped = true
			} else if char == '"' {
				inString = false
			}
			continue
		}
		if char == '"' {
			inString = true
			continue
		}
		switch char {
		case '{', '[':
			depth++
			if depth > maximum {
				return false
			}
		case '}', ']':
			depth--
			if depth < 0 {
				return false
			}
		}
	}
	return depth == 0 && !inString
}

func ordinaryProgress(event Event) bool {
	if event.State != StateRunning && event.State != StateLeased {
		return false
	}
	kind := strings.ToLower(event.Kind)
	return strings.Contains(kind, "progress") || strings.Contains(kind, "lease") || strings.Contains(kind, "running")
}

func eventPumpNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	return reflected.Kind() == reflect.Pointer && reflected.IsNil()
}
