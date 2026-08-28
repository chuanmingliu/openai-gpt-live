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
	"github.com/chuanmingliu/openai-gpt-live/internal/feature"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	eventPumpPageSize          = 64
	maximumEventSubscriptions  = 10_000
	maximumPresentationPayload = 64 << 10
	maximumPumpIdentityBytes   = 256
)

var (
	ErrNoLiveSession          = errors.New("no live session")
	ErrPresentationSuppressed = errors.New("presentation suppressed by safety policy")
)

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

// ReplayAttempt is an exact, one-shot reservation for publishing a replay
// subscription. Commit activates the replay without waking delivery, so the
// owner can remove any external publication fence before calling Notify.
// Commit and Rollback are infallible, concurrency-safe, and idempotent; the
// first terminal operation wins. Notify is idempotent after Commit.
type ReplayAttempt interface {
	Commit()
	Notify()
	Rollback()
}

// TransactionalReplayRegistrar keeps a replay invisible until the live
// session publication that owns it commits.
type TransactionalReplayRegistrar interface {
	BeginReplay(EventSubscription, int64) (ReplayAttempt, error)
}

type EventPump struct {
	Reader        EventReader
	Cursors       DeliveryCursorStore
	Notifications <-chan EventWakeup
	Sink          PresentationSink
	Clock         clock.Clock
	PollInterval  time.Duration
	Metrics       EventPumpMetrics
	Tracer        trace.Tracer
	Gates         interface{ Enabled(string) bool }

	mu                  sync.Mutex
	subscriptions       map[EventSubscription]struct{}
	replays             map[EventSubscription]int64
	replayAttempts      map[EventSubscription]*eventPumpReplayAttempt
	deliveryGenerations map[EventSubscription]*eventPumpDeliveryGeneration
	deliveryCancels     map[EventSubscription]eventPumpDeliveryCancel
	changed             chan struct{}
	running             bool
}

type eventPumpReplayAttempt struct {
	pump         *EventPump
	subscription EventSubscription
	afterSeq     int64
	notifyOnce   sync.Once
	activated    bool
	changed      chan struct{}
}

type eventPumpDeliveryCancel struct {
	generation *eventPumpDeliveryGeneration
	cancel     context.CancelFunc
}

// Pointer identity is the non-ABA delivery generation. The marker keeps
// distinct live allocations from being permitted to share an address.
type eventPumpDeliveryGeneration struct{ marker byte }

type EventPumpMetrics struct {
	Presentation func(time.Duration, string)
	Duplicate    func(string)
}

func (p *EventPump) RegisterReplay(subscription EventSubscription, afterSeq int64) error {
	attempt, err := p.BeginReplay(subscription, afterSeq)
	if err != nil {
		return err
	}
	attempt.Commit()
	attempt.Notify()
	return nil
}

// BeginReplay reserves capacity and exact ownership of a replay publication.
// The tentative base subscription and replay cursor stay outside the committed
// maps, so Run and polling cannot observe either before Commit.
func (p *EventPump) BeginReplay(subscription EventSubscription, afterSeq int64) (ReplayAttempt, error) {
	if p == nil {
		return nil, errors.New("event pump is nil")
	}
	if afterSeq < 0 {
		return nil, errors.New("event pump replay cursor is negative")
	}
	if err := validateEventSubscription(subscription); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.initializeRegistrationsLocked()
	if p.replayAttempts[subscription] != nil {
		return nil, errors.New("event pump replay attempt is already pending")
	}
	if _, committed := p.subscriptions[subscription]; !committed && p.subscriptionCountLocked() >= maximumEventSubscriptions {
		return nil, errors.New("event pump subscription limit exceeded")
	}
	attempt := &eventPumpReplayAttempt{pump: p, subscription: subscription, afterSeq: afterSeq}
	p.replayAttempts[subscription] = attempt
	return attempt, nil
}

func (a *eventPumpReplayAttempt) Commit() {
	if a == nil || a.pump == nil {
		return
	}
	p := a.pump
	p.mu.Lock()
	if p.replayAttempts[a.subscription] != a {
		p.mu.Unlock()
		return
	}
	delete(p.replayAttempts, a.subscription)
	cancelDelivery := p.takeDeliveryCancelLocked(a.subscription)
	p.subscriptions[a.subscription] = struct{}{}
	p.deliveryGenerations[a.subscription] = &eventPumpDeliveryGeneration{}
	if current, exists := p.replays[a.subscription]; !exists || a.afterSeq < current {
		p.replays[a.subscription] = a.afterSeq
	}
	a.activated = true
	a.changed = p.changed
	p.mu.Unlock()
	if cancelDelivery != nil {
		cancelDelivery()
	}
}

func (a *eventPumpReplayAttempt) Notify() {
	if a == nil || a.pump == nil {
		return
	}
	a.pump.mu.Lock()
	activated, changed := a.activated, a.changed
	a.pump.mu.Unlock()
	if !activated || changed == nil {
		return
	}
	a.notifyOnce.Do(func() {
		select {
		case changed <- struct{}{}:
		default:
		}
	})
}

func (a *eventPumpReplayAttempt) Rollback() {
	if a == nil || a.pump == nil {
		return
	}
	p := a.pump
	p.mu.Lock()
	if p.replayAttempts[a.subscription] == a {
		delete(p.replayAttempts, a.subscription)
	}
	p.mu.Unlock()
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
	p.initializeRegistrationsLocked()
	if _, exists := p.subscriptions[subscription]; exists {
		return nil
	}
	if p.subscriptionCountLocked() >= maximumEventSubscriptions {
		return errors.New("event pump subscription limit exceeded")
	}
	p.subscriptions[subscription] = struct{}{}
	p.deliveryGenerations[subscription] = &eventPumpDeliveryGeneration{}
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
	cancelDelivery := p.takeDeliveryCancelLocked(subscription)
	delete(p.subscriptions, subscription)
	delete(p.replays, subscription)
	delete(p.replayAttempts, subscription)
	delete(p.deliveryGenerations, subscription)
	p.mu.Unlock()
	if cancelDelivery != nil {
		cancelDelivery()
	}
}

func (p *EventPump) initializeRegistrationsLocked() {
	if p.subscriptions == nil {
		p.subscriptions = make(map[EventSubscription]struct{})
	}
	if p.replays == nil {
		p.replays = make(map[EventSubscription]int64)
	}
	if p.replayAttempts == nil {
		p.replayAttempts = make(map[EventSubscription]*eventPumpReplayAttempt)
	}
	if p.deliveryGenerations == nil {
		p.deliveryGenerations = make(map[EventSubscription]*eventPumpDeliveryGeneration)
	}
	if p.deliveryCancels == nil {
		p.deliveryCancels = make(map[EventSubscription]eventPumpDeliveryCancel)
	}
	if p.changed == nil {
		p.changed = make(chan struct{}, 1)
	}
}

func (p *EventPump) takeDeliveryCancelLocked(subscription EventSubscription) context.CancelFunc {
	if active, ok := p.deliveryCancels[subscription]; ok {
		delete(p.deliveryCancels, subscription)
		return active.cancel
	}
	return nil
}

func (p *EventPump) subscriptionCountLocked() int {
	count := len(p.subscriptions)
	for subscription := range p.replayAttempts {
		if _, committed := p.subscriptions[subscription]; !committed {
			count++
		}
	}
	return count
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
	p.initializeRegistrationsLocked()
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
	deliveryCtx, generation, ok := p.beginDeliveryGeneration(ctx, subscription)
	if !ok {
		return nil
	}
	defer p.finishDeliveryGeneration(subscription, generation)
	storedCursor, err := p.Cursors.LoadDeliveryCursor(deliveryCtx, subscription.ConsumerID, subscription.Scope)
	if err != nil {
		if !p.deliveryGenerationCurrent(subscription, generation) {
			return nil
		}
		return err
	}
	if !p.deliveryGenerationCurrent(subscription, generation) {
		return nil
	}
	if storedCursor < 0 {
		return errors.New("event pump cursor is negative")
	}
	cursor := storedCursor
	p.mu.Lock()
	if !p.deliveryGenerationCurrentLocked(subscription, generation) {
		p.mu.Unlock()
		return nil
	}
	if replay, ok := p.replays[subscription]; ok && replay < cursor {
		cursor = replay
	} else if ok {
		delete(p.replays, subscription)
	}
	p.mu.Unlock()
	for {
		events, err := p.Reader.ReadEvents(deliveryCtx, subscription.Scope, cursor, eventPumpPageSize)
		if err != nil {
			if !p.deliveryGenerationCurrent(subscription, generation) {
				return nil
			}
			return err
		}
		if !p.deliveryGenerationCurrent(subscription, generation) {
			return nil
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
			event := events[end-1]
			if event.Seq <= storedCursor && p.Metrics.Duplicate != nil {
				p.Metrics.Duplicate("replay")
			}
			// This exact-generation observation is the Sink effect admission
			// linearization point against replay activation.
			if !p.deliveryGenerationCurrent(subscription, generation) {
				return nil
			}
			offerCtx := deliveryCtx
			var span trace.Span
			if p.Tracer != nil {
				offerCtx, span = p.Tracer.Start(deliveryCtx, "gateway.presentation.offer")
			}
			var offerErr error
			shadow := p.Gates != nil && p.Gates.Enabled(feature.ShadowMode)
			if !shadow {
				offerErr = p.Sink.OfferHandoffEvent(offerCtx, subscription.Scope, CloneEvent(event))
				// Re-check at the completed presentation boundary so a concurrent
				// transition is recorded as shadow even when the session's final
				// visual/speech checks performed the actual suppression.
				shadow = p.Gates != nil && p.Gates.Enabled(feature.ShadowMode)
			}
			if !p.deliveryGenerationCurrent(subscription, generation) {
				if span != nil {
					span.End()
				}
				return nil
			}
			result := "ok"
			if shadow {
				result = "shadow"
			} else if offerErr == ErrPresentationSuppressed {
				result = "suppressed"
			} else if offerErr != nil {
				result = "error"
			}
			if p.Metrics.Presentation != nil {
				delay := p.Clock.Now().Sub(event.CreatedAt)
				if delay < 0 {
					delay = 0
				}
				p.Metrics.Presentation(delay, result)
			}
			if span != nil {
				span.SetAttributes(attribute.String("result", result))
				span.End()
			}
			if offerErr != nil && offerErr != ErrPresentationSuppressed {
				return offerErr
			}
			for acknowledged := index; acknowledged < end; acknowledged++ {
				if events[acknowledged].Seq > storedCursor {
					// This exact-generation observation is the cursor effect
					// admission linearization point against replay activation.
					if !p.deliveryGenerationCurrent(subscription, generation) {
						return nil
					}
					if err := p.Cursors.AdvanceDeliveryCursor(deliveryCtx, subscription.ConsumerID, subscription.Scope, events[acknowledged].Seq); err != nil {
						if !p.deliveryGenerationCurrent(subscription, generation) {
							return nil
						}
						return err
					}
					if !p.deliveryGenerationCurrent(subscription, generation) {
						return nil
					}
				}
				cursor = events[acknowledged].Seq
				p.mu.Lock()
				if !p.deliveryGenerationCurrentLocked(subscription, generation) {
					p.mu.Unlock()
					return nil
				}
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

func (p *EventPump) beginDeliveryGeneration(ctx context.Context, subscription EventSubscription) (context.Context, *eventPumpDeliveryGeneration, bool) {
	p.mu.Lock()
	generation, ok := p.deliveryGenerations[subscription]
	if !ok {
		p.mu.Unlock()
		return nil, nil, false
	}
	previousCancel := p.takeDeliveryCancelLocked(subscription)
	deliveryCtx, cancel := context.WithCancel(ctx)
	p.deliveryCancels[subscription] = eventPumpDeliveryCancel{generation: generation, cancel: cancel}
	p.mu.Unlock()
	if previousCancel != nil {
		previousCancel()
	}
	return deliveryCtx, generation, true
}

func (p *EventPump) finishDeliveryGeneration(subscription EventSubscription, generation *eventPumpDeliveryGeneration) {
	p.mu.Lock()
	active, ok := p.deliveryCancels[subscription]
	if ok && active.generation == generation {
		delete(p.deliveryCancels, subscription)
	}
	p.mu.Unlock()
	if ok && active.generation == generation {
		active.cancel()
	}
}

// deliveryGenerationCurrent serializes exact-generation observation with
// replay activation. Calls immediately before Sink or cursor I/O are the
// admission point for that external effect; completion is revalidated before
// any later effect or replay mutation.
func (p *EventPump) deliveryGenerationCurrent(subscription EventSubscription, generation *eventPumpDeliveryGeneration) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.deliveryGenerationCurrentLocked(subscription, generation)
}

func (p *EventPump) deliveryGenerationCurrentLocked(subscription EventSubscription, generation *eventPumpDeliveryGeneration) bool {
	_, subscribed := p.subscriptions[subscription]
	return subscribed && generation != nil && p.deliveryGenerations[subscription] == generation
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
