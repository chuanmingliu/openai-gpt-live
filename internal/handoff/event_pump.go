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

// SubscriptionLease is exact ownership of one committed subscription
// generation. Release removes state only while this lease is still current.
type SubscriptionLease interface {
	Release()
}

// SubscriptionAttempt is a hidden, exact reservation for publishing a base
// subscription. Commit returns the lease that owns the activated generation.
// A rolled-back or otherwise lost reservation returns an error from Commit.
type SubscriptionAttempt interface {
	Commit() (SubscriptionLease, error)
	Rollback()
}

// ReplayAttempt is an exact, one-shot reservation for publishing a replay
// subscription. Commit activates the replay without waking delivery, so the
// owner can remove any external publication fence before calling Notify.
// Commit and Rollback are concurrency-safe and idempotent; the first terminal
// operation wins. Commit reports a lost reservation instead of allowing its
// caller to publish without an active subscription. Notify is idempotent after
// a successful Commit.
type ReplayAttempt interface {
	Commit() (SubscriptionLease, error)
	Notify()
	Rollback()
}

// TransactionalReplayRegistrar keeps a replay invisible until the live
// session publication that owns it commits.
type TransactionalReplayRegistrar interface {
	BeginReplay(EventSubscription, int64) (ReplayAttempt, error)
}

// TransactionalSubscriptionRegistrar owns both initial and replay
// registration as hidden attempts with exact teardown leases.
type TransactionalSubscriptionRegistrar interface {
	TransactionalReplayRegistrar
	BeginSubscription(EventSubscription) (SubscriptionAttempt, error)
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

	mu                   sync.Mutex
	subscriptions        map[EventSubscription]struct{}
	replays              map[EventSubscription]int64
	subscriptionAttempts map[EventSubscription]*eventPumpSubscriptionAttempt
	replayAttempts       map[EventSubscription]*eventPumpReplayAttempt
	subscriptionLeases   map[EventSubscription]*eventPumpSubscriptionLease
	deliveryGenerations  map[EventSubscription]*eventPumpDeliveryGeneration
	deliveryCancels      map[EventSubscription]eventPumpDeliveryCancel
	changed              chan struct{}
	running              bool
}

type eventPumpSubscriptionAttempt struct {
	pump         *EventPump
	subscription EventSubscription
	lease        *eventPumpSubscriptionLease
	rolledBack   bool
}

type eventPumpReplayAttempt struct {
	pump         *EventPump
	subscription EventSubscription
	afterSeq     int64
	legacy       bool
	notifyOnce   sync.Once
	lease        *eventPumpSubscriptionLease
	rolledBack   bool
	changed      chan struct{}
}

type eventPumpSubscriptionLease struct {
	pump         *EventPump
	subscription EventSubscription
	legacy       bool
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
	attempt, err := p.beginReplay(subscription, afterSeq, true)
	if err != nil {
		return err
	}
	if _, err := attempt.Commit(); err != nil {
		return err
	}
	attempt.Notify()
	return nil
}

// BeginSubscription reserves capacity and exact ownership while leaving the
// currently committed base subscription and replay overlay untouched.
func (p *EventPump) BeginSubscription(subscription EventSubscription) (SubscriptionAttempt, error) {
	if p == nil {
		return nil, errors.New("event pump is nil")
	}
	if err := validateEventSubscription(subscription); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.initializeRegistrationsLocked()
	if p.subscriptionAttempts[subscription] != nil || p.replayAttempts[subscription] != nil {
		return nil, errors.New("event pump subscription attempt is already pending")
	}
	if _, committed := p.subscriptions[subscription]; !committed && p.subscriptionCountLocked() >= maximumEventSubscriptions {
		return nil, errors.New("event pump subscription limit exceeded")
	}
	attempt := &eventPumpSubscriptionAttempt{pump: p, subscription: subscription}
	p.subscriptionAttempts[subscription] = attempt
	return attempt, nil
}

// BeginReplay reserves capacity and exact ownership of a replay publication.
// The tentative base subscription and replay cursor stay outside the committed
// maps, so Run and polling cannot observe either before Commit.
func (p *EventPump) BeginReplay(subscription EventSubscription, afterSeq int64) (ReplayAttempt, error) {
	return p.beginReplay(subscription, afterSeq, false)
}

func (p *EventPump) beginReplay(subscription EventSubscription, afterSeq int64, legacy bool) (ReplayAttempt, error) {
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
	if p.subscriptionAttempts[subscription] != nil || p.replayAttempts[subscription] != nil {
		return nil, errors.New("event pump replay attempt is already pending")
	}
	if current := p.subscriptionLeases[subscription]; legacy && current != nil && !current.legacy {
		return nil, errors.New("event pump exact subscription is already active")
	}
	if _, committed := p.subscriptions[subscription]; !committed && p.subscriptionCountLocked() >= maximumEventSubscriptions {
		return nil, errors.New("event pump subscription limit exceeded")
	}
	attempt := &eventPumpReplayAttempt{pump: p, subscription: subscription, afterSeq: afterSeq, legacy: legacy}
	p.replayAttempts[subscription] = attempt
	return attempt, nil
}

func (a *eventPumpSubscriptionAttempt) Commit() (SubscriptionLease, error) {
	if a == nil || a.pump == nil {
		return nil, errors.New("event pump subscription attempt is nil")
	}
	p := a.pump
	p.mu.Lock()
	if a.lease != nil {
		lease := a.lease
		p.mu.Unlock()
		return lease, nil
	}
	if a.rolledBack || p.subscriptionAttempts[a.subscription] != a {
		p.mu.Unlock()
		return nil, errors.New("event pump subscription attempt is no longer pending")
	}
	delete(p.subscriptionAttempts, a.subscription)
	lease, cancelDelivery := p.activateSubscriptionLocked(a.subscription, false, nil)
	a.lease = lease
	changed := p.changed
	p.mu.Unlock()
	if cancelDelivery != nil {
		cancelDelivery()
	}
	select {
	case changed <- struct{}{}:
	default:
	}
	return lease, nil
}

func (a *eventPumpSubscriptionAttempt) Rollback() {
	if a == nil || a.pump == nil {
		return
	}
	p := a.pump
	p.mu.Lock()
	if p.subscriptionAttempts[a.subscription] == a {
		delete(p.subscriptionAttempts, a.subscription)
		a.rolledBack = true
	}
	p.mu.Unlock()
}

func (a *eventPumpReplayAttempt) Commit() (SubscriptionLease, error) {
	if a == nil || a.pump == nil {
		return nil, errors.New("event pump replay attempt is nil")
	}
	p := a.pump
	p.mu.Lock()
	if a.lease != nil {
		lease := a.lease
		p.mu.Unlock()
		return lease, nil
	}
	if a.rolledBack || p.replayAttempts[a.subscription] != a {
		p.mu.Unlock()
		return nil, errors.New("event pump replay attempt is no longer pending")
	}
	delete(p.replayAttempts, a.subscription)
	lease, cancelDelivery := p.activateSubscriptionLocked(a.subscription, a.legacy, &a.afterSeq)
	a.lease = lease
	a.changed = p.changed
	p.mu.Unlock()
	if cancelDelivery != nil {
		cancelDelivery()
	}
	return lease, nil
}

func (a *eventPumpReplayAttempt) Notify() {
	if a == nil || a.pump == nil {
		return
	}
	a.pump.mu.Lock()
	activated, changed := a.lease != nil, a.changed
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
		a.rolledBack = true
	}
	p.mu.Unlock()
}

func (l *eventPumpSubscriptionLease) Release() {
	if l == nil || l.pump == nil {
		return
	}
	p := l.pump
	p.mu.Lock()
	if p.subscriptionLeases[l.subscription] != l {
		p.mu.Unlock()
		return
	}
	cancelDelivery := p.takeDeliveryCancelLocked(l.subscription)
	delete(p.subscriptionLeases, l.subscription)
	delete(p.subscriptions, l.subscription)
	delete(p.replays, l.subscription)
	delete(p.deliveryGenerations, l.subscription)
	p.mu.Unlock()
	if cancelDelivery != nil {
		cancelDelivery()
	}
}

func (p *EventPump) activateSubscriptionLocked(
	subscription EventSubscription,
	legacy bool,
	replayAfter *int64,
) (*eventPumpSubscriptionLease, context.CancelFunc) {
	cancelDelivery := p.takeDeliveryCancelLocked(subscription)
	lease := &eventPumpSubscriptionLease{pump: p, subscription: subscription, legacy: legacy}
	p.subscriptions[subscription] = struct{}{}
	p.subscriptionLeases[subscription] = lease
	p.deliveryGenerations[subscription] = &eventPumpDeliveryGeneration{}
	if replayAfter == nil {
		delete(p.replays, subscription)
	} else if current, exists := p.replays[subscription]; !exists || *replayAfter < current {
		p.replays[subscription] = *replayAfter
	}
	return lease, cancelDelivery
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
	if p.subscriptionAttempts[subscription] != nil || p.replayAttempts[subscription] != nil {
		return errors.New("event pump subscription attempt is already pending")
	}
	if _, exists := p.subscriptions[subscription]; exists {
		return nil
	}
	if p.subscriptionCountLocked() >= maximumEventSubscriptions {
		return errors.New("event pump subscription limit exceeded")
	}
	lease := &eventPumpSubscriptionLease{pump: p, subscription: subscription, legacy: true}
	p.subscriptions[subscription] = struct{}{}
	p.subscriptionLeases[subscription] = lease
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
	lease := p.subscriptionLeases[subscription]
	if lease == nil || !lease.legacy {
		p.mu.Unlock()
		return
	}
	cancelDelivery := p.takeDeliveryCancelLocked(subscription)
	delete(p.subscriptionLeases, subscription)
	delete(p.subscriptions, subscription)
	delete(p.replays, subscription)
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
	if p.subscriptionAttempts == nil {
		p.subscriptionAttempts = make(map[EventSubscription]*eventPumpSubscriptionAttempt)
	}
	if p.replayAttempts == nil {
		p.replayAttempts = make(map[EventSubscription]*eventPumpReplayAttempt)
	}
	if p.subscriptionLeases == nil {
		p.subscriptionLeases = make(map[EventSubscription]*eventPumpSubscriptionLease)
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
	for subscription := range p.subscriptionAttempts {
		if _, committed := p.subscriptions[subscription]; !committed {
			count++
		}
	}
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
