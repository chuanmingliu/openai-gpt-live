package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/feature"
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/reasoner"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/chuanmingliu/openai-gpt-live/internal/store/postgres"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type HandoffWork struct {
	SessionID    identity.SessionID
	Incarnation  identity.Incarnation
	OriginEpoch  identity.ResponseEpoch
	Scope        handoff.Scope
	CallID       string
	ResponseID   string
	ToolName     string
	Arguments    json.RawMessage
	Turn         identity.TurnKey
	Transcript   string
	Snapshot     SessionHandoffSnapshot
	RejectReason string
	queuedAt     time.Time
}

type SessionHandoffSnapshot struct {
	ContextRef       string
	ContextHash      string
	Deadline         time.Time
	TokenBudget      int
	Priority         int16
	ToolAllowlist    []string
	ApprovalPolicy   json.RawMessage
	Versions         handoff.Versions
	PresentationHint string
}

type HandoffCoordinator struct {
	service         handoff.Service
	submit          SessionSubmitter
	work            chan HandoffWork
	slots           chan struct{}
	mu              sync.Mutex
	retainedBytes   int
	journal         CoordinatorJournal
	journalAffinity string
	journalOwner    string
	claimLease      time.Duration
	outageGrace     time.Duration
	outageClock     clock.Clock
	unavailable     atomic.Bool
	health          *durabilityHealth
	outageStarted   time.Time
	tracer          trace.Tracer
	metricsClock    clock.Clock
	ackMetric       func(time.Duration, string)
	gates           interface{ Enabled(string) bool }
	context         session.RecoveryStore
}

func (c *HandoffCoordinator) configureOutage(grace time.Duration, source clock.Clock) error {
	if c == nil || grace <= 0 || grace > 10*time.Minute || isNil(source) {
		return errors.New("invalid durability outage policy")
	}
	c.outageGrace, c.outageClock = grace, source
	if c.health == nil {
		c.health = &durabilityHealth{}
	}
	return nil
}

func (c *HandoffCoordinator) beginOutage() {
	if c.unavailable.CompareAndSwap(false, true) {
		c.outageStarted = c.outageClock.Now()
	}
	c.health.markUnavailable(handoffLane)
}

func (c *HandoffCoordinator) endOutage() {
	if c.unavailable.CompareAndSwap(true, false) {
		c.health.markHealthy(handoffLane)
	}
	c.outageStarted = time.Time{}
}

func (c *HandoffCoordinator) retryDurability(ctx context.Context, operation func() error) error {
	for {
		err := operation()
		if err == nil {
			c.endOutage()
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if c.outageGrace <= 0 || !transientDurabilityError(err) {
			return err
		}
		c.beginOutage()
		if waitErr := c.waitOutage(ctx); waitErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return errors.Join(ErrDurabilityGraceExpired, err)
		}
	}
}

func (c *HandoffCoordinator) waitOutage(ctx context.Context) error {
	remaining := c.outageGrace - c.outageClock.Now().Sub(c.outageStarted)
	if remaining <= 0 {
		return ErrDurabilityGraceExpired
	}
	delay := time.Second
	if remaining < delay {
		delay = remaining
	}
	ticker := c.outageClock.NewTicker(delay)
	defer ticker.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ticker.C():
		return nil
	}
}

func (c *HandoffCoordinator) configureJournal(journal CoordinatorJournal, affinity, owner string, lease time.Duration) error {
	if c == nil || isNil(journal) || strings.TrimSpace(affinity) == "" || len(affinity) > session.DurableIdentifierBytes ||
		strings.TrimSpace(owner) == "" || len(owner) > session.DurableIdentifierBytes ||
		lease < postgres.CoordinatorMinLease || lease > postgres.CoordinatorMaxLease {
		return errors.New("invalid handoff coordinator journal")
	}
	c.journal, c.journalAffinity, c.journalOwner, c.claimLease = journal, affinity, owner, lease
	return nil
}

const CoordinatorAggregateBytes = 1 << 20

var ErrCoordinatorWorkTooLarge = errors.New("durable coordinator work exceeds byte bounds")

func NewHandoffCoordinator(service handoff.Service, submit SessionSubmitter, capacity int) *HandoffCoordinator {
	if isNil(service) || isNil(submit) || capacity <= 0 || capacity > 10_000 {
		return nil
	}
	return &HandoffCoordinator{
		service: service, submit: submit, work: make(chan HandoffWork, capacity), slots: make(chan struct{}, capacity),
	}
}

func (c *HandoffCoordinator) SetContextStore(store session.RecoveryStore) {
	if c != nil && !isNil(store) {
		c.context = store
	}
}

func (c *HandoffCoordinator) warmPrefix(ctx context.Context, work HandoffWork) (reasoner.Prefix, error) {
	if c == nil || isNil(c.context) {
		return reasoner.Prefix{}, nil
	}
	snapshot, err := c.context.LoadRecovery(ctx, work.Scope, reasoner.MaxPrefixItems, reasoner.MaxPrefixBytes)
	if err != nil {
		return reasoner.Prefix{}, err
	}
	lines := make([]reasoner.PrefixLine, 0, len(snapshot.CommittedItems)+1)
	for _, item := range snapshot.CommittedItems {
		if reasoner.IsBackchannel(item.Content) {
			continue
		}
		lines = append(lines, reasoner.PrefixLine{Role: item.Role, Text: item.Content})
	}
	if work.Transcript != "" && !reasoner.IsBackchannel(work.Transcript) {
		lines = append(lines, reasoner.PrefixLine{Role: "user", Text: work.Transcript})
	}
	if len(lines) == 0 {
		return reasoner.Prefix{}, nil
	}
	prefix, err := reasoner.BuildPrefix(string(work.Scope.ConversationID), "", lines)
	if err != nil || prefix.Hash == "" {
		return reasoner.Prefix{}, err
	}
	return prefix, nil
}

// SetFeatureGates installs the process-local immutable authority view. New
// creation is checked after queueing/replay, immediately before durable Create.

func (c *HandoffCoordinator) SetFeatureGates(gates interface{ Enabled(string) bool }) error {
	if c == nil || isNil(gates) {
		return errors.New("handoff feature gates are required")
	}
	c.gates = gates
	return nil
}

func (c *HandoffCoordinator) Enqueue(work HandoffWork) error {
	if c == nil || c.work == nil {
		return errors.New("handoff coordinator is not configured")
	}
	if c.unavailable.Load() || c.health.anyUnavailable() {
		return ErrDurabilityUnavailable
	}
	retained, err := validateHandoffWorkBounds(work)
	if err != nil {
		return err
	}
	select {
	case c.slots <- struct{}{}:
	default:
		return ErrCoordinatorOverflow
	}
	c.mu.Lock()
	if retained > CoordinatorAggregateBytes-c.retainedBytes {
		c.mu.Unlock()
		<-c.slots
		return ErrCoordinatorOverflow
	}
	c.retainedBytes += retained
	c.mu.Unlock()
	owned := cloneHandoffWork(work)
	if !isNil(c.metricsClock) {
		owned.queuedAt = c.metricsClock.Now()
	}
	c.work <- owned
	return nil
}

func (c *HandoffCoordinator) Run(ctx context.Context) error {
	if c == nil || c.work == nil || isNil(c.service) || isNil(c.submit) {
		return errors.New("handoff coordinator is not configured")
	}
	if ctx == nil {
		return errors.New("handoff coordinator context is nil")
	}
	parentCtx := ctx
	var affinityGuard *coordinatorAffinityGuard
	if c.journal != nil {
		affinityGuard = startCoordinatorAffinityGuard(ctx, c.journal, c.journalAffinity, c.journalOwner, c.claimLease)
		defer affinityGuard.stopAndJoin()
		ctx = affinityGuard.ctx
	}
	if c.journal != nil {
		if err := c.retryDurability(ctx, func() error { return c.drainJournal(ctx) }); err != nil {
			if ctx.Err() != nil {
				return coordinatorCancellationResult(parentCtx, affinityGuard)
			}
			return err
		}
	}
	var replay <-chan time.Time
	stopReplay := func() {}
	if c.journal != nil {
		replay, stopReplay = durabilityTicker(c.outageClock, coordinatorReplayInterval(c.claimLease))
		defer stopReplay()
	}
	for {
		select {
		case <-ctx.Done():
			return coordinatorCancellationResult(parentCtx, affinityGuard)
		case <-replay:
			if err := c.retryDurability(ctx, func() error { return c.drainJournal(ctx) }); err != nil {
				if ctx.Err() != nil {
					return coordinatorCancellationResult(parentCtx, affinityGuard)
				}
				return err
			}
		case work := <-c.work:
			workCtx := ctx
			var span trace.Span
			if c.tracer != nil {
				workCtx, span = c.tracer.Start(ctx, "gateway.handoff.ack")
			}
			err := c.retryDurability(workCtx, func() error {
				// A request disabled while queued must not create even a coordinator
				// journal row. process repeats this check at the Create boundary.
				if c.creationDisabled(work) {
					completion, retryErr := c.process(workCtx, work)
					if retryErr != nil {
						return retryErr
					}
					return c.submitCompletion(workCtx, work, completion)
				}
				if c.journal != nil && work.RejectReason == "" {
					canonical, canonicalErr := canonicalizeHandoffWork(work)
					if canonicalErr == nil {
						err := c.admitJournal(workCtx, canonical)
						if err == nil {
							err = c.drainJournal(workCtx)
						}
						return err
					} else {
						completion, retryErr := c.process(workCtx, work)
						if retryErr != nil {
							return retryErr
						}
						return c.submitCompletion(workCtx, work, completion)
					}
				} else {
					completion, retryErr := c.process(workCtx, work)
					if retryErr != nil {
						return retryErr
					}
					return c.submitCompletion(workCtx, work, completion)
				}
			})
			result := "ok"
			if err != nil {
				result = "error"
			}
			if c.ackMetric != nil && !work.queuedAt.IsZero() {
				elapsed := c.metricsClock.Now().Sub(work.queuedAt)
				if elapsed < 0 {
					elapsed = 0
				}
				c.ackMetric(elapsed, result)
			}
			if span != nil {
				span.SetAttributes(attribute.String("result", result))
				span.End()
			}
			c.releaseWork(work)
			if err != nil {
				if ctx.Err() != nil {
					return coordinatorCancellationResult(parentCtx, affinityGuard)
				}
				return err
			}
		}
	}
}

func (c *HandoffCoordinator) creationDisabled(work HandoffWork) bool {
	return c != nil && c.gates != nil && work.RejectReason == "" && work.ToolName == handoff.ToolStartBackgroundTask && !c.gates.Enabled(feature.HandoffsEnabled)
}

func (c *HandoffCoordinator) submitCompletion(ctx context.Context, work HandoffWork, completion session.HandoffAcceptedInput) error {
	err := c.submit.Submit(ctx, work.SessionID, work.Incarnation, completion)
	if errors.Is(err, session.ErrSessionNotActive) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("submit handoff completion: %w", err)
	}
	return nil
}

func (c *HandoffCoordinator) releaseWork(work HandoffWork) {
	retained, _ := validateHandoffWorkBounds(work)
	c.mu.Lock()
	c.retainedBytes -= retained
	c.mu.Unlock()
	<-c.slots
}

func (c *HandoffCoordinator) admitJournal(ctx context.Context, work HandoffWork) error {
	payload, err := json.Marshal(work)
	if err != nil {
		return fmt.Errorf("encode handoff journal work: %w", err)
	}
	_, err = c.journal.AdmitCoordinatorWork(ctx, postgres.CoordinatorJournalRecord{
		WorkKey: handoffWorkKey(work), Kind: postgres.CoordinatorKindHandoff,
		Scope: work.Scope, SessionID: work.SessionID, Incarnation: work.Incarnation,
		LocalSequence: uint64(work.OriginEpoch), DeliveryAffinity: c.journalAffinity, Payload: payload,
	})
	return err
}

func (c *HandoffCoordinator) drainJournal(ctx context.Context) error {
	records, err := c.journal.ClaimCoordinatorWork(
		ctx, postgres.CoordinatorKindHandoff, c.journalAffinity, c.journalOwner, c.claimLease, 1,
	)
	if err != nil {
		return err
	}
	for _, record := range records {
		var work HandoffWork
		if err := decodeCoordinatorPayload(record.Payload, &work); err != nil {
			return errors.New("corrupt handoff coordinator journal payload")
		}
		canonical, err := canonicalizeHandoffWork(work)
		if err != nil || handoffWorkKey(canonical) != record.WorkKey || uint64(canonical.OriginEpoch) != record.LocalSequence ||
			canonical.Scope != record.Scope || canonical.SessionID != record.SessionID || canonical.Incarnation != record.Incarnation {
			return errors.New("corrupt handoff coordinator journal record")
		}
		operationCtx, guard := startCoordinatorLeaseGuard(
			ctx, c.journal, record.WorkKey, c.journalOwner, record.ClaimGeneration, c.claimLease,
		)
		completion, retryErr := c.process(operationCtx, canonical)
		if retryErr != nil {
			if leaseErr := guard.stopAndJoin(); leaseErr != nil {
				return fmt.Errorf("handoff coordinator lease lost: %w", leaseErr)
			}
			return retryErr
		}
		if err := ctx.Err(); err != nil {
			_ = guard.stopAndJoin()
			return err
		}
		err = c.submit.Submit(operationCtx, canonical.SessionID, canonical.Incarnation, completion)
		var fenceErr error
		if err == nil || errors.Is(err, session.ErrSessionNotActive) {
			fenceErr = c.journal.RenewCoordinatorWork(
				operationCtx, record.WorkKey, c.journalOwner, record.ClaimGeneration, c.claimLease,
			)
		}
		leaseErr := guard.stopAndJoin()
		if fenceErr != nil {
			return fmt.Errorf("handoff coordinator lease lost: %w", fenceErr)
		}
		if leaseErr != nil {
			return fmt.Errorf("handoff coordinator lease lost: %w", leaseErr)
		}
		if completion.Reason == "handoffs_disabled" {
			if discarder, ok := c.journal.(interface {
				DiscardCoordinatorWork(context.Context, string, string, uint64) error
			}); ok {
				if err := discarder.DiscardCoordinatorWork(ctx, record.WorkKey, c.journalOwner, record.ClaimGeneration); err != nil {
					return err
				}
				continue
			}
		}
		if errors.Is(err, session.ErrSessionNotActive) {
			// Incarnations are immutable and cannot become active again. The
			// durable handoff event remains the reconnect recovery source, so a
			// stale live delivery is terminal instead of a poison journal row.
			if err := c.journal.AckCoordinatorWork(ctx, record.WorkKey, c.journalOwner, record.ClaimGeneration); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("submit handoff completion: %w", err)
		}
		if err := c.journal.AckCoordinatorWork(ctx, record.WorkKey, c.journalOwner, record.ClaimGeneration); err != nil {
			return err
		}
	}
	return nil
}

func canonicalizeHandoffWork(work HandoffWork) (HandoffWork, error) {
	parsed, err := handoff.ParseReservedTool(work.ToolName, work.Arguments)
	if err != nil {
		return HandoffWork{}, err
	}
	canonical, err := json.Marshal(parsed)
	if err != nil {
		return HandoffWork{}, err
	}
	work.Arguments = canonical
	if _, err := validateHandoffWorkBounds(work); err != nil {
		return HandoffWork{}, err
	}
	return work, nil
}

func handoffWorkKey(work HandoffWork) string {
	return "handoff:" + handoffIdempotencyKey(work, work.Arguments)
}

func (c *HandoffCoordinator) process(appCtx context.Context, work HandoffWork) (session.HandoffAcceptedInput, error) {
	completion := session.HandoffAcceptedInput{
		OriginEpoch: work.OriginEpoch, Turn: work.Turn, ResponseID: work.ResponseID, CallID: work.CallID,
	}
	if work.RejectReason == "" && (work.OriginEpoch == 0 || work.Turn.ID == "" || work.Turn.Revision == 0) {
		completion.Reason = "missing_committed_turn"
		return completion, nil
	}
	if work.RejectReason != "" {
		completion.Reason = boundedReason(work.RejectReason)
		return completion, nil
	}
	parsed, err := handoff.ParseReservedTool(work.ToolName, work.Arguments)
	if err != nil {
		completion.Reason = "invalid_reserved_tool"
		return completion, nil
	}

	var snapshot handoff.Snapshot
	switch args := parsed.(type) {
	case handoff.StartBackgroundTaskArgs:
		if c.gates != nil && !c.gates.Enabled(feature.HandoffsEnabled) {
			completion.Reason = "handoffs_disabled"
			return completion, nil
		}
		canonical, marshalErr := json.Marshal(args)
		if marshalErr != nil {
			completion.Reason = "invalid_reserved_tool"
			return completion, nil
		}
		request := handoff.CreateRequest{
			Scope: work.Scope, SessionID: work.SessionID, SourceTurn: work.Turn,
			UserRequest: work.Transcript, Objective: args.Objective,
			ContextRef: work.Snapshot.ContextRef, ContextHash: work.Snapshot.ContextHash,
			Deadline: work.Snapshot.Deadline.UTC(), TokenBudget: work.Snapshot.TokenBudget,
			Priority: work.Snapshot.Priority, ToolAllowlist: cloneStringSlice(work.Snapshot.ToolAllowlist),
			ApprovalPolicy: append(json.RawMessage(nil), work.Snapshot.ApprovalPolicy...),
			Versions:       work.Snapshot.Versions, PresentationHint: work.Snapshot.PresentationHint,
			IdempotencyKey: handoffIdempotencyKey(work, canonical),
		}
		prefix, prefixErr := c.warmPrefix(appCtx, work)
		if prefixErr != nil {
			completion.Reason = "context_unavailable"
			return completion, nil
		}
		if prefix.Hash != "" {
			request.ContextRef = prefix.Ref
			request.ContextHash = prefix.Hash
			request.ContextPrefix = append(json.RawMessage(nil), prefix.Raw...)
		}
		snapshot, err = c.service.Create(appCtx, request)
	case handoff.SteerBackgroundTaskArgs:
		current, getErr := c.service.Get(appCtx, work.Scope, args.HandoffID)
		if getErr != nil {
			err = getErr
			break
		}
		if current.Request.Objective == args.Objective {
			snapshot = current
			break
		}
		canonical, _ := json.Marshal(args)
		snapshot, err = c.service.Steer(appCtx, work.Scope, args.HandoffID, handoff.SteerRequest{
			Objective: args.Objective, ExpectedRevision: current.ObjectiveRevision + 1,
			Actor: "assistant", IdempotencyKey: handoffIdempotencyKey(work, canonical),
		})
	case handoff.CancelBackgroundTaskArgs:
		canonical, _ := json.Marshal(args)
		snapshot, err = c.service.Cancel(appCtx, work.Scope, args.HandoffID, handoff.CancelRequest{
			Actor: "assistant", Reason: args.Reason, IdempotencyKey: handoffIdempotencyKey(work, canonical),
		})
	default:
		err = handoff.ErrInvalidRequest
	}
	if err != nil {
		if errors.Is(err, handoff.ErrHandoffsDisabled) {
			completion.Reason = "handoffs_disabled"
			return completion, nil
		}
		if !isStableHandoffRejection(err) {
			return session.HandoffAcceptedInput{}, fmt.Errorf("process handoff work: %w", err)
		}
		completion.Reason = "handoff_rejected"
		return completion, nil
	}
	if snapshot.ID == "" {
		return session.HandoffAcceptedInput{}, errors.New("process handoff work: empty handoff snapshot")
	}
	completion.Accepted = true
	completion.HandoffID = snapshot.ID
	return completion, nil
}

func isStableHandoffRejection(err error) bool {
	for _, stable := range []error{
		handoff.ErrInvalidRequest,
		handoff.ErrNotFound,
		handoff.ErrUncommittedTurn,
		handoff.ErrIdempotencyConflict,
		handoff.ErrRevisionConflict,
		handoff.ErrInvalidTransition,
		handoff.ErrTerminalState,
		handoff.ErrSteeringBlocked,
		handoff.ErrHandoffsDisabled,
	} {
		if errors.Is(err, stable) {
			return true
		}
	}
	return false
}

func handoffIdempotencyKey(work HandoffWork, canonical []byte) string {
	hash := sha256.New()
	for _, value := range []string{
		work.Scope.TenantID, string(work.Scope.ConversationID), work.Turn.ID,
		fmt.Sprintf("%d", work.Turn.Revision), work.CallID, work.ToolName,
	} {
		hash.Write([]byte(value))
		hash.Write([]byte{0})
	}
	hash.Write(canonical)
	return hex.EncodeToString(hash.Sum(nil))
}

func cloneHandoffWork(work HandoffWork) HandoffWork {
	work.Arguments = append(json.RawMessage(nil), work.Arguments...)
	work.Snapshot.ToolAllowlist = cloneStringSlice(work.Snapshot.ToolAllowlist)
	work.Snapshot.ApprovalPolicy = append(json.RawMessage(nil), work.Snapshot.ApprovalPolicy...)
	return work
}

func cloneStringSlice(values []string) []string {
	if values == nil {
		return nil
	}
	return append([]string{}, values...)
}

func cloneHandoffSnapshot(snapshot SessionHandoffSnapshot) SessionHandoffSnapshot {
	snapshot.ToolAllowlist = cloneStringSlice(snapshot.ToolAllowlist)
	snapshot.ApprovalPolicy = append(json.RawMessage(nil), snapshot.ApprovalPolicy...)
	return snapshot
}

func boundedReason(reason string) string {
	if len(reason) > 1024 {
		return reason[:1024]
	}
	return reason
}

func validateHandoffWorkBounds(work HandoffWork) (int, error) {
	stringsToCheck := []string{
		string(work.SessionID), work.Scope.TenantID, work.Scope.UserID, string(work.Scope.ConversationID),
		work.CallID, work.ResponseID, work.ToolName,
	}
	retained := len(work.Arguments) + len(work.Transcript) + len(work.Snapshot.ApprovalPolicy)
	for _, value := range stringsToCheck {
		if strings.TrimSpace(value) == "" || !utf8.ValidString(value) || len(value) > session.DurableIdentifierBytes {
			return 0, ErrCoordinatorWorkTooLarge
		}
		retained += len(value)
	}
	if work.Incarnation == 0 || work.OriginEpoch == 0 ||
		len(work.Arguments) == 0 || len(work.Arguments) > session.DurableArgumentsBytes ||
		len(work.Transcript) > session.DurableTranscriptBytes || !utf8.ValidString(work.Transcript) ||
		len(work.Snapshot.ApprovalPolicy) > 64<<10 || len(work.Snapshot.ToolAllowlist) > 64 {
		return 0, ErrCoordinatorWorkTooLarge
	}
	if work.RejectReason == "" {
		if strings.TrimSpace(work.Turn.ID) == "" || !utf8.ValidString(work.Turn.ID) ||
			len(work.Turn.ID) > session.DurableIdentifierBytes || work.Turn.Revision == 0 {
			return 0, ErrCoordinatorWorkTooLarge
		}
		retained += len(work.Turn.ID)
	} else if len(work.RejectReason) > 1024 || !utf8.ValidString(work.RejectReason) {
		return 0, ErrCoordinatorWorkTooLarge
	}
	retained += len(work.RejectReason)
	for _, value := range []string{work.Snapshot.ContextRef, work.Snapshot.ContextHash, work.Snapshot.PresentationHint} {
		if !utf8.ValidString(value) || len(value) > 2048 {
			return 0, ErrCoordinatorWorkTooLarge
		}
		retained += len(value)
	}
	for _, tool := range work.Snapshot.ToolAllowlist {
		if strings.TrimSpace(tool) == "" || !utf8.ValidString(tool) || len(tool) > session.DurableIdentifierBytes {
			return 0, ErrCoordinatorWorkTooLarge
		}
		retained += len(tool)
	}
	if retained > CoordinatorAggregateBytes {
		return 0, ErrCoordinatorWorkTooLarge
	}
	return retained, nil
}
