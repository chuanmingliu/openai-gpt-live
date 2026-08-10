package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/chuanmingliu/openai-gpt-live/internal/store/postgres"
)

var (
	ErrCoordinatorOverflow     = errors.New("durable coordinator queue is full")
	ErrDurabilityUnavailable   = errors.New("durability unavailable")
	ErrDurabilityGraceExpired  = errors.New("durability grace expired")
	ErrCoordinatorAffinityLost = errors.New("coordinator delivery shard authority lost")
)

type TranscriptWork struct {
	SessionID   identity.SessionID
	Incarnation identity.Incarnation
	Scope       handoff.Scope
	Turn        identity.TurnKey
	Sequence    uint64
	Role        string
	FinalText   string
}

type SessionSubmitter interface {
	Submit(context.Context, identity.SessionID, identity.Incarnation, session.Input) error
}

type ConversationWriter interface {
	AppendCommittedItem(context.Context, handoff.Scope, postgres.ConversationItem) (postgres.ConversationItem, error)
}

type CoordinatorJournal interface {
	AcquireCoordinatorAffinity(context.Context, string, string, time.Duration) (uint64, bool, error)
	RenewCoordinatorAffinity(context.Context, string, string, uint64, time.Duration) error
	AdmitCoordinatorWork(context.Context, postgres.CoordinatorJournalRecord) (postgres.CoordinatorJournalRecord, error)
	ClaimCoordinatorWork(context.Context, string, string, string, time.Duration, int) ([]postgres.CoordinatorJournalRecord, error)
	RenewCoordinatorWork(context.Context, string, string, uint64, time.Duration) error
	AckCoordinatorWork(context.Context, string, string, uint64) error
}

type coordinatorAffinityGuard struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan error
	once   sync.Once
	err    error
}

func startCoordinatorAffinityGuard(
	ctx context.Context,
	journal CoordinatorJournal,
	affinity, owner string,
	lease time.Duration,
) *coordinatorAffinityGuard {
	guard := &coordinatorAffinityGuard{done: make(chan error, 1)}
	guard.ctx, guard.cancel = context.WithCancel(ctx)
	go func() {
		err := runCoordinatorAffinityAuthority(guard.ctx, journal, affinity, owner, lease)
		if err != nil {
			guard.cancel()
		}
		guard.done <- err
	}()
	return guard
}

func (g *coordinatorAffinityGuard) stopAndJoin() error {
	if g == nil {
		return nil
	}
	g.once.Do(func() {
		g.cancel()
		g.err = <-g.done
	})
	return g.err
}

type coordinatorAffinityAcquisition struct {
	generation uint64
	acquired   bool
}

type coordinatorAffinityOperationResult[T any] struct {
	value T
	err   error
}

func runBoundedCoordinatorAffinityOperation[T any](
	ctx context.Context,
	timeout time.Duration,
	lossDeadline time.Time,
	operation func(context.Context) (T, error),
) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if !lossDeadline.IsZero() && !time.Now().Before(lossDeadline) {
		return zero, context.DeadlineExceeded
	}

	operationCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var lossTimer *time.Timer
	var loss <-chan time.Time
	if !lossDeadline.IsZero() {
		lossTimer = time.NewTimer(time.Until(lossDeadline))
		loss = lossTimer.C
		defer stopCoordinatorAffinityTimer(lossTimer)
	}

	result := make(chan coordinatorAffinityOperationResult[T], 1)
	// Authority operations are strictly sequential. Any operation that ignores
	// cancellation makes this guard terminate, so at most one such goroutine can
	// be abandoned per guard; the buffered result also lets a late return exit.
	go func() {
		value, err := operation(operationCtx)
		result <- coordinatorAffinityOperationResult[T]{value: value, err: err}
	}()

	select {
	case completed := <-result:
		if err := operationCtx.Err(); err != nil {
			return zero, err
		}
		if !lossDeadline.IsZero() && !time.Now().Before(lossDeadline) {
			return zero, context.DeadlineExceeded
		}
		return completed.value, completed.err
	case <-operationCtx.Done():
		return zero, operationCtx.Err()
	case <-loss:
		return zero, context.DeadlineExceeded
	}
}

func runCoordinatorAffinityAuthority(
	ctx context.Context,
	journal CoordinatorJournal,
	affinity, owner string,
	lease time.Duration,
) error {
	interval := coordinatorAffinityHeartbeatInterval(lease)
	operationTimeout := coordinatorAffinityOperationTimeout(lease)
	var generation uint64
	var lossDeadline time.Time
	for {
		if generation == 0 {
			operationStarted := time.Now()
			acquisition, err := runBoundedCoordinatorAffinityOperation(
				ctx,
				operationTimeout,
				time.Time{},
				func(operationCtx context.Context) (coordinatorAffinityAcquisition, error) {
					acquiredGeneration, acquired, acquireErr := journal.AcquireCoordinatorAffinity(operationCtx, affinity, owner, lease)
					return coordinatorAffinityAcquisition{generation: acquiredGeneration, acquired: acquired}, acquireErr
				},
			)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("acquire coordinator delivery shard: %w", err)
			}
			if acquisition.acquired {
				if acquisition.generation == 0 {
					return errors.New("coordinator delivery shard returned an invalid generation")
				}
				generation = acquisition.generation
				// The database computes its expiry during the operation. Measuring
				// from operation start is a conservative local upper bound.
				lossDeadline = operationStarted.Add(lease)
				if !time.Now().Before(lossDeadline) {
					return ErrCoordinatorAffinityLost
				}
			}
		}
		if err := waitForCoordinatorAffinityHeartbeat(ctx, interval, lossDeadline); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if generation == 0 {
			continue
		}

		operationStarted := time.Now()
		_, err := runBoundedCoordinatorAffinityOperation(
			ctx,
			operationTimeout,
			lossDeadline,
			func(operationCtx context.Context) (struct{}, error) {
				return struct{}{}, journal.RenewCoordinatorAffinity(operationCtx, affinity, owner, generation, lease)
			},
		)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return errors.Join(ErrCoordinatorAffinityLost, err)
		}
		lossDeadline = operationStarted.Add(lease)
		if !time.Now().Before(lossDeadline) {
			return ErrCoordinatorAffinityLost
		}
	}
}

func waitForCoordinatorAffinityHeartbeat(ctx context.Context, interval time.Duration, lossDeadline time.Time) error {
	wait := interval
	if !lossDeadline.IsZero() {
		remaining := time.Until(lossDeadline)
		if remaining <= 0 {
			return ErrCoordinatorAffinityLost
		}
		if remaining < wait {
			wait = remaining
		}
	}
	timer := time.NewTimer(wait)
	defer stopCoordinatorAffinityTimer(timer)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		if !lossDeadline.IsZero() && !time.Now().Before(lossDeadline) {
			return ErrCoordinatorAffinityLost
		}
		return nil
	}
}

func stopCoordinatorAffinityTimer(timer *time.Timer) {
	if timer == nil || timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}

func coordinatorAffinityHeartbeatInterval(lease time.Duration) time.Duration {
	interval := lease / 3
	if interval < time.Millisecond {
		return time.Millisecond
	}
	if interval > 5*time.Second {
		return 5 * time.Second
	}
	return interval
}

func coordinatorAffinityOperationTimeout(lease time.Duration) time.Duration {
	if lease < time.Millisecond {
		return time.Millisecond
	}
	if lease > 5*time.Second {
		return 5 * time.Second
	}
	return lease
}

func coordinatorCancellationResult(parent context.Context, guard *coordinatorAffinityGuard) error {
	err := guard.stopAndJoin()
	if parent.Err() != nil {
		return nil
	}
	if err == nil {
		return ErrCoordinatorAffinityLost
	}
	return err
}

type TranscriptCoordinator struct {
	writer          ConversationWriter
	submit          SessionSubmitter
	work            chan TranscriptWork
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
}

func (c *TranscriptCoordinator) configureOutage(grace time.Duration, source clock.Clock) error {
	if c == nil || grace <= 0 || grace > 10*time.Minute || isNil(source) {
		return errors.New("invalid durability outage policy")
	}
	c.outageGrace, c.outageClock = grace, source
	if c.health == nil {
		c.health = &durabilityHealth{}
	}
	return nil
}

func (c *TranscriptCoordinator) beginOutage() {
	if c.unavailable.CompareAndSwap(false, true) {
		c.outageStarted = c.outageClock.Now()
	}
	c.health.markUnavailable(transcriptLane)
}

func (c *TranscriptCoordinator) endOutage() {
	if c.unavailable.CompareAndSwap(true, false) {
		c.health.markHealthy(transcriptLane)
	}
	c.outageStarted = time.Time{}
}

func (c *TranscriptCoordinator) retryDurability(ctx context.Context, operation func() error) error {
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

func (c *TranscriptCoordinator) waitOutage(ctx context.Context) error {
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

func (c *TranscriptCoordinator) configureJournal(journal CoordinatorJournal, affinity, owner string, lease time.Duration) error {
	if c == nil || isNil(journal) || strings.TrimSpace(affinity) == "" || len(affinity) > session.DurableIdentifierBytes ||
		strings.TrimSpace(owner) == "" || len(owner) > session.DurableIdentifierBytes ||
		lease < postgres.CoordinatorMinLease || lease > postgres.CoordinatorMaxLease {
		return errors.New("invalid transcript coordinator journal")
	}
	c.journal, c.journalAffinity, c.journalOwner, c.claimLease = journal, affinity, owner, lease
	return nil
}

func NewTranscriptCoordinator(writer ConversationWriter, submit SessionSubmitter, capacity int) *TranscriptCoordinator {
	if isNil(writer) || isNil(submit) || capacity <= 0 || capacity > 10_000 {
		return nil
	}
	return &TranscriptCoordinator{
		writer: writer, submit: submit, work: make(chan TranscriptWork, capacity), slots: make(chan struct{}, capacity),
	}
}

func (c *TranscriptCoordinator) Enqueue(work TranscriptWork) error {
	if c == nil || c.work == nil {
		return errors.New("transcript coordinator is not configured")
	}
	retained, err := validateTranscriptWorkBounds(work)
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
	c.work <- work
	return nil
}

func (c *TranscriptCoordinator) Run(ctx context.Context) error {
	if c == nil || c.work == nil || isNil(c.writer) || isNil(c.submit) {
		return errors.New("transcript coordinator is not configured")
	}
	if ctx == nil {
		return errors.New("transcript coordinator context is nil")
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
			err := c.retryDurability(ctx, func() error {
				if c.journal != nil {
					err := c.admitJournal(ctx, work)
					if err == nil {
						err = c.drainJournal(ctx)
					}
					return err
				} else {
					err := c.persist(ctx, work, work.Sequence)
					if errors.Is(err, session.ErrSessionNotActive) {
						return nil
					}
					return err
				}
			})
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

func coordinatorReplayInterval(lease time.Duration) time.Duration {
	interval := lease / 2
	if interval < 10*time.Millisecond {
		return 10 * time.Millisecond
	}
	if interval > 5*time.Second {
		return 5 * time.Second
	}
	return interval
}

func (c *TranscriptCoordinator) releaseWork(work TranscriptWork) {
	retained, _ := validateTranscriptWorkBounds(work)
	c.mu.Lock()
	c.retainedBytes -= retained
	c.mu.Unlock()
	<-c.slots
}

func (c *TranscriptCoordinator) persist(ctx context.Context, work TranscriptWork, storageSequence uint64) error {
	if err := c.append(ctx, work, storageSequence); err != nil {
		return err
	}
	return c.submitPersisted(ctx, work)
}

func (c *TranscriptCoordinator) append(ctx context.Context, work TranscriptWork, storageSequence uint64) error {
	hash := sha256.Sum256([]byte(work.FinalText))
	now := time.Now().UTC()
	item := postgres.ConversationItem{
		SessionID: work.SessionID, Turn: work.Turn, Sequence: storageSequence,
		Role: work.Role, Content: work.FinalText, ContentHash: hex.EncodeToString(hash[:]),
		CreatedAt: now, UpdatedAt: now,
	}
	if _, err := c.writer.AppendCommittedItem(ctx, work.Scope, item); err != nil {
		return fmt.Errorf("append committed transcript: %w", err)
	}
	return nil
}

func (c *TranscriptCoordinator) submitPersisted(ctx context.Context, work TranscriptWork) error {
	var input session.Input = session.TranscriptPersistedInput{Sequence: work.Sequence}
	if work.Role == "user" {
		input = session.TurnCommittedInput{Sequence: work.Sequence, Turn: work.Turn, Transcript: work.FinalText}
	}
	err := c.submit.Submit(ctx, work.SessionID, work.Incarnation, input)
	if err != nil {
		return fmt.Errorf("submit committed turn: %w", err)
	}
	return nil
}

func (c *TranscriptCoordinator) admitJournal(ctx context.Context, work TranscriptWork) error {
	payload, err := json.Marshal(work)
	if err != nil {
		return fmt.Errorf("encode transcript journal work: %w", err)
	}
	_, err = c.journal.AdmitCoordinatorWork(ctx, postgres.CoordinatorJournalRecord{
		WorkKey: transcriptWorkKey(work), Kind: postgres.CoordinatorKindTranscript,
		Scope: work.Scope, SessionID: work.SessionID, Incarnation: work.Incarnation,
		LocalSequence: work.Sequence, DeliveryAffinity: c.journalAffinity, Payload: payload,
	})
	return err
}

func (c *TranscriptCoordinator) drainJournal(ctx context.Context) error {
	records, err := c.journal.ClaimCoordinatorWork(
		ctx, postgres.CoordinatorKindTranscript, c.journalAffinity, c.journalOwner, c.claimLease, 1,
	)
	if err != nil {
		return err
	}
	for _, record := range records {
		var work TranscriptWork
		if err := decodeCoordinatorPayload(record.Payload, &work); err != nil {
			return errors.New("corrupt transcript coordinator journal payload")
		}
		if _, err := validateTranscriptWorkBounds(work); err != nil || transcriptWorkKey(work) != record.WorkKey || record.StorageSequence == 0 ||
			work.Scope != record.Scope || work.SessionID != record.SessionID || work.Incarnation != record.Incarnation || work.Sequence != record.LocalSequence {
			return errors.New("corrupt transcript coordinator journal record")
		}
		operationCtx, guard := startCoordinatorLeaseGuard(
			ctx, c.journal, record.WorkKey, c.journalOwner, record.ClaimGeneration, c.claimLease,
		)
		err := c.append(operationCtx, work, record.StorageSequence)
		if err == nil {
			err = c.submitPersisted(operationCtx, work)
		}
		var fenceErr error
		if err == nil || errors.Is(err, session.ErrSessionNotActive) {
			fenceErr = c.journal.RenewCoordinatorWork(
				operationCtx, record.WorkKey, c.journalOwner, record.ClaimGeneration, c.claimLease,
			)
		}
		leaseErr := guard.stopAndJoin()
		if fenceErr != nil {
			return fmt.Errorf("transcript coordinator lease lost: %w", fenceErr)
		}
		if leaseErr != nil {
			return fmt.Errorf("transcript coordinator lease lost: %w", leaseErr)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if errors.Is(err, session.ErrSessionNotActive) {
			// The item is already durable and an exact incarnation cannot become
			// active again. Terminate stale live delivery to avoid a permanent
			// retry row; conversation replay is the reconnect recovery path.
			if err := c.journal.AckCoordinatorWork(ctx, record.WorkKey, c.journalOwner, record.ClaimGeneration); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		if err := c.journal.AckCoordinatorWork(ctx, record.WorkKey, c.journalOwner, record.ClaimGeneration); err != nil {
			return err
		}
	}
	return nil
}

func decodeCoordinatorPayload(payload json.RawMessage, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		if err == nil {
			return errors.New("multiple coordinator payload values")
		}
		return err
	}
	canonical, err := json.Marshal(target)
	if err != nil {
		return err
	}
	if !bytes.Equal(payload, canonical) {
		return errors.New("non-canonical coordinator payload")
	}
	return nil
}

type coordinatorLeaseGuard struct {
	stop   chan struct{}
	done   chan error
	cancel context.CancelFunc
	once   sync.Once
}

func startCoordinatorLeaseGuard(
	ctx context.Context,
	journal CoordinatorJournal,
	workKey, owner string,
	generation uint64,
	lease time.Duration,
) (context.Context, *coordinatorLeaseGuard) {
	operationCtx, cancel := context.WithCancel(ctx)
	guard := &coordinatorLeaseGuard{stop: make(chan struct{}), done: make(chan error, 1), cancel: cancel}
	go func() {
		ticker := time.NewTicker(coordinatorRenewInterval(lease))
		defer ticker.Stop()
		for {
			select {
			case <-guard.stop:
				guard.done <- nil
				return
			case <-operationCtx.Done():
				guard.done <- operationCtx.Err()
				return
			case <-ticker.C:
				if err := journal.RenewCoordinatorWork(operationCtx, workKey, owner, generation, lease); err != nil {
					cancel()
					guard.done <- err
					return
				}
			}
		}
	}()
	return operationCtx, guard
}

func (g *coordinatorLeaseGuard) stopAndJoin() error {
	g.once.Do(func() { close(g.stop) })
	err := <-g.done
	g.cancel()
	return err
}

func coordinatorRenewInterval(lease time.Duration) time.Duration {
	interval := lease / 3
	if interval < time.Millisecond {
		return time.Millisecond
	}
	return interval
}

func transcriptWorkKey(work TranscriptWork) string {
	hash := sha256.New()
	for _, value := range []string{
		work.Scope.TenantID, string(work.Scope.ConversationID), string(work.SessionID),
		fmt.Sprintf("%d", work.Incarnation), fmt.Sprintf("%d", work.Sequence),
		work.Turn.ID, fmt.Sprintf("%d", work.Turn.Revision), work.Role,
	} {
		hash.Write([]byte(value))
		hash.Write([]byte{0})
	}
	return "transcript:" + hex.EncodeToString(hash.Sum(nil))
}

func validateTranscriptWorkBounds(work TranscriptWork) (int, error) {
	values := []string{
		string(work.SessionID), work.Scope.TenantID, work.Scope.UserID, string(work.Scope.ConversationID), work.Turn.ID,
	}
	retained := len(work.FinalText) + len(work.Role)
	for _, value := range values {
		if strings.TrimSpace(value) == "" || !utf8.ValidString(value) || len(value) > session.DurableIdentifierBytes {
			return 0, ErrCoordinatorWorkTooLarge
		}
		retained += len(value)
	}
	if work.Incarnation == 0 || work.Turn.Revision == 0 || work.Sequence == 0 ||
		(work.Role != "user" && work.Role != "assistant") || !utf8.ValidString(work.FinalText) ||
		len(work.FinalText) > session.DurableTranscriptBytes || retained > CoordinatorAggregateBytes {
		return 0, ErrCoordinatorWorkTooLarge
	}
	return retained, nil
}
