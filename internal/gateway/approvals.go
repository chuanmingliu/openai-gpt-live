package gateway

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
)

const (
	defaultApprovalQueueCapacity  = 64
	defaultApprovalRequestTimeout = 5 * time.Second
	maximumApprovalRequestTimeout = 30 * time.Second
)

type ApprovalWork struct {
	SessionID        identity.SessionID
	Incarnation      identity.Incarnation
	ExternalSequence uint64
	Scope            handoff.Scope
	Resolution       handoff.ApprovalResolution
	Deadline         time.Time
}

type ApprovalCoordinator struct {
	store          handoff.ApprovalStore
	submit         SessionSubmitter
	work           chan ApprovalWork
	slots          chan struct{}
	requestTimeout time.Duration
}

func NewApprovalCoordinator(store handoff.ApprovalStore, submit SessionSubmitter, capacity int, requestTimeout time.Duration) *ApprovalCoordinator {
	if isNil(store) || isNil(submit) || capacity <= 0 || capacity > 10_000 ||
		requestTimeout <= 0 || requestTimeout > maximumApprovalRequestTimeout {
		return nil
	}
	return &ApprovalCoordinator{
		store: store, submit: submit, work: make(chan ApprovalWork, capacity),
		slots: make(chan struct{}, capacity), requestTimeout: requestTimeout,
	}
}

func (c *ApprovalCoordinator) Enqueue(work ApprovalWork) error {
	if c == nil || c.work == nil {
		return errors.New("approval coordinator is not configured")
	}
	if err := validateApprovalWork(work); err != nil {
		return err
	}
	select {
	case c.slots <- struct{}{}:
	default:
		return ErrCoordinatorOverflow
	}
	work.Deadline = time.Now().UTC().Add(c.requestTimeout)
	c.work <- work
	return nil
}

func (c *ApprovalCoordinator) Run(ctx context.Context) error {
	if c == nil || c.work == nil || isNil(c.store) || isNil(c.submit) {
		return errors.New("approval coordinator is not configured")
	}
	if ctx == nil {
		return errors.New("approval coordinator context is nil")
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case work := <-c.work:
			requestCtx, cancel := context.WithDeadline(ctx, work.Deadline)
			err := c.store.ResolveApproval(requestCtx, work.Scope, work.Resolution)
			cancel()
			<-c.slots
			if ctx.Err() != nil {
				return nil
			}
			completionCtx, completionCancel := context.WithTimeout(ctx, c.requestTimeout)
			_ = c.submit.Submit(completionCtx, work.SessionID, work.Incarnation, session.ApprovalResolutionCompletedInput{
				ExternalSequence: work.ExternalSequence,
				InvocationID:     work.Resolution.InvocationID,
				Accepted:         err == nil,
			})
			completionCancel()
		}
	}
}

func validateApprovalWork(work ApprovalWork) error {
	for _, value := range []string{
		string(work.SessionID), work.Scope.TenantID, work.Scope.UserID, string(work.Scope.ConversationID),
	} {
		if strings.TrimSpace(value) == "" || len(value) > session.DurableIdentifierBytes || !utf8.ValidString(value) {
			return ErrCoordinatorWorkTooLarge
		}
	}
	if work.Incarnation == 0 || work.ExternalSequence == 0 {
		return ErrCoordinatorWorkTooLarge
	}
	if err := handoff.ValidateApprovalResolution(work.Resolution); err != nil {
		return err
	}
	return nil
}

type approvalDispatcher struct {
	coordinator *ApprovalCoordinator
}

func (d *approvalDispatcher) DispatchApproval(trusted session.TrustedSessionContext, request session.ApprovalResolutionRequestedEffect) error {
	if d == nil || d.coordinator == nil {
		return errors.New("approval coordinator is unavailable")
	}
	return d.coordinator.Enqueue(ApprovalWork{
		SessionID: trusted.IDs.SessionID, Incarnation: trusted.IDs.Incarnation,
		ExternalSequence: request.ExternalSequence,
		Scope: handoff.Scope{
			TenantID: trusted.Subject.TenantID, UserID: trusted.Subject.UserID, ConversationID: trusted.IDs.ConversationID,
		},
		Resolution: handoff.ApprovalResolution{
			InvocationID: request.InvocationID, Decision: request.Decision,
			ActorTenantID: trusted.Subject.TenantID, ActorUserID: trusted.Subject.UserID,
			Reason: request.Reason, IdempotencyKey: request.IdempotencyKey, ResolvedAt: request.ResolvedAt,
		},
	})
}
