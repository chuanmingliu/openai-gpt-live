package handoff

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
)

type Clock interface {
	Now() time.Time
}

type IDGenerator interface {
	NewHandoffID() identity.HandoffID
	NewEventID() string
}

type Service interface {
	Create(context.Context, CreateRequest) (Snapshot, error)
	Get(context.Context, Scope, identity.HandoffID) (Snapshot, error)
	ListActive(context.Context, Scope) ([]Snapshot, error)
	Steer(context.Context, Scope, identity.HandoffID, SteerRequest) (Snapshot, error)
	ProvideInput(context.Context, Scope, identity.HandoffID, InputRequest) (Snapshot, error)
	Cancel(context.Context, Scope, identity.HandoffID, CancelRequest) (Snapshot, error)
}

type service struct {
	repo  Repository
	clock Clock
	ids   IDGenerator
}

func NewService(repo Repository, clock Clock, ids IDGenerator) (Service, error) {
	if isNilInterface(repo) || isNilInterface(clock) || isNilInterface(ids) {
		return nil, invalid("service dependencies are required", nil)
	}
	return &service{repo: repo, clock: clock, ids: ids}, nil
}

func (s *service) Create(ctx context.Context, request CreateRequest) (Snapshot, error) {
	now := s.clock.Now().UTC()
	normalized, err := ValidateCreateRequest(request, now)
	if err != nil {
		return Snapshot{}, err
	}
	committed, err := s.repo.IsCommittedTurn(ctx, normalized.Scope, normalized.SourceTurn)
	if err != nil {
		return Snapshot{}, fmt.Errorf("check committed turn: %w", err)
	}
	if !committed {
		return Snapshot{}, ErrUncommittedTurn
	}

	hash, err := CanonicalRequestHash(normalized)
	if err != nil {
		return Snapshot{}, err
	}
	handoffID := s.ids.NewHandoffID()
	if err := boundedRequired("generated handoff id", string(handoffID), maxIdentityBytes); err != nil {
		return Snapshot{}, err
	}
	eventID := s.ids.NewEventID()
	if err := boundedRequired("generated event id", eventID, maxIdentityBytes); err != nil {
		return Snapshot{}, err
	}
	record := CreateRecord{HandoffID: handoffID, EventID: eventID, Request: cloneRequest(normalized), RequestHash: hash, CreatedAt: now}
	snapshot, created, err := s.repo.CreateHandoff(ctx, record)
	if err != nil {
		return Snapshot{}, fmt.Errorf("create handoff: %w", err)
	}
	if snapshot.ID == "" {
		return Snapshot{}, ErrNotFound
	}
	if created && snapshot.ID != handoffID {
		return Snapshot{}, ErrNotFound
	}
	if !sameScope(snapshot.Request.Scope, normalized.Scope) {
		return Snapshot{}, ErrNotFound
	}
	if !created && snapshot.RequestHash != hash {
		return Snapshot{}, ErrIdempotencyConflict
	}
	return CloneSnapshot(snapshot), nil
}

func (s *service) Get(ctx context.Context, scope Scope, id identity.HandoffID) (Snapshot, error) {
	if err := validateScopeAndID(scope, id); err != nil {
		return Snapshot{}, err
	}
	snapshot, err := s.repo.GetHandoff(ctx, scope, id)
	if err != nil {
		return Snapshot{}, fmt.Errorf("get handoff: %w", err)
	}
	return checkedSnapshot(snapshot, scope, id)
}

func (s *service) ListActive(ctx context.Context, scope Scope) ([]Snapshot, error) {
	if err := validateScope(scope); err != nil {
		return nil, err
	}
	snapshots, err := s.repo.ListActive(ctx, scope)
	if err != nil {
		return nil, fmt.Errorf("list active handoffs: %w", err)
	}
	for _, snapshot := range snapshots {
		if snapshot.ID == "" || !sameScope(snapshot.Request.Scope, scope) {
			return nil, ErrNotFound
		}
	}
	return cloneSnapshots(snapshots), nil
}

func (s *service) Steer(ctx context.Context, scope Scope, id identity.HandoffID, request SteerRequest) (Snapshot, error) {
	if err := validateScopeAndID(scope, id); err != nil {
		return Snapshot{}, err
	}
	if err := validateSteer(request); err != nil {
		return Snapshot{}, err
	}
	snapshot, err := s.repo.SteerHandoff(ctx, scope, id, request)
	if err != nil {
		return Snapshot{}, fmt.Errorf("steer handoff: %w", err)
	}
	return checkedSnapshot(snapshot, scope, id)
}

func (s *service) ProvideInput(ctx context.Context, scope Scope, id identity.HandoffID, request InputRequest) (Snapshot, error) {
	if err := validateScopeAndID(scope, id); err != nil {
		return Snapshot{}, err
	}
	validated, err := validateInput(request)
	if err != nil {
		return Snapshot{}, err
	}
	snapshot, err := s.repo.ProvideHandoffInput(ctx, scope, id, validated)
	if err != nil {
		return Snapshot{}, fmt.Errorf("provide handoff input: %w", err)
	}
	return checkedSnapshot(snapshot, scope, id)
}

func (s *service) Cancel(ctx context.Context, scope Scope, id identity.HandoffID, request CancelRequest) (Snapshot, error) {
	if err := validateScopeAndID(scope, id); err != nil {
		return Snapshot{}, err
	}
	if err := validateCancel(request); err != nil {
		return Snapshot{}, err
	}
	snapshot, err := s.repo.RequestHandoffCancel(ctx, scope, id, request)
	if err != nil {
		return Snapshot{}, fmt.Errorf("cancel handoff: %w", err)
	}
	return checkedSnapshot(snapshot, scope, id)
}

func validateScopeAndID(scope Scope, id identity.HandoffID) error {
	if err := validateScope(scope); err != nil {
		return err
	}
	return boundedRequired("handoff id", string(id), maxIdentityBytes)
}

func validateSteer(request SteerRequest) error {
	if err := boundedRequired("objective", request.Objective, maxObjectiveBytes); err != nil {
		return err
	}
	if request.ExpectedRevision == 0 {
		return invalid("expected revision must be positive", nil)
	}
	if err := boundedRequired("actor", request.Actor, maxIdentityBytes); err != nil {
		return err
	}
	return boundedRequired("idempotency key", request.IdempotencyKey, maxIdentityBytes)
}

func validateInput(request InputRequest) (InputRequest, error) {
	if request.ExpectedRevision == 0 {
		return InputRequest{}, invalid("expected revision must be positive", nil)
	}
	if err := boundedRequired("actor", request.Actor, maxIdentityBytes); err != nil {
		return InputRequest{}, err
	}
	if err := boundedRequired("idempotency key", request.IdempotencyKey, maxIdentityBytes); err != nil {
		return InputRequest{}, err
	}
	input, err := canonicalJSON(request.Input, maxJSONBytes, maxJSONDepth)
	if err != nil {
		return InputRequest{}, invalid("input", err)
	}
	request.Input = input
	return request, nil
}

func validateCancel(request CancelRequest) error {
	if err := boundedRequired("actor", request.Actor, maxIdentityBytes); err != nil {
		return err
	}
	if err := boundedOptional("reason", request.Reason, maxReasonBytes); err != nil {
		return err
	}
	return boundedRequired("idempotency key", request.IdempotencyKey, maxIdentityBytes)
}

func checkedSnapshot(snapshot Snapshot, scope Scope, id identity.HandoffID) (Snapshot, error) {
	if snapshot.ID != id || !sameScope(snapshot.Request.Scope, scope) {
		return Snapshot{}, ErrNotFound
	}
	return CloneSnapshot(snapshot), nil
}

func sameScope(left, right Scope) bool {
	return left.TenantID == right.TenantID && left.UserID == right.UserID && left.ConversationID == right.ConversationID
}

func isNilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
