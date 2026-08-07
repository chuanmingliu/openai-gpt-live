package handoff

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewService_RejectsNilDependencies(t *testing.T) {
	repo := &fakeRepository{}
	clock := fixedClock{now: time.Now()}
	ids := &fixedIDs{handoffID: "h", eventID: "e"}
	_, err := NewService(nil, clock, ids)
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = NewService(repo, nil, ids)
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = NewService(repo, clock, nil)
	assert.ErrorIs(t, err, ErrInvalidRequest)
}

func TestCreate_RequiresCommittedTurnBeforeGeneratingOrPersisting(t *testing.T) {
	now := time.Date(2026, 8, 7, 4, 0, 0, 0, time.UTC)
	repo := &fakeRepository{committed: false}
	ids := &fixedIDs{handoffID: "handoff-1", eventID: "event-1"}
	service := mustService(t, repo, fixedClock{now: now}, ids)

	_, err := service.Create(context.Background(), validCreateRequest(now))
	assert.ErrorIs(t, err, ErrUncommittedTurn)
	assert.Equal(t, 0, ids.calls)
	assert.Equal(t, 0, repo.createCalls)
}

func TestCreate_GeneratesIDsAndTimeAndDoesNotRetainInput(t *testing.T) {
	now := time.Date(2026, 8, 7, 4, 5, 6, 7, time.FixedZone("CST", 8*60*60))
	repo := &fakeRepository{committed: true}
	repo.createFn = func(record CreateRecord) (Snapshot, bool, error) {
		repo.record = cloneCreateRecord(record)
		return Snapshot{ID: record.HandoffID, Request: record.Request, RequestHash: record.RequestHash, State: StatePending, CreatedAt: record.CreatedAt, UpdatedAt: record.CreatedAt}, true, nil
	}
	ids := &fixedIDs{handoffID: "server-handoff", eventID: "server-event"}
	service := mustService(t, repo, fixedClock{now: now}, ids)
	req := validCreateRequest(now)

	got, err := service.Create(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, identity.HandoffID("server-handoff"), repo.record.HandoffID)
	assert.Equal(t, "server-event", repo.record.EventID)
	assert.Equal(t, now.UTC(), repo.record.CreatedAt)
	assert.Equal(t, 2, ids.calls)

	req.ToolAllowlist[0] = "mutated input"
	req.ApprovalPolicy[0] = '['
	assert.Equal(t, "read", repo.record.Request.ToolAllowlist[0])
	assert.Equal(t, byte('{'), repo.record.Request.ApprovalPolicy[0])
	got.Request.ToolAllowlist[0] = "mutated output"
	got.Request.ApprovalPolicy[0] = '['
	assert.Equal(t, "read", repo.record.Request.ToolAllowlist[0])
	assert.Equal(t, byte('{'), repo.record.Request.ApprovalPolicy[0])
}

func TestCreate_BlankGeneratedIDsFailBeforeRepositoryMutation(t *testing.T) {
	now := time.Date(2026, 8, 7, 4, 0, 0, 0, time.UTC)
	for _, ids := range []*fixedIDs{{handoffID: "", eventID: "event"}, {handoffID: "handoff", eventID: ""}} {
		repo := &fakeRepository{committed: true}
		service := mustService(t, repo, fixedClock{now: now}, ids)
		_, err := service.Create(context.Background(), validCreateRequest(now))
		assert.ErrorIs(t, err, ErrInvalidRequest)
		assert.Zero(t, repo.createCalls)
	}
}

func TestCreate_ReplayAndIdempotencyConflict(t *testing.T) {
	now := time.Date(2026, 8, 7, 4, 0, 0, 0, time.UTC)
	req := validCreateRequest(now)
	hash, err := CanonicalRequestHash(req)
	require.NoError(t, err)

	for _, tc := range []struct {
		name string
		hash string
		want error
	}{
		{"same request", hash, nil},
		{"different request", "different", ErrIdempotencyConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &fakeRepository{committed: true}
			repo.createFn = func(record CreateRecord) (Snapshot, bool, error) {
				return Snapshot{ID: "existing", Request: record.Request, RequestHash: tc.hash, State: StateRunning}, false, nil
			}
			service := mustService(t, repo, fixedClock{now: now}, &fixedIDs{handoffID: "new", eventID: "event"})
			got, err := service.Create(context.Background(), req)
			if tc.want != nil {
				assert.ErrorIs(t, err, tc.want)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, identity.HandoffID("existing"), got.ID)
		})
	}
}

func TestCreate_RejectsBlankRepositorySnapshotID(t *testing.T) {
	now := time.Date(2026, 8, 7, 4, 0, 0, 0, time.UTC)
	req := validCreateRequest(now)
	hash, err := CanonicalRequestHash(req)
	require.NoError(t, err)
	repo := &fakeRepository{committed: true}
	repo.createFn = func(record CreateRecord) (Snapshot, bool, error) {
		return Snapshot{Request: record.Request, RequestHash: hash, State: StateRunning}, false, nil
	}
	service := mustService(t, repo, fixedClock{now: now}, &fixedIDs{handoffID: "new", eventID: "event"})

	_, err = service.Create(context.Background(), req)
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestService_ValidatesBeforeRepositoryIO(t *testing.T) {
	now := time.Date(2026, 8, 7, 4, 0, 0, 0, time.UTC)
	repo := &fakeRepository{committed: true}
	service := mustService(t, repo, fixedClock{now: now}, &fixedIDs{handoffID: "h", eventID: "e"})
	badScope := Scope{}

	_, err := service.Get(context.Background(), badScope, "h")
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = service.ListActive(context.Background(), badScope)
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = service.Steer(context.Background(), badScope, "h", SteerRequest{})
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = service.ProvideInput(context.Background(), badScope, "h", InputRequest{})
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = service.Cancel(context.Background(), badScope, "h", CancelRequest{})
	assert.ErrorIs(t, err, ErrInvalidRequest)
	assert.Zero(t, repo.ioCalls)

	scope := validCreateRequest(now).Scope
	_, err = service.Get(context.Background(), scope, "")
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = service.Steer(context.Background(), scope, "h", SteerRequest{Objective: "", ExpectedRevision: 1, Actor: "a", IdempotencyKey: "i"})
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = service.ProvideInput(context.Background(), scope, "h", InputRequest{Input: json.RawMessage(`{}`), ExpectedRevision: 0, Actor: "a", IdempotencyKey: "i"})
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = service.Cancel(context.Background(), scope, "h", CancelRequest{Actor: "", Reason: "reason", IdempotencyKey: "i"})
	assert.ErrorIs(t, err, ErrInvalidRequest)
	assert.Zero(t, repo.ioCalls)
}

func TestService_RejectsEveryInvalidScopeFieldBeforeRepositoryIO(t *testing.T) {
	now := time.Date(2026, 8, 7, 4, 0, 0, 0, time.UTC)
	validScope := validCreateRequest(now).Scope
	tests := []struct {
		name   string
		mutate func(*Scope)
	}{
		{"tenant", func(scope *Scope) { scope.TenantID = "" }},
		{"user", func(scope *Scope) { scope.UserID = "" }},
		{"conversation", func(scope *Scope) { scope.ConversationID = "" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scope := validScope
			tc.mutate(&scope)
			repo := &fakeRepository{}
			service := mustService(t, repo, fixedClock{now: now}, &fixedIDs{})
			_, err := service.Get(context.Background(), scope, "h")
			assert.ErrorIs(t, err, ErrInvalidRequest)
			assert.Zero(t, repo.ioCalls)
		})
	}
}

func TestService_RejectsEveryInvalidControlGuardBeforeRepositoryIO(t *testing.T) {
	now := time.Date(2026, 8, 7, 4, 0, 0, 0, time.UTC)
	scope := validCreateRequest(now).Scope
	deep := json.RawMessage(strings.Repeat("[", 65) + strings.Repeat("]", 65))
	invalidUTF8 := json.RawMessage{'"', 0xff, '"'}
	rawOverLimit := json.RawMessage(`"` + strings.Repeat("x", 64*1024-1) + `"`)
	canonicalOverLimit := json.RawMessage(`"` + strings.Repeat("<", 10000) + strings.Repeat("x", 5535) + `"`)

	tests := []struct {
		name   string
		invoke func(Service) error
	}{
		{"steer/overlong handoff id", func(service Service) error {
			_, err := service.Steer(context.Background(), scope, identity.HandoffID(strings.Repeat("h", 257)), validSteerRequest())
			return err
		}},
		{"steer/blank objective", func(service Service) error {
			request := validSteerRequest()
			request.Objective = " "
			_, err := service.Steer(context.Background(), scope, "h", request)
			return err
		}},
		{"steer/objective limit plus one", func(service Service) error {
			request := validSteerRequest()
			request.Objective = strings.Repeat("o", 8*1024+1)
			_, err := service.Steer(context.Background(), scope, "h", request)
			return err
		}},
		{"steer/zero revision", func(service Service) error {
			request := validSteerRequest()
			request.ExpectedRevision = 0
			_, err := service.Steer(context.Background(), scope, "h", request)
			return err
		}},
		{"steer/blank actor", func(service Service) error {
			request := validSteerRequest()
			request.Actor = ""
			_, err := service.Steer(context.Background(), scope, "h", request)
			return err
		}},
		{"steer/actor limit plus one", func(service Service) error {
			request := validSteerRequest()
			request.Actor = strings.Repeat("a", 257)
			_, err := service.Steer(context.Background(), scope, "h", request)
			return err
		}},
		{"steer/blank idempotency", func(service Service) error {
			request := validSteerRequest()
			request.IdempotencyKey = "\t"
			_, err := service.Steer(context.Background(), scope, "h", request)
			return err
		}},
		{"steer/idempotency limit plus one", func(service Service) error {
			request := validSteerRequest()
			request.IdempotencyKey = strings.Repeat("i", 257)
			_, err := service.Steer(context.Background(), scope, "h", request)
			return err
		}},
		{"input/overlong handoff id", func(service Service) error {
			_, err := service.ProvideInput(context.Background(), scope, identity.HandoffID(strings.Repeat("h", 257)), validInputRequest())
			return err
		}},
		{"input/zero revision", func(service Service) error {
			request := validInputRequest()
			request.ExpectedRevision = 0
			_, err := service.ProvideInput(context.Background(), scope, "h", request)
			return err
		}},
		{"input/blank actor", func(service Service) error {
			request := validInputRequest()
			request.Actor = " "
			_, err := service.ProvideInput(context.Background(), scope, "h", request)
			return err
		}},
		{"input/actor limit plus one", func(service Service) error {
			request := validInputRequest()
			request.Actor = strings.Repeat("a", 257)
			_, err := service.ProvideInput(context.Background(), scope, "h", request)
			return err
		}},
		{"input/blank idempotency", func(service Service) error {
			request := validInputRequest()
			request.IdempotencyKey = ""
			_, err := service.ProvideInput(context.Background(), scope, "h", request)
			return err
		}},
		{"input/idempotency limit plus one", func(service Service) error {
			request := validInputRequest()
			request.IdempotencyKey = strings.Repeat("i", 257)
			_, err := service.ProvideInput(context.Background(), scope, "h", request)
			return err
		}},
		{"input/malformed", invalidInputInvocation(scope, json.RawMessage(`{"x":`))},
		{"input/empty", invalidInputInvocation(scope, nil)},
		{"input/trailing", invalidInputInvocation(scope, json.RawMessage(`{} {}`))},
		{"input/duplicate", invalidInputInvocation(scope, json.RawMessage(`{"x":1,"x":2}`))},
		{"input/depth 65", invalidInputInvocation(scope, deep)},
		{"input/invalid utf8", invalidInputInvocation(scope, invalidUTF8)},
		{"input/raw limit plus one", invalidInputInvocation(scope, rawOverLimit)},
		{"input/canonical limit plus one", invalidInputInvocation(scope, canonicalOverLimit)},
		{"cancel/overlong handoff id", func(service Service) error {
			_, err := service.Cancel(context.Background(), scope, identity.HandoffID(strings.Repeat("h", 257)), validCancelRequest())
			return err
		}},
		{"cancel/blank actor", func(service Service) error {
			request := validCancelRequest()
			request.Actor = ""
			_, err := service.Cancel(context.Background(), scope, "h", request)
			return err
		}},
		{"cancel/actor limit plus one", func(service Service) error {
			request := validCancelRequest()
			request.Actor = strings.Repeat("a", 257)
			_, err := service.Cancel(context.Background(), scope, "h", request)
			return err
		}},
		{"cancel/blank idempotency", func(service Service) error {
			request := validCancelRequest()
			request.IdempotencyKey = " "
			_, err := service.Cancel(context.Background(), scope, "h", request)
			return err
		}},
		{"cancel/idempotency limit plus one", func(service Service) error {
			request := validCancelRequest()
			request.IdempotencyKey = strings.Repeat("i", 257)
			_, err := service.Cancel(context.Background(), scope, "h", request)
			return err
		}},
		{"cancel/reason limit plus one", func(service Service) error {
			request := validCancelRequest()
			request.Reason = strings.Repeat("r", 2*1024+1)
			_, err := service.Cancel(context.Background(), scope, "h", request)
			return err
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo := &fakeRepository{}
			service := mustService(t, repo, fixedClock{now: now}, &fixedIDs{})
			err := tc.invoke(service)
			assert.ErrorIs(t, err, ErrInvalidRequest)
			assert.Zero(t, repo.ioCalls)
		})
	}
}

func TestService_AcceptsControlFieldsAtEveryByteLimit(t *testing.T) {
	now := time.Date(2026, 8, 7, 4, 0, 0, 0, time.UTC)
	scope := validCreateRequest(now).Scope
	handoffID := identity.HandoffID(strings.Repeat("h", 256))
	baseSnapshot := repositoryOwnedSnapshot(now)
	baseSnapshot.ID = handoffID

	t.Run("steer", func(t *testing.T) {
		repo := &fakeRepository{getSnapshot: baseSnapshot}
		service := mustService(t, repo, fixedClock{now: now}, &fixedIDs{})
		_, err := service.Steer(context.Background(), scope, handoffID, SteerRequest{Objective: strings.Repeat("o", 8*1024), ExpectedRevision: 1, Actor: strings.Repeat("a", 256), IdempotencyKey: strings.Repeat("i", 256)})
		require.NoError(t, err)
	})
	t.Run("input raw and metadata", func(t *testing.T) {
		repo := &fakeRepository{getSnapshot: baseSnapshot}
		service := mustService(t, repo, fixedClock{now: now}, &fixedIDs{})
		_, err := service.ProvideInput(context.Background(), scope, handoffID, InputRequest{Input: json.RawMessage(`"` + strings.Repeat("x", 64*1024-2) + `"`), ExpectedRevision: 1, Actor: strings.Repeat("a", 256), IdempotencyKey: strings.Repeat("i", 256)})
		require.NoError(t, err)
		assert.Len(t, repo.inputRequest.Input, 64*1024)
	})
	t.Run("input canonical output", func(t *testing.T) {
		repo := &fakeRepository{getSnapshot: baseSnapshot}
		service := mustService(t, repo, fixedClock{now: now}, &fixedIDs{})
		input := json.RawMessage(`"` + strings.Repeat("<", 10000) + strings.Repeat("x", 5534) + `"`)
		_, err := service.ProvideInput(context.Background(), scope, handoffID, InputRequest{Input: input, ExpectedRevision: 1, Actor: "a", IdempotencyKey: "i"})
		require.NoError(t, err)
		assert.Len(t, repo.inputRequest.Input, 64*1024)
	})
	t.Run("cancel", func(t *testing.T) {
		repo := &fakeRepository{getSnapshot: baseSnapshot}
		service := mustService(t, repo, fixedClock{now: now}, &fixedIDs{})
		_, err := service.Cancel(context.Background(), scope, handoffID, CancelRequest{Actor: strings.Repeat("a", 256), Reason: strings.Repeat("r", 2*1024), IdempotencyKey: strings.Repeat("i", 256)})
		require.NoError(t, err)
	})
}

func TestService_DefensivelyRejectsCrossScopeRepositoryOutput(t *testing.T) {
	now := time.Date(2026, 8, 7, 4, 0, 0, 0, time.UTC)
	scope := validCreateRequest(now).Scope
	valid := Snapshot{ID: "h", Request: validCreateRequest(now), State: StateRunning}

	tests := []struct {
		name   string
		mutate func(*Snapshot)
	}{
		{"tenant", func(s *Snapshot) { s.Request.Scope.TenantID = "other" }},
		{"user", func(s *Snapshot) { s.Request.Scope.UserID = "other" }},
		{"conversation", func(s *Snapshot) { s.Request.Scope.ConversationID = "other" }},
		{"handoff id", func(s *Snapshot) { s.ID = "other" }},
	}
	for _, tc := range tests {
		t.Run("get "+tc.name, func(t *testing.T) {
			snapshot := CloneSnapshot(valid)
			tc.mutate(&snapshot)
			repo := &fakeRepository{getSnapshot: snapshot}
			service := mustService(t, repo, fixedClock{now: now}, &fixedIDs{})
			_, err := service.Get(context.Background(), scope, "h")
			assert.ErrorIs(t, err, ErrNotFound)
		})
	}

	listMismatches := []struct {
		name   string
		mutate func(*Snapshot)
	}{
		{"tenant", func(snapshot *Snapshot) { snapshot.Request.Scope.TenantID = "other" }},
		{"user", func(snapshot *Snapshot) { snapshot.Request.Scope.UserID = "other" }},
		{"conversation", func(snapshot *Snapshot) { snapshot.Request.Scope.ConversationID = "other" }},
		{"blank id", func(snapshot *Snapshot) { snapshot.ID = "" }},
	}
	for _, tc := range listMismatches {
		t.Run("list "+tc.name, func(t *testing.T) {
			bad := CloneSnapshot(valid)
			tc.mutate(&bad)
			repo := &fakeRepository{listSnapshots: []Snapshot{valid, bad}}
			service := mustService(t, repo, fixedClock{now: now}, &fixedIDs{})
			got, err := service.ListActive(context.Background(), scope)
			assert.ErrorIs(t, err, ErrNotFound)
			assert.Nil(t, got)
		})
	}
}

func TestService_PreservesRepositoryErrorIdentityAndOutputIsolation(t *testing.T) {
	now := time.Date(2026, 8, 7, 4, 0, 0, 0, time.UTC)
	scope := validCreateRequest(now).Scope
	sentinel := errors.New("repository unavailable")
	repo := &fakeRepository{getErr: sentinel}
	service := mustService(t, repo, fixedClock{now: now}, &fixedIDs{})
	_, err := service.Get(context.Background(), scope, "h")
	assert.ErrorIs(t, err, sentinel)

	snapshot := Snapshot{ID: "h", Request: validCreateRequest(now), State: StateRunning, Result: &Result{Structured: json.RawMessage(`{"x":1}`)}}
	repo.getErr = nil
	repo.getSnapshot = snapshot
	first, err := service.Get(context.Background(), scope, "h")
	require.NoError(t, err)
	first.Request.ToolAllowlist[0] = "changed"
	first.Result.Structured[0] = '['
	second, err := service.Get(context.Background(), scope, "h")
	require.NoError(t, err)
	assert.Equal(t, "read", second.Request.ToolAllowlist[0])
	assert.Equal(t, byte('{'), second.Result.Structured[0])
}

func TestService_ControlOperationsHappyPathsAndRepositoryErrors(t *testing.T) {
	now := time.Date(2026, 8, 7, 4, 0, 0, 0, time.UTC)
	scope := validCreateRequest(now).Scope
	snapshot := Snapshot{ID: "h", Request: validCreateRequest(now), State: StateRunning}
	sentinel := errors.New("control repository error")
	tests := []struct {
		name   string
		invoke func(Service) (Snapshot, error)
	}{
		{"steer", func(service Service) (Snapshot, error) {
			return service.Steer(context.Background(), scope, "h", SteerRequest{Objective: "new objective", ExpectedRevision: 1, Actor: "user", IdempotencyKey: "steer-1"})
		}},
		{"provide input", func(service Service) (Snapshot, error) {
			return service.ProvideInput(context.Background(), scope, "h", InputRequest{Input: json.RawMessage(`{"answer":true}`), ExpectedRevision: 1, Actor: "user", IdempotencyKey: "input-1"})
		}},
		{"cancel", func(service Service) (Snapshot, error) {
			return service.Cancel(context.Background(), scope, "h", CancelRequest{Actor: "user", Reason: "stop", IdempotencyKey: "cancel-1"})
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name+" success", func(t *testing.T) {
			repo := &fakeRepository{getSnapshot: snapshot}
			service := mustService(t, repo, fixedClock{now: now}, &fixedIDs{})
			got, err := tc.invoke(service)
			require.NoError(t, err)
			assert.Equal(t, identity.HandoffID("h"), got.ID)
		})
		t.Run(tc.name+" repository error", func(t *testing.T) {
			repo := &fakeRepository{getSnapshot: snapshot, getErr: sentinel}
			service := mustService(t, repo, fixedClock{now: now}, &fixedIDs{})
			_, err := tc.invoke(service)
			assert.ErrorIs(t, err, sentinel)
		})
	}
}

func TestService_ControlOperationsRejectEveryMismatchedRepositoryOutput(t *testing.T) {
	now := time.Date(2026, 8, 7, 4, 0, 0, 0, time.UTC)
	scope := validCreateRequest(now).Scope
	valid := Snapshot{ID: "h", Request: validCreateRequest(now), State: StateRunning}
	operations := []struct {
		name   string
		invoke func(Service) (Snapshot, error)
	}{
		{"steer", func(service Service) (Snapshot, error) {
			return service.Steer(context.Background(), scope, "h", SteerRequest{Objective: "new objective", ExpectedRevision: 1, Actor: "user", IdempotencyKey: "steer-1"})
		}},
		{"provide input", func(service Service) (Snapshot, error) {
			return service.ProvideInput(context.Background(), scope, "h", InputRequest{Input: json.RawMessage(`{"answer":true}`), ExpectedRevision: 1, Actor: "user", IdempotencyKey: "input-1"})
		}},
		{"cancel", func(service Service) (Snapshot, error) {
			return service.Cancel(context.Background(), scope, "h", CancelRequest{Actor: "user", Reason: "stop", IdempotencyKey: "cancel-1"})
		}},
	}
	mismatches := []struct {
		name   string
		mutate func(*Snapshot)
	}{
		{"tenant", func(snapshot *Snapshot) { snapshot.Request.Scope.TenantID = "other" }},
		{"user", func(snapshot *Snapshot) { snapshot.Request.Scope.UserID = "other" }},
		{"conversation", func(snapshot *Snapshot) { snapshot.Request.Scope.ConversationID = "other" }},
		{"handoff id", func(snapshot *Snapshot) { snapshot.ID = "other" }},
	}
	for _, operation := range operations {
		for _, mismatch := range mismatches {
			t.Run(operation.name+"/"+mismatch.name, func(t *testing.T) {
				snapshot := CloneSnapshot(valid)
				mismatch.mutate(&snapshot)
				repo := &fakeRepository{getSnapshot: snapshot}
				service := mustService(t, repo, fixedClock{now: now}, &fixedIDs{})
				_, err := operation.invoke(service)
				assert.ErrorIs(t, err, ErrNotFound)
			})
		}
	}
}

func TestProvideInput_CanonicalizesPrivateRepositoryHandoff(t *testing.T) {
	now := time.Date(2026, 8, 7, 4, 0, 0, 0, time.UTC)
	scope := validCreateRequest(now).Scope
	repo := &fakeRepository{getSnapshot: Snapshot{ID: "h", Request: validCreateRequest(now), State: StateRunning}}
	service := mustService(t, repo, fixedClock{now: now}, &fixedIDs{})
	request := InputRequest{Input: json.RawMessage(`{ "b": 2.0, "a": 1 }`), ExpectedRevision: 1, Actor: "user", IdempotencyKey: "input-1"}

	_, err := service.ProvideInput(context.Background(), scope, "h", request)
	require.NoError(t, err)
	assert.Equal(t, json.RawMessage(`{"a":1,"b":2}`), repo.inputRequest.Input)
	request.Input[2] = 'x'
	assert.Equal(t, byte('{'), repo.inputRequest.Input[0])
}

func TestService_ClonesEveryRepositoryOwnedMutableOutput(t *testing.T) {
	now := time.Date(2026, 8, 7, 4, 0, 0, 0, time.UTC)
	scope := validCreateRequest(now).Scope
	operations := []struct {
		name   string
		invoke func(Service) (Snapshot, error)
	}{
		{"get", func(service Service) (Snapshot, error) { return service.Get(context.Background(), scope, "h") }},
		{"steer", func(service Service) (Snapshot, error) {
			return service.Steer(context.Background(), scope, "h", validSteerRequest())
		}},
		{"provide input", func(service Service) (Snapshot, error) {
			return service.ProvideInput(context.Background(), scope, "h", validInputRequest())
		}},
		{"cancel", func(service Service) (Snapshot, error) {
			return service.Cancel(context.Background(), scope, "h", validCancelRequest())
		}},
	}
	for _, tc := range operations {
		t.Run(tc.name, func(t *testing.T) {
			repo := &fakeRepository{getSnapshot: repositoryOwnedSnapshot(now)}
			service := mustService(t, repo, fixedClock{now: now}, &fixedIDs{})
			got, err := tc.invoke(service)
			require.NoError(t, err)
			mutateEverySnapshotReference(got)
			assertRepositorySnapshotUnchanged(t, repo.getSnapshot)
		})
	}

	t.Run("list active", func(t *testing.T) {
		repo := &fakeRepository{listSnapshots: []Snapshot{repositoryOwnedSnapshot(now)}}
		service := mustService(t, repo, fixedClock{now: now}, &fixedIDs{})
		got, err := service.ListActive(context.Background(), scope)
		require.NoError(t, err)
		require.Len(t, got, 1)
		mutateEverySnapshotReference(got[0])
		got[0].ID = "caller-id"
		got[0].State = StateFailed
		assertRepositorySnapshotUnchanged(t, repo.listSnapshots[0])
		assert.Equal(t, identity.HandoffID("h"), repo.listSnapshots[0].ID)
		assert.Equal(t, StateRunning, repo.listSnapshots[0].State)
	})
}

func mustService(t *testing.T, repo Repository, clock Clock, ids IDGenerator) Service {
	t.Helper()
	service, err := NewService(repo, clock, ids)
	require.NoError(t, err)
	return service
}

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

type fixedIDs struct {
	handoffID identity.HandoffID
	eventID   string
	calls     int
}

func (g *fixedIDs) NewHandoffID() identity.HandoffID { g.calls++; return g.handoffID }
func (g *fixedIDs) NewEventID() string               { g.calls++; return g.eventID }

type fakeRepository struct {
	committed     bool
	committedErr  error
	createFn      func(CreateRecord) (Snapshot, bool, error)
	record        CreateRecord
	createCalls   int
	ioCalls       int
	getSnapshot   Snapshot
	getErr        error
	listSnapshots []Snapshot
	inputRequest  InputRequest
}

func (r *fakeRepository) IsCommittedTurn(context.Context, Scope, identity.TurnKey) (bool, error) {
	return r.committed, r.committedErr
}
func (r *fakeRepository) CreateHandoff(_ context.Context, record CreateRecord) (Snapshot, bool, error) {
	r.createCalls++
	if r.createFn != nil {
		return r.createFn(record)
	}
	return Snapshot{}, false, nil
}
func (r *fakeRepository) GetHandoff(context.Context, Scope, identity.HandoffID) (Snapshot, error) {
	r.ioCalls++
	return r.getSnapshot, r.getErr
}
func (r *fakeRepository) ListActive(context.Context, Scope) ([]Snapshot, error) {
	r.ioCalls++
	return r.listSnapshots, nil
}
func (r *fakeRepository) SteerHandoff(context.Context, Scope, identity.HandoffID, SteerRequest) (Snapshot, error) {
	r.ioCalls++
	return r.getSnapshot, r.getErr
}
func (r *fakeRepository) ProvideHandoffInput(_ context.Context, _ Scope, _ identity.HandoffID, request InputRequest) (Snapshot, error) {
	r.ioCalls++
	r.inputRequest = request
	return r.getSnapshot, r.getErr
}
func (r *fakeRepository) RequestHandoffCancel(context.Context, Scope, identity.HandoffID, CancelRequest) (Snapshot, error) {
	r.ioCalls++
	return r.getSnapshot, r.getErr
}

func cloneCreateRecord(record CreateRecord) CreateRecord {
	record.Request = cloneCreateRequest(record.Request)
	return record
}

func validSteerRequest() SteerRequest {
	return SteerRequest{Objective: "new objective", ExpectedRevision: 1, Actor: "user", IdempotencyKey: "steer-1"}
}

func validInputRequest() InputRequest {
	return InputRequest{Input: json.RawMessage(`{"answer":true}`), ExpectedRevision: 1, Actor: "user", IdempotencyKey: "input-1"}
}

func validCancelRequest() CancelRequest {
	return CancelRequest{Actor: "user", Reason: "stop", IdempotencyKey: "cancel-1"}
}

func invalidInputInvocation(scope Scope, input json.RawMessage) func(Service) error {
	return func(service Service) error {
		request := validInputRequest()
		request.Input = input
		_, err := service.ProvideInput(context.Background(), scope, "h", request)
		return err
	}
}

func repositoryOwnedSnapshot(now time.Time) Snapshot {
	return Snapshot{
		ID: "h", Request: validCreateRequest(now), State: StateRunning,
		Result:  &Result{MutationID: "result-1", Summary: "done", Structured: json.RawMessage(`{"result":true}`), Usage: json.RawMessage(`{"tokens":2}`)},
		Failure: &Failure{MutationID: "failure-1", Code: "failed", Message: "repository message"},
	}
}

func mutateEverySnapshotReference(snapshot Snapshot) {
	snapshot.Request.ToolAllowlist[0] = "caller tool"
	snapshot.Request.ApprovalPolicy[0] = '['
	snapshot.Result.Structured[0] = '['
	snapshot.Result.Usage[0] = '['
	snapshot.Result.Summary = "caller summary"
	snapshot.Failure.Message = "caller message"
}

func assertRepositorySnapshotUnchanged(t *testing.T, snapshot Snapshot) {
	t.Helper()
	assert.Equal(t, "read", snapshot.Request.ToolAllowlist[0])
	assert.Equal(t, byte('{'), snapshot.Request.ApprovalPolicy[0])
	assert.Equal(t, byte('{'), snapshot.Result.Structured[0])
	assert.Equal(t, byte('{'), snapshot.Result.Usage[0])
	assert.Equal(t, "done", snapshot.Result.Summary)
	assert.Equal(t, "repository message", snapshot.Failure.Message)
}
