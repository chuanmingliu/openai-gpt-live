package handoff

import (
	"context"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
)

type EventReader interface {
	ReadEvents(context.Context, Scope, int64, int) ([]Event, error)
}

type Repository interface {
	IsCommittedTurn(context.Context, Scope, identity.TurnKey) (bool, error)
	CreateHandoff(context.Context, CreateRecord) (Snapshot, bool, error)
	GetHandoff(context.Context, Scope, identity.HandoffID) (Snapshot, error)
	ListActive(context.Context, Scope) ([]Snapshot, error)
	SteerHandoff(context.Context, Scope, identity.HandoffID, SteerRequest) (Snapshot, error)
	ProvideHandoffInput(context.Context, Scope, identity.HandoffID, InputRequest) (Snapshot, error)
	RequestHandoffCancel(context.Context, Scope, identity.HandoffID, CancelRequest) (Snapshot, error)
}
