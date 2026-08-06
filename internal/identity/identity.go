// Package identity defines the stable identifiers carried across GPT-Live
// boundaries. They are distinct named types to prevent accidental mixing.
package identity

type SessionID string
type ConversationID string
type HandoffID string
type Incarnation uint64
type ResponseEpoch uint64

type TurnKey struct {
	ID       string
	Revision uint32
}

type Metadata struct {
	SessionID      SessionID
	ConversationID ConversationID
	Incarnation    Incarnation
	ResponseEpoch  ResponseEpoch
	Turn           *TurnKey
	Sequence       uint64
}
