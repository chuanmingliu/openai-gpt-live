package protocol

import (
	"encoding/json"
	"time"
)

type ServerEvent interface {
	EventType() string
	isServerEvent()
}

type SessionCreatedEvent struct {
	Type      string             `json:"type"`
	EventID   string             `json:"event_id,omitempty"`
	Session   json.RawMessage    `json:"session"`
	Reconnect *ReconnectMetadata `json:"reconnect,omitempty"`
}

type ReconnectMetadata struct {
	Credential     string    `json:"credential"`
	SessionID      string    `json:"session_id"`
	ConversationID string    `json:"conversation_id"`
	Incarnation    uint64    `json:"incarnation"`
	Cursor         int64     `json:"cursor"`
	Model          string    `json:"model"`
	ExpiresAt      time.Time `json:"expires_at"`
}

func (e SessionCreatedEvent) EventType() string { return e.Type }
func (SessionCreatedEvent) isServerEvent()      {}

// SessionReconnectUpdatedEvent replaces reconnect metadata after a bounded
// credential rotation. The prior credential remains accepted only for the
// server-controlled overlap window.
type SessionReconnectUpdatedEvent struct {
	Type      string            `json:"type"`
	EventID   string            `json:"event_id"`
	Reconnect ReconnectMetadata `json:"reconnect"`
}

func (e SessionReconnectUpdatedEvent) EventType() string { return e.Type }
func (SessionReconnectUpdatedEvent) isServerEvent()      {}

type SessionUpdatedEvent struct {
	Type    string          `json:"type"`
	EventID string          `json:"event_id,omitempty"`
	Session json.RawMessage `json:"session"`
}

func (e SessionUpdatedEvent) EventType() string { return e.Type }
func (SessionUpdatedEvent) isServerEvent()      {}

type ProtocolError struct {
	Type    string `json:"type"`
	Code    string `json:"code,omitempty"`
	Message string `json:"message"`
	Param   string `json:"param,omitempty"`
	EventID string `json:"event_id,omitempty"`
}

type ErrorEvent struct {
	Type    string        `json:"type"`
	EventID string        `json:"event_id,omitempty"`
	Error   ProtocolError `json:"error"`
}

func (e ErrorEvent) EventType() string { return e.Type }
func (ErrorEvent) isServerEvent()      {}

type InputAudioBufferSpeechStartedEvent struct {
	Type         string `json:"type"`
	EventID      string `json:"event_id,omitempty"`
	AudioStartMS int    `json:"audio_start_ms"`
	ItemID       string `json:"item_id"`
}

func (e InputAudioBufferSpeechStartedEvent) EventType() string { return e.Type }
func (InputAudioBufferSpeechStartedEvent) isServerEvent()      {}

type InputAudioBufferSpeechStoppedEvent struct {
	Type       string `json:"type"`
	EventID    string `json:"event_id,omitempty"`
	AudioEndMS int    `json:"audio_end_ms"`
	ItemID     string `json:"item_id"`
}

func (e InputAudioBufferSpeechStoppedEvent) EventType() string { return e.Type }
func (InputAudioBufferSpeechStoppedEvent) isServerEvent()      {}

type ConversationItemInputAudioTranscriptionDeltaEvent struct {
	Type         string `json:"type"`
	EventID      string `json:"event_id,omitempty"`
	ItemID       string `json:"item_id"`
	ContentIndex int    `json:"content_index"`
	Delta        string `json:"delta"`
}

func (e ConversationItemInputAudioTranscriptionDeltaEvent) EventType() string { return e.Type }
func (ConversationItemInputAudioTranscriptionDeltaEvent) isServerEvent()      {}

type ConversationItemInputAudioTranscriptionCompletedEvent struct {
	Type         string `json:"type"`
	EventID      string `json:"event_id,omitempty"`
	ItemID       string `json:"item_id"`
	ContentIndex int    `json:"content_index"`
	Transcript   string `json:"transcript"`
}

func (e ConversationItemInputAudioTranscriptionCompletedEvent) EventType() string { return e.Type }
func (ConversationItemInputAudioTranscriptionCompletedEvent) isServerEvent()      {}

type ResponseCreatedEvent struct {
	Type     string          `json:"type"`
	EventID  string          `json:"event_id,omitempty"`
	Response ResponsePayload `json:"response"`
}

func (e ResponseCreatedEvent) EventType() string { return e.Type }
func (ResponseCreatedEvent) isServerEvent()      {}

type ResponseOutputAudioDeltaEvent struct {
	Type         string `json:"type"`
	EventID      string `json:"event_id,omitempty"`
	ResponseID   string `json:"response_id,omitempty"`
	ItemID       string `json:"item_id"`
	OutputIndex  int    `json:"output_index"`
	ContentIndex int    `json:"content_index"`
	Delta        string `json:"delta"`
}

func (e ResponseOutputAudioDeltaEvent) EventType() string { return e.Type }
func (ResponseOutputAudioDeltaEvent) isServerEvent()      {}

type ResponseOutputAudioDoneEvent struct {
	Type         string `json:"type"`
	EventID      string `json:"event_id,omitempty"`
	ResponseID   string `json:"response_id,omitempty"`
	ItemID       string `json:"item_id"`
	OutputIndex  int    `json:"output_index"`
	ContentIndex int    `json:"content_index"`
}

func (e ResponseOutputAudioDoneEvent) EventType() string { return e.Type }
func (ResponseOutputAudioDoneEvent) isServerEvent()      {}

type ResponseOutputAudioTranscriptDeltaEvent struct {
	Type         string `json:"type"`
	EventID      string `json:"event_id,omitempty"`
	ResponseID   string `json:"response_id,omitempty"`
	ItemID       string `json:"item_id"`
	OutputIndex  int    `json:"output_index"`
	ContentIndex int    `json:"content_index"`
	Delta        string `json:"delta"`
}

func (e ResponseOutputAudioTranscriptDeltaEvent) EventType() string { return e.Type }
func (ResponseOutputAudioTranscriptDeltaEvent) isServerEvent()      {}

type ResponseOutputAudioTranscriptDoneEvent struct {
	Type         string `json:"type"`
	EventID      string `json:"event_id,omitempty"`
	ResponseID   string `json:"response_id,omitempty"`
	ItemID       string `json:"item_id"`
	OutputIndex  int    `json:"output_index"`
	ContentIndex int    `json:"content_index"`
	Transcript   string `json:"transcript"`
}

func (e ResponseOutputAudioTranscriptDoneEvent) EventType() string { return e.Type }
func (ResponseOutputAudioTranscriptDoneEvent) isServerEvent()      {}

type ResponseFunctionCallArgumentsDoneEvent struct {
	Type        string `json:"type"`
	EventID     string `json:"event_id,omitempty"`
	ResponseID  string `json:"response_id,omitempty"`
	ItemID      string `json:"item_id"`
	OutputIndex int    `json:"output_index"`
	CallID      string `json:"call_id"`
	Name        string `json:"name"`
	Arguments   string `json:"arguments"`
}

func (e ResponseFunctionCallArgumentsDoneEvent) EventType() string { return e.Type }
func (ResponseFunctionCallArgumentsDoneEvent) isServerEvent()      {}

type ResponseDoneEvent struct {
	Type     string          `json:"type"`
	EventID  string          `json:"event_id,omitempty"`
	Response ResponsePayload `json:"response"`
}

func (e ResponseDoneEvent) EventType() string { return e.Type }
func (ResponseDoneEvent) isServerEvent()      {}

// ResponsePayload exposes the provider-neutral response correlation fields and
// retains the bounded provider payload for lossless forwarding.
type ResponsePayload struct {
	ID     string          `json:"id"`
	Status string          `json:"status"`
	Raw    json.RawMessage `json:"-"`
}

func (p *ResponsePayload) UnmarshalJSON(data []byte) error {
	type core struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	var decoded core
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	p.ID = decoded.ID
	p.Status = decoded.Status
	p.Raw = append(p.Raw[:0], data...)
	return nil
}

func (p ResponsePayload) MarshalJSON() ([]byte, error) {
	type core struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	if len(p.Raw) == 0 {
		return json.Marshal(core{ID: p.ID, Status: p.Status})
	}
	var original core
	if err := json.Unmarshal(p.Raw, &original); err != nil {
		return nil, err
	}
	if original.ID == p.ID && original.Status == p.Status {
		return p.Raw, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(p.Raw, &fields); err != nil {
		return nil, err
	}
	fields["id"], _ = json.Marshal(p.ID)
	fields["status"], _ = json.Marshal(p.Status)
	return json.Marshal(fields)
}

type LiveHandoffCreatedEvent struct {
	Type       string          `json:"type"`
	EventID    string          `json:"event_id,omitempty"`
	SequenceID int64           `json:"sequence_id"`
	HandoffID  string          `json:"handoff_id"`
	Status     string          `json:"status"`
	Details    json.RawMessage `json:"details,omitempty"`
}

type LiveHandoffProgressEvent struct {
	Type       string          `json:"type"`
	EventID    string          `json:"event_id,omitempty"`
	SequenceID int64           `json:"sequence_id"`
	HandoffID  string          `json:"handoff_id"`
	Status     string          `json:"status"`
	Details    json.RawMessage `json:"details,omitempty"`
}

type LiveHandoffNeedsInputEvent struct {
	Type       string          `json:"type"`
	EventID    string          `json:"event_id,omitempty"`
	SequenceID int64           `json:"sequence_id"`
	HandoffID  string          `json:"handoff_id"`
	Status     string          `json:"status"`
	Details    json.RawMessage `json:"details,omitempty"`
}

type LiveHandoffCompletedEvent struct {
	Type       string          `json:"type"`
	EventID    string          `json:"event_id,omitempty"`
	SequenceID int64           `json:"sequence_id"`
	HandoffID  string          `json:"handoff_id"`
	Status     string          `json:"status"`
	Details    json.RawMessage `json:"details,omitempty"`
}

type LiveHandoffFailedEvent struct {
	Type       string          `json:"type"`
	EventID    string          `json:"event_id,omitempty"`
	SequenceID int64           `json:"sequence_id"`
	HandoffID  string          `json:"handoff_id"`
	Status     string          `json:"status"`
	Details    json.RawMessage `json:"details,omitempty"`
}

type LiveHandoffCancelledEvent struct {
	Type       string          `json:"type"`
	EventID    string          `json:"event_id,omitempty"`
	SequenceID int64           `json:"sequence_id"`
	HandoffID  string          `json:"handoff_id"`
	Status     string          `json:"status"`
	Details    json.RawMessage `json:"details,omitempty"`
}

func (e LiveHandoffCreatedEvent) EventType() string    { return e.Type }
func (e LiveHandoffProgressEvent) EventType() string   { return e.Type }
func (e LiveHandoffNeedsInputEvent) EventType() string { return e.Type }
func (e LiveHandoffCompletedEvent) EventType() string  { return e.Type }
func (e LiveHandoffFailedEvent) EventType() string     { return e.Type }
func (e LiveHandoffCancelledEvent) EventType() string  { return e.Type }
func (LiveHandoffCreatedEvent) isServerEvent()         {}
func (LiveHandoffProgressEvent) isServerEvent()        {}
func (LiveHandoffNeedsInputEvent) isServerEvent()      {}
func (LiveHandoffCompletedEvent) isServerEvent()       {}
func (LiveHandoffFailedEvent) isServerEvent()          {}
func (LiveHandoffCancelledEvent) isServerEvent()       {}

type LiveToolApprovalRequiredEvent struct {
	Type          string    `json:"type"`
	EventID       string    `json:"event_id"`
	SequenceID    int64     `json:"sequence_id"`
	InvocationID  string    `json:"invocation_id"`
	HandoffID     string    `json:"handoff_id"`
	ActionSummary string    `json:"action_summary"`
	ExpiresAt     time.Time `json:"expires_at"`
	RiskClass     string    `json:"risk_class"`
}

func (e LiveToolApprovalRequiredEvent) EventType() string { return e.Type }
func (LiveToolApprovalRequiredEvent) isServerEvent()      {}

type LiveSessionDegradedEvent struct {
	Type       string          `json:"type"`
	EventID    string          `json:"event_id,omitempty"`
	SequenceID int64           `json:"sequence_id"`
	Status     string          `json:"status"`
	Details    json.RawMessage `json:"details,omitempty"`
}

type LiveSessionResumedEvent struct {
	Type       string          `json:"type"`
	EventID    string          `json:"event_id,omitempty"`
	SequenceID int64           `json:"sequence_id"`
	Status     string          `json:"status"`
	Details    json.RawMessage `json:"details,omitempty"`
}

func (e LiveSessionDegradedEvent) EventType() string { return e.Type }
func (e LiveSessionResumedEvent) EventType() string  { return e.Type }
func (LiveSessionDegradedEvent) isServerEvent()      {}
func (LiveSessionResumedEvent) isServerEvent()       {}
