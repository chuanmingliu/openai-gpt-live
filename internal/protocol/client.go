package protocol

import "encoding/json"

type ClientEvent interface {
	EventType() string
	isClientEvent()
}

type SessionUpdateEvent struct {
	Type    string        `json:"type"`
	EventID string        `json:"event_id,omitempty"`
	Session SessionUpdate `json:"session"`
}

func (e SessionUpdateEvent) EventType() string { return e.Type }
func (SessionUpdateEvent) isClientEvent()      {}

type SessionUpdate struct {
	Type             string          `json:"type"`
	Instructions     string          `json:"instructions,omitempty"`
	Model            string          `json:"model,omitempty"`
	Audio            *SessionAudio   `json:"audio,omitempty"`
	OutputModalities []string        `json:"output_modalities,omitempty"`
	Tools            json.RawMessage `json:"tools,omitempty"`
	ToolChoice       json.RawMessage `json:"tool_choice,omitempty"`
	Metadata         json.RawMessage `json:"metadata,omitempty"`
}

type SessionAudio struct {
	Input  *SessionAudioInput  `json:"input,omitempty"`
	Output *SessionAudioOutput `json:"output,omitempty"`
}

type SessionAudioInput struct {
	Format         *AudioFormat    `json:"format,omitempty"`
	Transcription  json.RawMessage `json:"transcription,omitempty"`
	TurnDetection  json.RawMessage `json:"turn_detection,omitempty"`
	NoiseReduction json.RawMessage `json:"noise_reduction,omitempty"`
}

type SessionAudioOutput struct {
	Format *AudioFormat `json:"format,omitempty"`
	Voice  string       `json:"voice,omitempty"`
	Speed  float64      `json:"speed,omitempty"`
}

type AudioFormat struct {
	Type string `json:"type"`
	Rate int    `json:"rate,omitempty"`
}

type InputAudioBufferAppendEvent struct {
	Type    string `json:"type"`
	EventID string `json:"event_id,omitempty"`
	Audio   string `json:"audio"`
}

func (e InputAudioBufferAppendEvent) EventType() string { return e.Type }
func (InputAudioBufferAppendEvent) isClientEvent()      {}

type InputAudioBufferCommitEvent struct {
	Type    string `json:"type"`
	EventID string `json:"event_id,omitempty"`
}

func (e InputAudioBufferCommitEvent) EventType() string { return e.Type }
func (InputAudioBufferCommitEvent) isClientEvent()      {}

type ConversationItemCreateEvent struct {
	Type           string           `json:"type"`
	EventID        string           `json:"event_id,omitempty"`
	PreviousItemID string           `json:"previous_item_id,omitempty"`
	Item           ConversationItem `json:"item"`
}

func (e ConversationItemCreateEvent) EventType() string { return e.Type }
func (ConversationItemCreateEvent) isClientEvent()      {}

type ConversationItem struct {
	ID        string          `json:"id,omitempty"`
	Type      string          `json:"type"`
	Role      string          `json:"role,omitempty"`
	CallID    string          `json:"call_id,omitempty"`
	Name      string          `json:"name,omitempty"`
	Arguments string          `json:"arguments,omitempty"`
	Output    string          `json:"output,omitempty"`
	Status    string          `json:"status,omitempty"`
	Content   json.RawMessage `json:"content,omitempty"`
}

type ResponseCreateEvent struct {
	Type     string          `json:"type"`
	EventID  string          `json:"event_id,omitempty"`
	Response *ResponseConfig `json:"response,omitempty"`
}

func (e ResponseCreateEvent) EventType() string { return e.Type }
func (ResponseCreateEvent) isClientEvent()      {}

type ResponseConfig struct {
	Conversation     string          `json:"conversation,omitempty"`
	Instructions     string          `json:"instructions,omitempty"`
	OutputModalities []string        `json:"output_modalities,omitempty"`
	ToolChoice       json.RawMessage `json:"tool_choice,omitempty"`
	Tools            json.RawMessage `json:"tools,omitempty"`
	Metadata         json.RawMessage `json:"metadata,omitempty"`
	Input            json.RawMessage `json:"input,omitempty"`
}

type ResponseCancelEvent struct {
	Type       string `json:"type"`
	EventID    string `json:"event_id,omitempty"`
	ResponseID string `json:"response_id,omitempty"`
}

func (e ResponseCancelEvent) EventType() string { return e.Type }
func (ResponseCancelEvent) isClientEvent()      {}

// LiveToolApprovalResolveEvent contains no identity claims. The gateway binds
// it to the authenticated subject and current conversation/incarnation.
type LiveToolApprovalResolveEvent struct {
	Type           string `json:"type"`
	InvocationID   string `json:"invocation_id"`
	Decision       string `json:"decision"`
	Reason         string `json:"reason"`
	IdempotencyKey string `json:"idempotency_key"`
}

func (e LiveToolApprovalResolveEvent) EventType() string { return e.Type }
func (LiveToolApprovalResolveEvent) isClientEvent()      {}
