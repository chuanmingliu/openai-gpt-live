package realtime

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

// InputTurnMode identifies the single authority allowed to close an input
// audio turn. It is deliberately provider-neutral: a provider VAD owns commit
// events, while manual mode admits client commit commands.
type InputTurnMode uint8

const (
	InputTurnModeUnspecified InputTurnMode = iota
	InputTurnModeManual
	InputTurnModeProviderVAD
)

var (
	ErrInputTurnModeUnconfirmed  = errors.New("input turn mode is not confirmed")
	ErrManualAudioCommitDisabled = errors.New("manual input audio commit is disabled by provider VAD")
	ErrInputTurnModeImmutable    = errors.New("input turn mode cannot change within a session")
	ErrPendingInputTurnMode      = errors.New("input turn mode update is pending provider confirmation")
)

// ParseInputTurnMode interprets the Realtime turn_detection value. An omitted
// value means that a session update leaves the mode unchanged; explicit null
// selects manual commit authority.
func ParseInputTurnMode(raw json.RawMessage) (InputTurnMode, bool, error) {
	if len(raw) == 0 {
		return InputTurnModeUnspecified, false, nil
	}
	trimmed := bytes.TrimSpace(raw)
	if bytes.Equal(trimmed, []byte("null")) {
		return InputTurnModeManual, true, nil
	}
	if len(trimmed) == 0 || trimmed[0] != '{' || !json.Valid(trimmed) {
		return InputTurnModeUnspecified, false, errors.New("turn detection must be null or an object")
	}
	var value struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(trimmed, &value); err != nil {
		return InputTurnModeUnspecified, false, fmt.Errorf("decode turn detection: %w", err)
	}
	switch value.Type {
	case "server_vad", "semantic_vad":
		return InputTurnModeProviderVAD, true, nil
	default:
		return InputTurnModeUnspecified, false, fmt.Errorf("unsupported turn detection type %q", value.Type)
	}
}

// ParseSessionUpdatedInputTurnMode extracts the effective turn_detection value
// from an actual provider session.updated payload. Missing is distinct from
// explicit null so callers cannot mistake an incomplete acknowledgement for
// confirmation of manual mode.
func ParseSessionUpdatedInputTurnMode(payload json.RawMessage) (InputTurnMode, bool, error) {
	if len(payload) == 0 || !json.Valid(payload) {
		return InputTurnModeUnspecified, false, errors.New("provider session.updated payload is invalid")
	}
	var envelope struct {
		Session struct {
			Audio *struct {
				Input *struct {
					TurnDetection json.RawMessage `json:"turn_detection"`
				} `json:"input"`
			} `json:"audio"`
		} `json:"session"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return InputTurnModeUnspecified, false, fmt.Errorf("decode provider session.updated: %w", err)
	}
	if envelope.Session.Audio == nil || envelope.Session.Audio.Input == nil ||
		len(envelope.Session.Audio.Input.TurnDetection) == 0 {
		return InputTurnModeUnspecified, false, nil
	}
	return ParseInputTurnMode(envelope.Session.Audio.Input.TurnDetection)
}
