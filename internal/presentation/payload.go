package presentation

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/worker"
)

const maxPublicPayloadBytes = 64 << 10

var ErrUnsupportedEvent = errors.New("unsupported handoff event")

// PublicEvent is the only durable-event representation allowed to cross a
// presentation boundary. Details is reconstructed from explicitly public
// fields; repository payload bytes are never forwarded.
type PublicEvent struct {
	Kind    string
	State   handoff.State
	Summary string
	Details json.RawMessage
}

type publicSummary struct {
	Summary string `json:"summary,omitempty"`
}

type publicNeedsInput struct {
	Prompt string          `json:"prompt"`
	Schema json.RawMessage `json:"schema,omitempty"`
}

type publicCompletion struct {
	Summary    string          `json:"summary"`
	Structured json.RawMessage `json:"structured,omitempty"`
}

type publicFailure struct {
	Code      string `json:"code"`
	Retryable bool   `json:"retryable"`
	Message   string `json:"message"`
}

type publicCancel struct {
	Reason string `json:"reason,omitempty"`
}

type approvalOutcomeRecord struct {
	InvocationID string `json:"invocation_id"`
	Outcome      string `json:"outcome"`
}

// SanitizeEvent validates the exact kind/state pairs emitted by the postgres
// repository and decodes the concrete producer record before constructing a
// bounded public payload.
func SanitizeEvent(event handoff.Event) (PublicEvent, error) {
	out := PublicEvent{Kind: event.Kind, State: event.State}
	var details any = struct{}{}
	switch event.Kind {
	case "handoff.created":
		if event.State != handoff.StatePending {
			return PublicEvent{}, unsupported(event)
		}
	case "handoff.leased":
		if event.State != handoff.StateLeased && event.State != handoff.StateRunning {
			return PublicEvent{}, unsupported(event)
		}
	case "handoff.running":
		if event.State != handoff.StateRunning {
			return PublicEvent{}, unsupported(event)
		}
	case "handoff.steered":
		if !nonterminal(event.State) {
			return PublicEvent{}, unsupported(event)
		}
		var record struct {
			Objective string `json:"objective"`
		}
		if err := decodeRecord(event.Payload, &record); err != nil || !publicString(record.Objective, 8192, true) {
			return PublicEvent{}, malformed(event, err)
		}
		out.Summary = "A background task objective was updated."
		details = publicSummary{Summary: out.Summary}
	case "handoff.input_provided":
		if event.State != handoff.StateNeedsInput {
			return PublicEvent{}, unsupported(event)
		}
		out.Summary = "Additional input was provided to a background task."
		details = publicSummary{Summary: out.Summary}
	case "handoff.cancel_requested":
		if !nonterminal(event.State) {
			return PublicEvent{}, unsupported(event)
		}
		var record struct {
			Reason string `json:"reason"`
		}
		if err := decodeRecord(event.Payload, &record); err != nil || !publicString(record.Reason, 2048, false) {
			return PublicEvent{}, malformed(event, err)
		}
		out.Summary = record.Reason
		details = publicCancel{Reason: record.Reason}
	case "handoff.retry":
		if event.State != handoff.StateRunning {
			return PublicEvent{}, unsupported(event)
		}
	case "handoff.progress":
		if event.State != handoff.StateRunning {
			return PublicEvent{}, unsupported(event)
		}
		var record worker.ProgressRecord
		if err := decodeRecord(event.Payload, &record); err != nil || !publicString(record.Summary, 16<<10, true) {
			return PublicEvent{}, malformed(event, err)
		}
		out.Summary = record.Summary
		details = publicSummary{Summary: record.Summary}
	case "tool.approval_approved":
		if event.State != handoff.StatePending {
			return PublicEvent{}, unsupported(event)
		}
		var record approvalOutcomeRecord
		if err := decodeRecord(event.Payload, &record); err != nil || !publicString(record.InvocationID, 256, true) || record.Outcome != "approved" {
			return PublicEvent{}, malformed(event, err)
		}
		out.Summary = "Tool approval accepted."
		details = publicSummary{Summary: out.Summary}
	case "tool.approval_denied", "tool.approval_expired":
		if event.State != handoff.StateFailed {
			return PublicEvent{}, unsupported(event)
		}
		want, code, message := "denied", "approval_denied", "Tool approval was denied."
		if event.Kind == "tool.approval_expired" {
			want, code, message = "expired", "approval_expired", "Tool approval expired."
		}
		var record approvalOutcomeRecord
		if err := decodeRecord(event.Payload, &record); err != nil || !publicString(record.InvocationID, 256, true) || record.Outcome != want {
			return PublicEvent{}, malformed(event, err)
		}
		out.Summary = message
		details = publicFailure{Code: code, Retryable: false, Message: message}
	case "handoff.needs-input":
		if event.State != handoff.StateNeedsInput {
			return PublicEvent{}, unsupported(event)
		}
		var record worker.NeedsInputRecord
		if err := decodeRecord(event.Payload, &record); err != nil || !publicString(record.Prompt, 16<<10, true) || !publicJSON(record.Schema) {
			return PublicEvent{}, malformed(event, err)
		}
		out.Summary = record.Prompt
		details = publicNeedsInput{Prompt: record.Prompt, Schema: cloneJSON(record.Schema)}
	case "handoff.complete":
		if event.State != handoff.StateSucceeded {
			return PublicEvent{}, unsupported(event)
		}
		var record worker.CompletionRecord
		if err := decodeRecord(event.Payload, &record); err != nil || !publicString(record.Result.Summary, 16<<10, true) || !publicJSON(record.Result.Structured) {
			return PublicEvent{}, malformed(event, err)
		}
		out.Summary = record.Result.Summary
		details = publicCompletion{Summary: record.Result.Summary, Structured: cloneJSON(record.Result.Structured)}
	case "handoff.fail":
		if event.State != handoff.StateFailed {
			return PublicEvent{}, unsupported(event)
		}
		var record worker.FailureRecord
		if err := decodeRecord(event.Payload, &record); err != nil || !publicString(record.Failure.Code, 512, true) || !publicString(record.Failure.Message, 16<<10, true) {
			return PublicEvent{}, malformed(event, err)
		}
		out.Summary = record.Failure.Message
		details = publicFailure{Code: record.Failure.Code, Retryable: record.Failure.Retryable, Message: record.Failure.Message}
	case "handoff.cancel":
		if event.State != handoff.StateCancelled {
			return PublicEvent{}, unsupported(event)
		}
		var record worker.CancelledRecord
		if err := decodeRecord(event.Payload, &record); err != nil || !publicString(record.Reason, 2048, false) {
			return PublicEvent{}, malformed(event, err)
		}
		out.Summary = record.Reason
		details = publicCancel{Reason: record.Reason}
	case "handoff.lease_expired":
		if event.State != handoff.StatePending && event.State != handoff.StateRunning {
			return PublicEvent{}, unsupported(event)
		}
	case "handoff.cancelled":
		if event.State != handoff.StateCancelled {
			return PublicEvent{}, unsupported(event)
		}
	case "handoff.expired":
		if event.State != handoff.StateExpired {
			return PublicEvent{}, unsupported(event)
		}
	default:
		return PublicEvent{}, unsupported(event)
	}
	encoded, err := json.Marshal(details)
	if err != nil || len(encoded) > maxPublicPayloadBytes {
		return PublicEvent{}, fmt.Errorf("sanitize %s: public payload exceeds limit", event.Kind)
	}
	out.Details = encoded
	return out, nil
}

func nonterminal(state handoff.State) bool {
	return state == handoff.StatePending || state == handoff.StateLeased || state == handoff.StateRunning || state == handoff.StateNeedsInput
}

func decodeRecord(raw json.RawMessage, target any) error {
	if len(raw) == 0 || len(raw) > maxPublicPayloadBytes || !json.Valid(raw) {
		return errors.New("invalid record JSON")
	}
	return json.Unmarshal(raw, target)
}

func publicString(value string, limit int, required bool) bool {
	return (!required || strings.TrimSpace(value) != "") && len(value) <= limit && utf8.ValidString(value)
}

func publicJSON(value json.RawMessage) bool {
	return len(value) == 0 || (len(value) <= maxPublicPayloadBytes && json.Valid(value))
}

func cloneJSON(value json.RawMessage) json.RawMessage { return append(json.RawMessage(nil), value...) }

func unsupported(event handoff.Event) error {
	return fmt.Errorf("%w: %s/%s", ErrUnsupportedEvent, event.Kind, event.State)
}

func malformed(event handoff.Event, err error) error {
	if err == nil {
		err = errors.New("record fields are invalid")
	}
	return fmt.Errorf("sanitize %s: %w", event.Kind, err)
}
