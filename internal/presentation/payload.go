package presentation

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"unicode"
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

type publicProgress struct {
	Phase   string `json:"phase"`
	Summary string `json:"summary"`
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
		if record.Phase != "" {
			summary, ok := worker.StableProgressSummary(record.Phase)
			if !ok || record.Summary != summary {
				return PublicEvent{}, malformed(event, errors.New("progress phase is invalid"))
			}
			details = publicProgress{Phase: record.Phase, Summary: summary}
		} else {
			// Legacy content-free generation heartbeats remain present for
			// non-phased tools; stable tool phases always use publicProgress.
			details = publicSummary{Summary: record.Summary}
		}
		out.Summary = record.Summary
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
		if err := decodeRecord(event.Payload, &record); err != nil || !publicString(record.Result.Summary, 16<<10, true) {
			return PublicEvent{}, malformed(event, err)
		}
		structured, err := sanitizeCompletionStructured(record.Result.Structured)
		if err != nil {
			return PublicEvent{}, malformed(event, err)
		}
		out.Summary = record.Result.Summary
		details = publicCompletion{Summary: record.Result.Summary, Structured: structured}
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

type completionSource struct {
	Title string `json:"title"`
	URL   string `json:"url"`
}

func sanitizeCompletionStructured(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 || len(raw) > maxPublicPayloadBytes || !utf8.Valid(raw) {
		return nil, errors.New("completion projection is invalid")
	}
	object, err := strictRawObject(raw)
	if err != nil {
		return nil, errors.New("completion projection is invalid")
	}
	if len(object) == 0 {
		return nil, nil
	}
	if len(object) < 1 || len(object) > 2 {
		return nil, errors.New("completion projection is invalid")
	}
	textRaw, ok := object["text"]
	if !ok {
		return nil, errors.New("completion projection is invalid")
	}
	var text string
	if err := json.Unmarshal(textRaw, &text); err != nil || !publicString(text, worker.MaxCompletionTextBytes, true) {
		return nil, errors.New("completion projection is invalid")
	}
	var sources []completionSource
	if sourcesRaw, present := object["sources"]; present {
		sources, err = decodeCompletionSources(sourcesRaw)
		if err != nil {
			return nil, err
		}
	}
	if len(object) != 1 && object["sources"] == nil {
		return nil, errors.New("completion projection is invalid")
	}
	if object["sources"] == nil {
		encoded, err := json.Marshal(struct {
			Text string `json:"text"`
		}{Text: text})
		return json.RawMessage(encoded), err
	}
	encoded, err := json.Marshal(struct {
		Text    string             `json:"text"`
		Sources []completionSource `json:"sources"`
	}{Text: text, Sources: sources})
	if err != nil || len(encoded) > maxPublicPayloadBytes {
		return nil, errors.New("completion projection is invalid")
	}
	return json.RawMessage(encoded), nil
}

func strictRawObject(raw json.RawMessage) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, errors.New("object is invalid")
	}
	object := make(map[string]json.RawMessage)
	for decoder.More() {
		keyToken, err := decoder.Token()
		key, ok := keyToken.(string)
		if err != nil || !ok {
			return nil, errors.New("object is invalid")
		}
		if _, duplicate := object[key]; duplicate {
			return nil, errors.New("object is invalid")
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, errors.New("object is invalid")
		}
		object[key] = value
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') {
		return nil, errors.New("object is invalid")
	}
	if _, err = decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("object is invalid")
	}
	return object, nil
}

func decodeCompletionSources(raw json.RawMessage) ([]completionSource, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('[') {
		return nil, errors.New("completion sources are invalid")
	}
	sources := make([]completionSource, 0)
	for decoder.More() {
		if len(sources) == 10 {
			return nil, errors.New("completion sources are invalid")
		}
		var sourceRaw json.RawMessage
		if err := decoder.Decode(&sourceRaw); err != nil {
			return nil, errors.New("completion sources are invalid")
		}
		object, err := strictRawObject(sourceRaw)
		if err != nil || len(object) != 2 || object["title"] == nil || object["url"] == nil {
			return nil, errors.New("completion sources are invalid")
		}
		var source completionSource
		if json.Unmarshal(object["title"], &source.Title) != nil || json.Unmarshal(object["url"], &source.URL) != nil || !validCompletionTitle(source.Title) || !validCompletionURL(source.URL) {
			return nil, errors.New("completion sources are invalid")
		}
		sources = append(sources, source)
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim(']') {
		return nil, errors.New("completion sources are invalid")
	}
	if _, err = decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("completion sources are invalid")
	}
	return sources, nil
}

func validCompletionTitle(value string) bool {
	if !utf8.ValidString(value) {
		return false
	}
	count := 0
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
		count++
	}
	return count >= 1 && count <= 256
}

func validCompletionURL(raw string) bool {
	if len(raw) < 1 || len(raw) > 2048 || !utf8.ValidString(raw) {
		return false
	}
	for _, character := range raw {
		if character > unicode.MaxASCII || unicode.IsControl(character) {
			return false
		}
	}
	decoded, err := url.PathUnescape(raw)
	if err != nil || !utf8.ValidString(decoded) {
		return false
	}
	for _, character := range decoded {
		if unicode.IsControl(character) {
			return false
		}
	}
	parsed, err := url.Parse(raw)
	return err == nil && parsed.Scheme == "https" && parsed.Host != "" && parsed.Hostname() != "" && parsed.User == nil && parsed.Opaque == ""
}

func unsupported(event handoff.Event) error {
	return fmt.Errorf("%w: %s/%s", ErrUnsupportedEvent, event.Kind, event.State)
}

func malformed(event handoff.Event, err error) error {
	if err == nil {
		err = errors.New("record fields are invalid")
	}
	return fmt.Errorf("sanitize %s: %w", event.Kind, err)
}
