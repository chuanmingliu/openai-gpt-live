package safety

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
)

const (
	MaximumSummaryBytes    = 16 << 10
	MaximumStructuredBytes = 64 << 10
	MaximumReasonCodeBytes = 64
)

var reasonCodePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]*$`)

type Action string
type Channel string

const (
	ChannelWorker Channel = "worker"
	ChannelVisual Channel = "visual"
	ChannelSpeech Channel = "speech"
)

const (
	ActionAllow                Action = "allow"
	ActionRedact               Action = "redact"
	ActionSuppress             Action = "suppress"
	ActionRequestClarification Action = "request_clarification"
	ActionCancelResponse       Action = "cancel_response"
	ActionTerminateSession     Action = "terminate_session"
)

type WorkerResultCandidate struct {
	SessionID        identity.SessionID
	ConversationID   identity.ConversationID
	HandoffID        identity.HandoffID
	Channel          Channel
	Summary          string
	Structured       json.RawMessage
	RequiresApproval bool
	Approved         bool
	ProviderBlocked  bool
	RequestedAction  Action
}

func (c WorkerResultCandidate) Clone() WorkerResultCandidate {
	c.Structured = append(json.RawMessage(nil), c.Structured...)
	return c
}

func (c WorkerResultCandidate) Validate() error {
	if invalidIdentity(string(c.SessionID)) || invalidIdentity(string(c.ConversationID)) || invalidIdentity(string(c.HandoffID)) {
		return errors.New("safety candidate identity is invalid")
	}
	if c.Channel != ChannelWorker && c.Channel != ChannelVisual && c.Channel != ChannelSpeech {
		return errors.New("safety candidate channel is invalid")
	}
	if c.Approved && !c.RequiresApproval {
		return errors.New("safety candidate approval metadata is invalid")
	}
	if c.RequestedAction != "" && c.RequestedAction != ActionCancelResponse && c.RequestedAction != ActionTerminateSession {
		return errors.New("safety candidate requested action is invalid")
	}
	if strings.TrimSpace(c.Summary) == "" || len(c.Summary) > MaximumSummaryBytes || !utf8.ValidString(c.Summary) {
		return errors.New("safety candidate summary is invalid")
	}
	if len(c.Structured) == 0 || len(c.Structured) > MaximumStructuredBytes || !utf8.Valid(c.Structured) || !strictJSON(c.Structured, 32) {
		return errors.New("safety candidate structured result is invalid")
	}
	return nil
}

func invalidIdentity(value string) bool {
	return strings.TrimSpace(value) == "" || len(value) > 256 || !utf8.ValidString(value)
}

func strictJSON(raw []byte, maximumDepth int) bool {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var readValue func(int) error
	readValue = func(depth int) error {
		if depth > maximumDepth {
			return errors.New("JSON nesting too deep")
		}
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delim, compound := token.(json.Delim)
		if !compound {
			return nil
		}
		switch delim {
		case '{':
			seen := make(map[string]struct{})
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok {
					return errors.New("JSON key is invalid")
				}
				if _, duplicate := seen[key]; duplicate {
					return errors.New("JSON key is duplicated")
				}
				seen[key] = struct{}{}
				if err := readValue(depth + 1); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim('}') {
				return errors.New("JSON object is invalid")
			}
		case '[':
			for decoder.More() {
				if err := readValue(depth + 1); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') {
				return errors.New("JSON array is invalid")
			}
		default:
			return errors.New("JSON delimiter is invalid")
		}
		return nil
	}
	if err := readValue(1); err != nil {
		return false
	}
	_, err := decoder.Token()
	return errors.Is(err, io.EOF)
}

type Decision struct {
	Action      Action `json:"action"`
	SafeSummary string `json:"safe_summary,omitempty"`
	ReasonCode  string `json:"reason_code"`
}

func (d Decision) Validate() error {
	switch d.Action {
	case ActionAllow, ActionRedact, ActionSuppress, ActionRequestClarification, ActionCancelResponse, ActionTerminateSession:
	default:
		return errors.New("safety decision action is invalid")
	}
	if strings.TrimSpace(d.ReasonCode) == "" || len(d.ReasonCode) > MaximumReasonCodeBytes || !utf8.ValidString(d.ReasonCode) || !reasonCodePattern.MatchString(d.ReasonCode) {
		return errors.New("safety decision reason is invalid")
	}
	if len(d.SafeSummary) > MaximumSummaryBytes || !utf8.ValidString(d.SafeSummary) {
		return errors.New("safety decision summary is invalid")
	}
	switch d.Action {
	case ActionRedact, ActionRequestClarification:
		if strings.TrimSpace(d.SafeSummary) == "" {
			return errors.New("safety decision summary is required")
		}
	case ActionAllow, ActionSuppress, ActionTerminateSession:
		if d.SafeSummary != "" {
			return errors.New("safety decision summary is forbidden")
		}
	}
	return nil
}

type Policy interface {
	EvaluateWorkerResult(context.Context, WorkerResultCandidate) (Decision, error)
}

type DenyAll struct{}

func (DenyAll) EvaluateWorkerResult(ctx context.Context, candidate WorkerResultCandidate) (Decision, error) {
	if ctx == nil {
		return Decision{}, errors.New("safety context is required")
	}
	if err := ctx.Err(); err != nil {
		return Decision{}, err
	}
	return Decision{Action: ActionSuppress, ReasonCode: "deny_all"}, nil
}

func EvaluateOrSuppress(ctx context.Context, policy Policy, candidate WorkerResultCandidate) Decision {
	if nilPolicy(policy) || ctx == nil || ctx.Err() != nil || candidate.Validate() != nil {
		return Decision{Action: ActionSuppress, ReasonCode: "policy_error"}
	}
	decision, err, panicked := evaluatePolicy(ctx, policy, candidate.Clone())
	if panicked || err != nil || decision.Validate() != nil {
		return Decision{Action: ActionSuppress, ReasonCode: "policy_error"}
	}
	return decision
}

func evaluatePolicy(ctx context.Context, policy Policy, candidate WorkerResultCandidate) (decision Decision, err error, panicked bool) {
	defer func() {
		if recover() != nil {
			decision = Decision{}
			err = nil
			panicked = true
		}
	}()
	decision, err = policy.EvaluateWorkerResult(ctx, candidate)
	return decision, err, false
}

func nilPolicy(policy Policy) bool {
	if policy == nil {
		return true
	}
	value := reflect.ValueOf(policy)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
