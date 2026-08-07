package handoff

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
)

const (
	ToolStartBackgroundTask  = "start_background_task"
	ToolSteerBackgroundTask  = "steer_background_task"
	ToolCancelBackgroundTask = "cancel_background_task"
)

type StartBackgroundTaskArgs struct {
	Objective        string `json:"objective"`
	PresentationHint string `json:"presentation_hint,omitempty"`
}

type SteerBackgroundTaskArgs struct {
	HandoffID identity.HandoffID `json:"handoff_id"`
	Objective string             `json:"objective"`
}

type CancelBackgroundTaskArgs struct {
	HandoffID identity.HandoffID `json:"handoff_id"`
	Reason    string             `json:"reason,omitempty"`
}

func ParseReservedTool(name string, arguments json.RawMessage) (any, error) {
	canonical, err := canonicalJSON(arguments, maxJSONBytes, maxJSONDepth)
	if err != nil {
		return nil, invalid("tool arguments", err)
	}
	switch name {
	case ToolStartBackgroundTask:
		fields, err := exactToolObject(canonical, "objective", "presentation_hint")
		if err != nil {
			return nil, err
		}
		objective, err := requiredToolString(fields, "objective")
		if err != nil {
			return nil, err
		}
		presentationHint, err := optionalToolString(fields, "presentation_hint")
		if err != nil {
			return nil, err
		}
		args := StartBackgroundTaskArgs{Objective: objective, PresentationHint: presentationHint}
		if err := boundedRequired("objective", args.Objective, maxObjectiveBytes); err != nil {
			return nil, err
		}
		if _, present := fields["presentation_hint"]; present {
			if err := boundedRequired("presentation hint", args.PresentationHint, maxPresentationBytes); err != nil {
				return nil, err
			}
		}
		return args, nil
	case ToolSteerBackgroundTask:
		fields, err := exactToolObject(canonical, "handoff_id", "objective")
		if err != nil {
			return nil, err
		}
		handoffID, err := requiredToolString(fields, "handoff_id")
		if err != nil {
			return nil, err
		}
		objective, err := requiredToolString(fields, "objective")
		if err != nil {
			return nil, err
		}
		args := SteerBackgroundTaskArgs{HandoffID: identity.HandoffID(handoffID), Objective: objective}
		if err := boundedRequired("handoff id", string(args.HandoffID), maxIdentityBytes); err != nil {
			return nil, err
		}
		if err := boundedRequired("objective", args.Objective, maxObjectiveBytes); err != nil {
			return nil, err
		}
		return args, nil
	case ToolCancelBackgroundTask:
		fields, err := exactToolObject(canonical, "handoff_id", "reason")
		if err != nil {
			return nil, err
		}
		handoffID, err := requiredToolString(fields, "handoff_id")
		if err != nil {
			return nil, err
		}
		reason, err := optionalToolString(fields, "reason")
		if err != nil {
			return nil, err
		}
		args := CancelBackgroundTaskArgs{HandoffID: identity.HandoffID(handoffID), Reason: reason}
		if err := boundedRequired("handoff id", string(args.HandoffID), maxIdentityBytes); err != nil {
			return nil, err
		}
		if _, present := fields["reason"]; present {
			if err := boundedRequired("reason", args.Reason, maxReasonBytes); err != nil {
				return nil, err
			}
		}
		return args, nil
	default:
		return nil, invalid("unknown reserved tool", nil)
	}
}

func exactToolObject(raw []byte, allowed ...string) (map[string]json.RawMessage, error) {
	if len(raw) == 0 || raw[0] != '{' {
		return nil, invalid("tool arguments must be one object", nil)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, invalid("tool arguments", err)
	}
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, key := range allowed {
		allowedSet[key] = struct{}{}
	}
	for key := range fields {
		if _, ok := allowedSet[key]; !ok {
			return nil, invalid(fmt.Sprintf("unknown tool argument %q", key), nil)
		}
	}
	return fields, nil
}

func requiredToolString(fields map[string]json.RawMessage, key string) (string, error) {
	raw, ok := fields[key]
	if !ok {
		return "", invalid(fmt.Sprintf("tool argument %q is required", key), nil)
	}
	return decodeToolString(raw, key)
}

func optionalToolString(fields map[string]json.RawMessage, key string) (string, error) {
	raw, ok := fields[key]
	if !ok {
		return "", nil
	}
	return decodeToolString(raw, key)
}

func decodeToolString(raw json.RawMessage, key string) (string, error) {
	if bytes.Equal(raw, []byte("null")) {
		return "", invalid(fmt.Sprintf("tool argument %q must be a string", key), nil)
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", invalid(fmt.Sprintf("tool argument %q must be a string", key), err)
	}
	return value, nil
}
