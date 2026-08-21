package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"time"
)

const (
	maximumOperatorActionBytes = 4 << 10
	// Compose may build images, wait for health checks, and honour service stop
	// grace periods.  This is intentionally longer than those reviewed bounds.
	operatorActionTimeout = 150 * time.Second
)

type operatorActionRequest struct {
	Component operatorComponent
	Action    operatorAction
}

type operatorAPIStatus struct {
	Component operatorComponent `json:"component"`
	State     string            `json:"state"`
	Code      string            `json:"code"`
	Ready     bool              `json:"ready"`
	Busy      bool              `json:"busy"`
	Owned     bool              `json:"owned"`
}

type operatorAPISnapshot struct {
	Components []operatorAPIStatus `json:"components"`
}

func newConfiguredOperatorSupervisor(cfg demoConfig) (*operatorSupervisor, error) {
	if !cfg.Operator.Enabled {
		return nil, nil
	}
	if cfg.Operator.supervisor != nil {
		return cfg.Operator.supervisor, nil
	}
	runner, err := newLocalOperatorRunner(cfg.Operator, nil, nil)
	if err != nil {
		return nil, err
	}
	return newOperatorSupervisor(runner, operatorActionTimeout), nil
}

func (handler *demoHandler) serveOperatorStatus(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if handler.operator == nil {
		writer.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	statuses := handler.operator.Snapshot(request.Context())
	response := operatorAPISnapshot{Components: make([]operatorAPIStatus, 0, len(statuses))}
	for _, status := range statuses {
		response.Components = append(response.Components, operatorAPIStatus(status))
	}
	writeOperatorJSON(writer, http.StatusOK, response)
}

func (handler *demoHandler) serveVoiceReadiness(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	// Client-only mode deliberately delegates readiness to its externally
	// managed Gateway. Operator mode has stronger local knowledge and must not
	// let the browser open a microphone or WebSocket into a stopped voice path.
	if handler.operator == nil {
		writer.WriteHeader(http.StatusNoContent)
		return
	}
	ready := make(map[operatorComponent]bool, 3)
	for _, status := range handler.operator.Snapshot(request.Context()) {
		ready[status.Component] = status.Ready
	}
	for _, component := range []operatorComponent{componentSpeech, componentGateway, componentClient} {
		if !ready[component] {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
	}
	writer.WriteHeader(http.StatusNoContent)
}

func (handler *demoHandler) serveOperatorAction(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if handler.operator == nil {
		writer.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	origins := request.Header.Values("Origin")
	if len(origins) != 1 || origins[0] != handler.cfg.Origin {
		writer.WriteHeader(http.StatusForbidden)
		return
	}
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	action, err := decodeOperatorAction(writer, request)
	if err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	if !validOperation(action.Component, action.Action) {
		writer.WriteHeader(http.StatusUnprocessableEntity)
		return
	}
	operatorContext := handler.operatorContext
	if operatorContext == nil {
		operatorContext = context.Background()
	}
	status := handler.operator.Apply(operatorContext, action.Component, action.Action)
	writeOperatorJSON(writer, operatorActionResponseStatus(status), operatorAPIStatus(status))
}

func decodeOperatorAction(writer http.ResponseWriter, request *http.Request) (operatorActionRequest, error) {
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, maximumOperatorActionBytes))
	token, err := decoder.Token()
	if err != nil {
		return operatorActionRequest{}, errors.New("invalid operator action")
	}
	delimiter, ok := token.(json.Delim)
	if !ok || delimiter != '{' {
		return operatorActionRequest{}, errors.New("invalid operator action")
	}
	seen := make(map[string]bool, 2)
	var action operatorActionRequest
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return operatorActionRequest{}, errors.New("invalid operator action")
		}
		key, ok := token.(string)
		if !ok || seen[key] {
			return operatorActionRequest{}, errors.New("invalid operator action")
		}
		seen[key] = true
		value, err := decodeOperatorActionString(decoder)
		if err != nil {
			return operatorActionRequest{}, err
		}
		switch key {
		case "component":
			action.Component = operatorComponent(value)
		case "action":
			action.Action = operatorAction(value)
		default:
			return operatorActionRequest{}, errors.New("invalid operator action")
		}
	}
	token, err = decoder.Token()
	if err != nil {
		return operatorActionRequest{}, errors.New("invalid operator action")
	}
	delimiter, ok = token.(json.Delim)
	if !ok || delimiter != '}' || !seen["component"] || !seen["action"] || len(seen) != 2 {
		return operatorActionRequest{}, errors.New("invalid operator action")
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return operatorActionRequest{}, errors.New("invalid operator action")
	}
	return action, nil
}

func decodeOperatorActionString(decoder *json.Decoder) (string, error) {
	var raw json.RawMessage
	if err := decoder.Decode(&raw); err != nil || len(raw) < 2 || raw[0] != '"' || raw[len(raw)-1] != '"' {
		return "", errors.New("invalid operator action")
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", errors.New("invalid operator action")
	}
	return value, nil
}

func operatorActionResponseStatus(status componentStatus) int {
	switch status.Code {
	case "ready", "stopped":
		return http.StatusOK
	case "dependency_not_ready", "dependency_running", "operation_in_progress", "process_exited":
		return http.StatusConflict
	case "missing_configuration", "startup_timeout", "operation_failed":
		return http.StatusServiceUnavailable
	default:
		return http.StatusServiceUnavailable
	}
}

func writeOperatorJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
