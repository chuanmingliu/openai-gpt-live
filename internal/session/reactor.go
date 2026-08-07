package session

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/protocol"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
)

const (
	PendingBackgroundToolLimit = 16
	PendingBackgroundToolBytes = 1 << 20
	PendingSessionUpdateLimit  = 16
	PendingSessionUpdateBytes  = 1 << 20
	PendingHandoffResultLimit  = 16
	PendingHandoffResultBytes  = 64 << 10
	DurableIdentifierBytes     = 256
	DurableTranscriptBytes     = 64 << 10
	DurableArgumentsBytes      = 64 << 10

	StartBackgroundTaskTool              = handoff.ToolStartBackgroundTask
	SteerBackgroundTaskTool              = handoff.ToolSteerBackgroundTask
	CancelBackgroundTaskTool             = handoff.ToolCancelBackgroundTask
	DurableKindTurnProvisional           = "turn.provisional"
	DurableKindTurnCommitted             = "turn.committed"
	AsyncKindStartBackgroundTask         = StartBackgroundTaskTool
	AsyncKindStartBackgroundTaskRejected = "start_background_task.rejected"
)

var (
	ErrInvalidTransition         = errors.New("invalid session transition")
	ErrUnsupportedInput          = errors.New("unsupported session input")
	ErrMediaInput                = errors.New("audio append belongs to the media loop")
	ErrDurableSequenceGap        = errors.New("durable sequence gap")
	ErrPendingToolLimit          = errors.New("pending background tool limit exceeded")
	ErrInvalidVoiceEvent         = errors.New("invalid voice event")
	ErrUnsupportedField          = errors.New("unsupported nonzero client field")
	ErrInvalidSessionTool        = errors.New("invalid session tool")
	ErrPendingSessionUpdateLimit = errors.New("pending session update limit exceeded")
	ErrInvalidSessionIDs         = errors.New("invalid session IDs")
	ErrTurnRevisionExhausted     = errors.New("session turn revision exhausted")
	ErrTurnRevisionRegression    = errors.New("session turn revision must increase")
)

type terminalMarker struct {
	responseID string
	epoch      identity.ResponseEpoch
}

type pendingSessionUpdate struct {
	session       json.RawMessage
	retainedBytes int
}

type pendingTool struct {
	responseID string
	callID     string
	name       string
	arguments  json.RawMessage
	turn       *identity.TurnKey
}

type activeInputTurn struct {
	key                identity.TurnKey
	committed          bool
	transcriptComplete bool
	transcript         string
	durableSequence    uint64
}

type handoffCall struct {
	originEpoch identity.ResponseEpoch
	turn        identity.TurnKey
	responseID  string
	callID      string
	completed   *HandoffAcceptedInput
	emitted     bool
	bytes       int
}

type handoffToken struct {
	originEpoch identity.ResponseEpoch
	turn        identity.TurnKey
	responseID  string
	callID      string
}

type finalizedTranscript struct {
	turn identity.TurnKey
	text string
}

type Reactor struct {
	ids     IDs
	state   State
	fence   *EpochFence
	nextSeq uint64
	initErr error

	activeResponseID          string
	activeResponseEpoch       identity.ResponseEpoch
	terminal                  *terminalMarker
	pendingSessionUpdates     map[uint64]pendingSessionUpdate
	pendingSessionUpdateBytes int
	connected                 bool
	pendingTools              []pendingTool
	pendingToolBytes          int
	activeTurn                *activeInputTurn
	inputTurnRevision         uint32
	pendingCancelSequence     uint64
	synthEventSequence        uint64
	transcriptSequence        uint64
	committedTranscript       string
	handoffCalls              map[string]*handoffCall
	handoffCallOrder          []string
	handoffCallBytes          int
	terminalResponses         map[string]struct{}
	finalizedAssistantTexts   map[string]finalizedTranscript
	finalizedAssistantOrder   []string
	finalizedAssistantBytes   int
	resolvedHandoffs          map[handoffToken]struct{}
	resolvedHandoffOrder      []handoffToken
}

func NewReactor(ids IDs, fence *EpochFence) *Reactor {
	if fence == nil {
		fence = NewEpochFence(0)
	}
	reactor := &Reactor{
		ids:                     ids,
		state:                   State{Response: ResponseIdle},
		fence:                   fence,
		nextSeq:                 1,
		pendingSessionUpdates:   make(map[uint64]pendingSessionUpdate),
		handoffCalls:            make(map[string]*handoffCall),
		terminalResponses:       make(map[string]struct{}),
		finalizedAssistantTexts: make(map[string]finalizedTranscript),
		resolvedHandoffs:        make(map[handoffToken]struct{}),
	}
	reactor.initErr = validateSessionIDs(ids)
	return reactor
}

func validateSessionIDs(ids IDs) error {
	if ids.SessionID == "" {
		return fmt.Errorf("%w: session ID is empty", ErrInvalidSessionIDs)
	}
	if ids.ConversationID == "" {
		return fmt.Errorf("%w: conversation ID is empty", ErrInvalidSessionIDs)
	}
	if ids.Incarnation == 0 {
		return fmt.Errorf("%w: incarnation is zero", ErrInvalidSessionIDs)
	}
	return nil
}

func (r *Reactor) Apply(input Input) ([]Effect, error) {
	if r == nil {
		return nil, errors.New("session reactor is nil")
	}
	if r.initErr != nil {
		return nil, r.initErr
	}
	switch typed := input.(type) {
	case ConnectedInput:
		return r.applyConnected(typed)
	case ClientInput:
		return r.applyClient(typed.Event)
	case VoiceInput:
		return r.applyVoice(typed)
	case VoiceCommandCompletedInput:
		return r.applyVoiceCommandCompleted(typed)
	case DurableInput:
		return r.applyDurable(typed)
	case TurnCommittedInput:
		return r.applyTurnCommitted(typed)
	case TranscriptPersistedInput:
		return r.applyTranscriptPersisted(typed)
	case HandoffAcceptedInput:
		return r.applyHandoffAccepted(typed)
	case FailedInput:
		return r.applyFailed(typed)
	default:
		return nil, fmt.Errorf("%w: %T", ErrUnsupportedInput, input)
	}
}

func (r *Reactor) applyDurable(input DurableInput) ([]Effect, error) {
	if input.Sequence <= r.state.LastDurableSeq {
		return nil, nil
	}
	want := r.state.LastDurableSeq + 1
	if input.Sequence != want {
		return nil, fmt.Errorf("%w: got %d, want %d", ErrDurableSequenceGap, input.Sequence, want)
	}

	switch input.Kind {
	case DurableKindTurnProvisional:
		turn, err := decodeTurnPayload(input.Payload)
		if err != nil {
			return nil, err
		}
		if r.activeTurn != nil && r.activeTurn.key == turn {
			r.state.LastDurableSeq = input.Sequence
			return nil, nil
		}
		if turn.Revision <= r.inputTurnRevision {
			return nil, fmt.Errorf("%w: got %d after %d", ErrTurnRevisionRegression, turn.Revision, r.inputTurnRevision)
		}
		r.activeTurn = &activeInputTurn{key: turn}
		r.inputTurnRevision = turn.Revision
		r.state.LastDurableSeq = input.Sequence
		return nil, nil
	case DurableKindTurnCommitted:
		turn, err := decodeTurnPayload(input.Payload)
		if err != nil {
			return nil, err
		}
		if turn.Revision < r.inputTurnRevision {
			return r.applyStaleTurnCommit(input.Sequence, turn)
		}
		if turn.Revision == r.inputTurnRevision && r.activeTurn != nil && r.activeTurn.key != turn {
			return nil, fmt.Errorf("%w: revision %d belongs to active turn %q", ErrTurnRevisionRegression, turn.Revision, r.activeTurn.key.ID)
		}
		prepared, err := r.preparePendingToolEffects(turn)
		if err != nil {
			return nil, err
		}
		isCurrent := r.activeTurn != nil && r.activeTurn.key == turn
		isNew := turn.Revision > r.inputTurnRevision
		r.state.CommittedTurn = cloneTurn(&turn)
		if isNew {
			r.activeTurn = &activeInputTurn{key: turn, committed: true}
		} else if isCurrent && turn.Revision == r.inputTurnRevision {
			r.activeTurn.committed = true
		}
		if isNew {
			r.inputTurnRevision = turn.Revision
		}
		r.state.LastDurableSeq = input.Sequence
		r.pendingTools = nil
		r.pendingToolBytes = 0
		for index := range prepared {
			prepared[index].Sequence = r.allocateSequence()
		}
		effects := make([]Effect, len(prepared))
		for index := range prepared {
			async := prepared[index]
			effects[index] = Effect{Async: &async}
		}
		return effects, nil
	default:
		r.state.LastDurableSeq = input.Sequence
		return nil, nil
	}
}

func (r *Reactor) applyTranscriptPersisted(input TranscriptPersistedInput) ([]Effect, error) {
	if input.Sequence <= r.state.LastDurableSeq {
		return nil, nil
	}
	want := r.state.LastDurableSeq + 1
	if input.Sequence != want {
		return nil, fmt.Errorf("%w: got %d, want %d", ErrDurableSequenceGap, input.Sequence, want)
	}
	r.state.LastDurableSeq = input.Sequence
	return nil, nil
}

func (r *Reactor) applyTurnCommitted(input TurnCommittedInput) ([]Effect, error) {
	if input.Sequence <= r.state.LastDurableSeq {
		return nil, nil
	}
	want := r.state.LastDurableSeq + 1
	if input.Sequence != want {
		return nil, fmt.Errorf("%w: got %d, want %d", ErrDurableSequenceGap, input.Sequence, want)
	}
	if input.Turn.ID == "" || input.Turn.Revision == 0 || input.Transcript == "" {
		return nil, errors.New("committed turn requires turn and transcript")
	}
	if err := validateDurableID("committed turn ID", input.Turn.ID); err != nil {
		return nil, err
	}
	if err := validateDurableText("committed transcript", input.Transcript); err != nil {
		return nil, err
	}
	if input.Turn.Revision < r.inputTurnRevision {
		return r.applyTypedStaleTurnCommit(input)
	}
	if input.Turn.Revision == r.inputTurnRevision && r.activeTurn != nil && r.activeTurn.key != input.Turn {
		return nil, fmt.Errorf("%w: revision %d belongs to active turn %q", ErrTurnRevisionRegression, input.Turn.Revision, r.activeTurn.key.ID)
	}
	if r.activeTurn != nil && r.activeTurn.key == input.Turn && r.activeTurn.transcriptComplete {
		if r.activeTurn.transcript != input.Transcript ||
			(r.activeTurn.durableSequence != 0 && r.activeTurn.durableSequence != input.Sequence) {
			return nil, errors.New("committed turn does not match finalized transcript work")
		}
	}

	prepared, err := r.preparePendingToolEffects(input.Turn)
	if err != nil {
		return nil, err
	}
	isCurrent := r.activeTurn != nil && r.activeTurn.key == input.Turn
	isNew := input.Turn.Revision > r.inputTurnRevision
	r.state.CommittedTurn = cloneTurn(&input.Turn)
	r.committedTranscript = input.Transcript
	if isNew {
		r.activeTurn = &activeInputTurn{key: input.Turn, committed: true, transcriptComplete: true, transcript: input.Transcript}
		r.inputTurnRevision = input.Turn.Revision
	} else if isCurrent {
		r.activeTurn.committed = true
		r.activeTurn.transcript = input.Transcript
	}
	r.state.LastDurableSeq = input.Sequence
	r.pendingTools = nil
	r.pendingToolBytes = 0

	effects := make([]Effect, 0, len(prepared))
	for index := range prepared {
		prepared[index].Sequence = r.allocateSequence()
		legacy := prepared[index]
		var payload toolAsyncPayload
		if err := json.Unmarshal(legacy.Payload, &payload); err != nil {
			return nil, fmt.Errorf("decode prepared handoff: %w", err)
		}
		handoffEffect := HandoffRequestedEffect{
			ResponseID: payload.ResponseID, CallID: payload.CallID, ToolName: payload.Name,
			Arguments: cloneRaw(payload.Arguments), Turn: payload.Turn,
			Transcript: input.Transcript, RejectReason: payload.Reason,
		}
		if call := r.handoffCalls[payload.CallID]; call != nil {
			call.turn = payload.Turn
			handoffEffect.OriginEpoch = call.originEpoch
		}
		effects = append(effects, Effect{Async: &legacy, Handoff: &handoffEffect})
	}
	return effects, nil
}

func (r *Reactor) applyTypedStaleTurnCommit(input TurnCommittedInput) ([]Effect, error) {
	legacy, err := r.applyStaleTurnCommit(input.Sequence, input.Turn)
	if err != nil {
		return nil, err
	}
	for index := range legacy {
		if legacy[index].Async == nil {
			continue
		}
		var payload toolAsyncPayload
		if err := json.Unmarshal(legacy[index].Async.Payload, &payload); err != nil {
			return nil, err
		}
		legacy[index].Handoff = &HandoffRequestedEffect{
			ResponseID: payload.ResponseID, CallID: payload.CallID, ToolName: payload.Name,
			Arguments: cloneRaw(payload.Arguments), Turn: payload.Turn,
			Transcript: input.Transcript, RejectReason: payload.Reason,
		}
		if call := r.handoffCalls[payload.CallID]; call != nil {
			legacy[index].Handoff.OriginEpoch = call.originEpoch
		}
	}
	return legacy, nil
}

func (r *Reactor) applyHandoffAccepted(input HandoffAcceptedInput) ([]Effect, error) {
	for name, value := range map[string]string{
		"completion response ID": input.ResponseID,
		"completion call ID":     input.CallID,
		"completion turn ID":     input.Turn.ID,
		"completion handoff ID":  string(input.HandoffID),
	} {
		if value != "" {
			if err := validateDurableID(name, value); err != nil {
				return nil, err
			}
		}
	}
	if len(input.Reason) > 1024 || !utf8.ValidString(input.Reason) {
		return nil, errors.New("completion reason exceeds durable-work bounds")
	}
	token := handoffToken{
		originEpoch: input.OriginEpoch, turn: input.Turn, responseID: input.ResponseID, callID: input.CallID,
	}
	if _, resolved := r.resolvedHandoffs[token]; resolved {
		return nil, nil
	}
	call := r.handoffCalls[input.CallID]
	if call == nil || input.ResponseID == "" || call.responseID != input.ResponseID ||
		call.originEpoch != input.OriginEpoch || call.turn != input.Turn || call.emitted || call.completed != nil {
		return nil, nil
	}
	if input.Accepted == (input.HandoffID == "") {
		return nil, errors.New("accepted handoff result has inconsistent durable ID")
	}
	owned := input
	retained := len(owned.ResponseID) + len(owned.CallID) + len(owned.HandoffID) + len(owned.Reason)
	if retained > PendingHandoffResultBytes-r.handoffCallBytes {
		return nil, ErrPendingToolLimit
	}
	call.completed = &owned
	call.bytes += retained
	r.handoffCallBytes += retained
	if _, terminal := r.terminalResponses[input.ResponseID]; !terminal {
		return nil, nil
	}
	return r.emitHandoffAcceptance(call)
}

func (r *Reactor) emitHandoffAcceptance(call *handoffCall) ([]Effect, error) {
	if call == nil || call.completed == nil || call.emitted {
		return nil, nil
	}
	if r.state.Response != ResponseIdle && r.state.Response != ResponseTerminal {
		return nil, nil
	}
	output := struct {
		Accepted  bool               `json:"accepted"`
		HandoffID identity.HandoffID `json:"handoff_id,omitempty"`
		Reason    string             `json:"reason,omitempty"`
	}{Accepted: call.completed.Accepted, HandoffID: call.completed.HandoffID, Reason: call.completed.Reason}
	outputRaw, err := json.Marshal(output)
	if err != nil {
		return nil, err
	}
	itemRaw, err := json.Marshal(struct {
		Type   string `json:"type"`
		CallID string `json:"call_id"`
		Output string `json:"output"`
	}{Type: "function_call_output", CallID: call.callID, Output: string(outputRaw)})
	if err != nil {
		return nil, err
	}
	itemCommand := realtime.Command{Kind: realtime.CommandCreateItem, Sequence: r.allocateSequence(), Item: itemRaw}
	epoch := r.fence.Advance()
	responseCommand := realtime.Command{Kind: realtime.CommandCreateResponse, Sequence: r.allocateSequence(), Epoch: epoch}
	r.state.Response = ResponsePending
	r.activeResponseID = ""
	r.activeResponseEpoch = epoch
	r.terminal = nil
	r.pendingCancelSequence = 0
	call.emitted = true
	r.handoffCallBytes -= call.bytes
	token := handoffToken{originEpoch: call.originEpoch, turn: call.turn, responseID: call.responseID, callID: call.callID}
	if len(r.resolvedHandoffOrder) >= PendingHandoffResultLimit*2 {
		oldest := r.resolvedHandoffOrder[0]
		r.resolvedHandoffOrder = r.resolvedHandoffOrder[1:]
		delete(r.resolvedHandoffs, oldest)
	}
	r.resolvedHandoffs[token] = struct{}{}
	r.resolvedHandoffOrder = append(r.resolvedHandoffOrder, token)
	delete(r.handoffCalls, call.callID)
	for index, callID := range r.handoffCallOrder {
		if callID == call.callID {
			r.handoffCallOrder = append(r.handoffCallOrder[:index], r.handoffCallOrder[index+1:]...)
			break
		}
	}
	responseStillReferenced := false
	for _, pending := range r.handoffCalls {
		if pending != nil && pending.responseID == call.responseID {
			responseStillReferenced = true
			break
		}
	}
	if !responseStillReferenced {
		delete(r.terminalResponses, call.responseID)
	}
	return []Effect{{VoiceCommand: &itemCommand}, {VoiceCommand: &responseCommand}}, nil
}

func (r *Reactor) applyStaleTurnCommit(sequence uint64, turn identity.TurnKey) ([]Effect, error) {
	prepared := make([]AsyncEffect, 0, len(r.pendingTools))
	kept := make([]pendingTool, 0, len(r.pendingTools))
	keptBytes := 0
	for _, tool := range r.pendingTools {
		isCurrent := tool.turn != nil &&
			r.activeTurn != nil &&
			*tool.turn == r.activeTurn.key &&
			tool.turn.Revision == r.inputTurnRevision
		if isCurrent {
			kept = append(kept, tool)
			keptBytes += len(tool.arguments)
			continue
		}
		reason := r.pendingToolRejectionReason(tool, turn)
		if reason == "" {
			reason = "superseded_turn_revision"
		}
		effectTurn := turn
		if tool.turn != nil {
			effectTurn = *tool.turn
		}
		effect, err := makeToolAsyncEffect(tool, effectTurn, reason)
		if err != nil {
			return nil, err
		}
		prepared = append(prepared, effect)
	}

	r.state.LastDurableSeq = sequence
	r.pendingTools = kept
	r.pendingToolBytes = keptBytes
	effects := make([]Effect, len(prepared))
	for index := range prepared {
		prepared[index].Sequence = r.allocateSequence()
		async := prepared[index]
		effects[index] = Effect{Async: &async}
	}
	return effects, nil
}

func (r *Reactor) Snapshot() Snapshot {
	if r == nil {
		return Snapshot{}
	}
	return Snapshot{
		IDs:            r.ids,
		Response:       r.state.Response,
		ResponseEpoch:  r.fence.Current(),
		UserSpeaking:   r.state.UserSpeaking,
		CommittedTurn:  cloneTurn(r.state.CommittedTurn),
		LastDurableSeq: r.state.LastDurableSeq,
	}
}

func cloneTurn(turn *identity.TurnKey) *identity.TurnKey {
	if turn == nil {
		return nil
	}
	cloned := *turn
	return &cloned
}

func (r *Reactor) applyClient(event protocol.ClientEvent) ([]Effect, error) {
	switch typed := event.(type) {
	case protocol.SessionUpdateEvent:
		return r.applySessionUpdate(typed)
	case protocol.InputAudioBufferAppendEvent:
		return nil, ErrMediaInput
	case protocol.InputAudioBufferCommitEvent:
		return r.applyAudioCommit()
	case protocol.ConversationItemCreateEvent:
		return r.applyConversationItemCreate(typed)
	case protocol.ResponseCreateEvent:
		return r.applyResponseCreate(typed)
	case protocol.ResponseCancelEvent:
		return r.applyResponseCancel(typed)
	default:
		return nil, fmt.Errorf("%w: client event %T", ErrUnsupportedInput, event)
	}
}

func (r *Reactor) applyConnected(input ConnectedInput) ([]Effect, error) {
	providerSession := cloneRaw(input.ProviderSession)
	if len(providerSession) == 0 {
		providerSession = json.RawMessage(`{}`)
	}
	trimmed := bytes.TrimSpace(providerSession)
	if !json.Valid(providerSession) || len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, errors.New("provider session must be a JSON object")
	}

	effects := make([]Effect, 0, 3)
	terminalizedPending := false
	if r.connected {
		r.clearPendingSessionUpdates()
		epoch := r.fence.Advance()
		effects = append(effects, Effect{FlushBefore: epoch})
		if r.state.Response == ResponsePending {
			r.activeResponseEpoch = epoch
			r.state.Response = ResponseTerminal
			terminalizedPending = true
		} else if r.state.Response == ResponseStreaming || r.state.Response == ResponseCancelRequested {
			r.activeResponseEpoch = epoch
			terminal, err := r.synthesizeResponseDone("cancelled")
			if err != nil {
				return nil, err
			}
			effects = append(effects, terminal...)
		}
	} else {
		r.connected = true
	}

	created := protocol.SessionCreatedEvent{
		Type:    "session.created",
		EventID: r.providerOrSynthEventID(""),
		Session: cloneRaw(providerSession),
	}
	effects = append(effects, Effect{ClientEvent: created})
	if terminalizedPending {
		acceptance, err := r.emitNextReadyHandoffAcceptance()
		if err != nil {
			return nil, err
		}
		effects = append(effects, acceptance...)
	}
	return effects, nil
}

func (r *Reactor) applySessionUpdate(event protocol.SessionUpdateEvent) ([]Effect, error) {
	if len(r.pendingSessionUpdates) >= PendingSessionUpdateLimit {
		return nil, ErrPendingSessionUpdateLimit
	}
	commandSession, err := normalizeSessionUpdate(event.Session)
	if err != nil {
		return nil, err
	}
	// json.Marshal creates the transient encoding needed to determine admission
	// size. It is not retained by the reactor when the byte budget rejects it.
	snapshot, err := json.Marshal(event.Session)
	if err != nil {
		return nil, fmt.Errorf("encode session update snapshot: %w", err)
	}
	retainedBytes := len(snapshot)
	if retainedBytes > PendingSessionUpdateBytes-r.pendingSessionUpdateBytes {
		return nil, ErrPendingSessionUpdateLimit
	}

	// Allocate the exact-capacity retained copy only after both admission checks.
	ownedSnapshot := make(json.RawMessage, retainedBytes)
	copy(ownedSnapshot, snapshot)
	retainedBytes = cap(ownedSnapshot)
	sequence := r.allocateSequence()
	// The map count limit bounds entry overhead; retainedBytes accounts for the
	// complete owned RawMessage backing array held by each entry.
	r.pendingSessionUpdates[sequence] = pendingSessionUpdate{
		session:       ownedSnapshot,
		retainedBytes: retainedBytes,
	}
	r.pendingSessionUpdateBytes += retainedBytes
	command := realtime.Command{
		Kind:     realtime.CommandSessionUpdate,
		Sequence: sequence,
		Session:  commandSession,
	}
	return []Effect{{VoiceCommand: &command}}, nil
}

func (r *Reactor) applyAudioCommit() ([]Effect, error) {
	command := realtime.Command{
		Kind:     realtime.CommandCommitAudio,
		Sequence: r.allocateSequence(),
	}
	return []Effect{{VoiceCommand: &command}}, nil
}

func (r *Reactor) applyConversationItemCreate(event protocol.ConversationItemCreateEvent) ([]Effect, error) {
	if event.PreviousItemID != "" {
		return nil, fmt.Errorf("%w: conversation previous item ID", ErrUnsupportedField)
	}
	isUserMessage := event.Item.Type == "message" && event.Item.Role == "user"
	if isUserMessage && event.Item.ID == "" {
		return nil, errors.New("user conversation item requires an ID")
	}
	item, err := json.Marshal(event.Item)
	if err != nil {
		return nil, fmt.Errorf("encode conversation item: %w", err)
	}
	if isUserMessage {
		if err := r.beginInputTurn(event.Item.ID); err != nil {
			return nil, err
		}
	}
	command := realtime.Command{
		Kind:     realtime.CommandCreateItem,
		Sequence: r.allocateSequence(),
		Item:     cloneRaw(item),
	}
	return []Effect{{VoiceCommand: &command}}, nil
}

func (r *Reactor) applyResponseCreate(event protocol.ResponseCreateEvent) ([]Effect, error) {
	if r.state.Response != ResponseIdle && r.state.Response != ResponseTerminal {
		return nil, fmt.Errorf("%w: response.create from state %d", ErrInvalidTransition, r.state.Response)
	}
	response, err := normalizeResponseConfig(event.Response)
	if err != nil {
		return nil, err
	}

	epoch := r.fence.Advance()
	r.state.Response = ResponsePending
	r.activeResponseID = ""
	r.activeResponseEpoch = epoch
	r.terminal = nil
	r.pendingCancelSequence = 0
	command := realtime.Command{
		Kind:     realtime.CommandCreateResponse,
		Sequence: r.allocateSequence(),
		Epoch:    epoch,
		Response: response,
	}
	return []Effect{{VoiceCommand: &command}}, nil
}

func (r *Reactor) applyResponseCancel(event protocol.ResponseCancelEvent) ([]Effect, error) {
	if r.state.Response == ResponseIdle || r.state.Response == ResponseTerminal || r.state.Response == ResponseCancelRequested {
		return nil, nil
	}
	if r.state.Response != ResponsePending && r.state.Response != ResponseStreaming {
		return nil, fmt.Errorf("%w: response.cancel from state %d", ErrInvalidTransition, r.state.Response)
	}
	if event.ResponseID != "" && event.ResponseID != r.activeResponseID {
		return nil, fmt.Errorf("%w: response.cancel names inactive response %q", ErrInvalidTransition, event.ResponseID)
	}

	epoch := r.fence.Advance()
	r.activeResponseEpoch = epoch
	command := realtime.Command{
		Kind:       realtime.CommandCancelResponse,
		Sequence:   r.allocateSequence(),
		Epoch:      epoch,
		ResponseID: r.activeResponseID,
	}
	r.pendingCancelSequence = command.Sequence
	if r.activeResponseID == "" {
		r.state.Response = ResponseTerminal
	} else {
		r.state.Response = ResponseCancelRequested
	}
	effects := []Effect{
		{FlushBefore: epoch},
		{VoiceCommand: &command},
	}
	if r.state.Response == ResponseTerminal {
		acceptance, err := r.emitNextReadyHandoffAcceptance()
		if err != nil {
			return nil, err
		}
		effects = append(effects, acceptance...)
	}
	return effects, nil
}

func (r *Reactor) applyVoice(input VoiceInput) ([]Effect, error) {
	if !r.acceptIncarnation(input.Incarnation) {
		return nil, nil
	}
	if err := validateVoiceEvent(input.Event); err != nil {
		return nil, err
	}
	switch input.Event.Kind {
	case realtime.EventSessionCreated, realtime.EventSessionUpdated:
		return nil, nil
	case realtime.EventSpeechStarted:
		return r.applySpeechStarted(input.Event)
	case realtime.EventSpeechStopped:
		return r.applySpeechStopped(input.Event)
	case realtime.EventResponseCreated:
		if input.Event.NeedsEpochBinding {
			return r.applyImplicitResponseCreated(input.Event)
		}
		return r.applyExplicitResponseCreated(input.Event)
	case realtime.EventTranscriptDelta:
		return r.applyTranscriptDelta(input.Event)
	case realtime.EventTranscriptionDelta:
		return r.applyInputTranscriptionDelta(input.Event)
	case realtime.EventTranscriptionCompleted:
		return r.applyInputTranscriptionCompleted(input.Event)
	case realtime.EventAudioDelta:
		if !r.currentResponseEvent(input.Event) || r.state.Response != ResponseStreaming {
			return nil, nil
		}
		return nil, nil
	case realtime.EventAudioDone:
		return r.applyAudioDone(input.Event)
	case realtime.EventTranscriptDone:
		return r.applyTranscriptDone(input.Event)
	case realtime.EventFunctionCallDone:
		return r.applyFunctionCallDone(input.Event)
	case realtime.EventResponseDone:
		return r.applyResponseDone(input.Event)
	case realtime.EventError:
		return r.applyProviderError(input.Event)
	default:
		return nil, fmt.Errorf("%w: voice event kind %d", ErrUnsupportedInput, input.Event.Kind)
	}
}

func (r *Reactor) applySpeechStarted(event realtime.Event) ([]Effect, error) {
	if r.state.UserSpeaking {
		if r.activeTurn != nil && r.activeTurn.key.ID == event.ItemID {
			return nil, nil
		}
		if err := r.beginInputTurn(event.ItemID); err != nil {
			return nil, err
		}
		return nil, nil
	}

	if err := r.beginInputTurn(event.ItemID); err != nil {
		return nil, err
	}
	epoch := r.fence.Advance()
	r.state.UserSpeaking = true
	effects := []Effect{{FlushBefore: epoch}}
	if r.state.Response == ResponsePending || r.state.Response == ResponseStreaming {
		r.activeResponseEpoch = epoch
		cancel := realtime.Command{
			Kind:       realtime.CommandCancelResponse,
			Sequence:   r.allocateSequence(),
			Epoch:      epoch,
			ResponseID: r.activeResponseID,
		}
		r.pendingCancelSequence = cancel.Sequence
		if r.activeResponseID == "" {
			r.state.Response = ResponseTerminal
		} else {
			r.state.Response = ResponseCancelRequested
		}
		effects = append(effects, Effect{VoiceCommand: &cancel})
	} else if r.state.Response == ResponseCancelRequested {
		// The existing cancellation completion remains authoritative, while the
		// newer speech edge becomes the fence for every late provider event.
		r.activeResponseEpoch = epoch
	}

	started := protocol.InputAudioBufferSpeechStartedEvent{
		Type:    "input_audio_buffer.speech_started",
		EventID: r.providerOrSynthEventID(event.ProviderEventID),
		ItemID:  event.ItemID,
	}
	effects = append(effects, Effect{ClientEvent: started})
	return effects, nil
}

func (r *Reactor) applySpeechStopped(event realtime.Event) ([]Effect, error) {
	if !r.state.UserSpeaking {
		return nil, nil
	}
	r.state.UserSpeaking = false
	stopped := protocol.InputAudioBufferSpeechStoppedEvent{
		Type:    "input_audio_buffer.speech_stopped",
		EventID: r.providerOrSynthEventID(event.ProviderEventID),
		ItemID:  event.ItemID,
	}
	return []Effect{{ClientEvent: stopped}}, nil
}

func (r *Reactor) applyExplicitResponseCreated(event realtime.Event) ([]Effect, error) {
	if event.Epoch != r.fence.Current() {
		return nil, nil
	}
	if r.state.Response != ResponsePending {
		return nil, fmt.Errorf("%w: response.created from state %d", ErrInvalidTransition, r.state.Response)
	}
	if err := validateDurableID("response ID", event.ResponseID); err != nil {
		return nil, err
	}

	r.state.Response = ResponseStreaming
	r.activeResponseID = event.ResponseID
	r.activeResponseEpoch = event.Epoch
	created := protocol.ResponseCreatedEvent{
		Type:    "response.created",
		EventID: r.providerOrSynthEventID(event.ProviderEventID),
		Response: protocol.ResponsePayload{
			ID:     event.ResponseID,
			Status: event.Status,
		},
	}
	return []Effect{{ClientEvent: created}}, nil
}

func (r *Reactor) applyImplicitResponseCreated(event realtime.Event) ([]Effect, error) {
	if r.state.Response != ResponseIdle && r.state.Response != ResponseTerminal {
		return nil, fmt.Errorf("%w: implicit response.created from state %d", ErrInvalidTransition, r.state.Response)
	}
	if err := validateDurableID("response ID", event.ResponseID); err != nil {
		return nil, err
	}

	epoch := r.fence.Advance()
	r.state.Response = ResponseStreaming
	r.activeResponseID = event.ResponseID
	r.activeResponseEpoch = epoch
	r.terminal = nil
	r.pendingCancelSequence = 0
	bind := realtime.Command{
		Kind:       realtime.CommandBindResponseEpoch,
		Sequence:   r.allocateSequence(),
		Epoch:      epoch,
		ResponseID: event.ResponseID,
	}
	created := protocol.ResponseCreatedEvent{
		Type:    "response.created",
		EventID: r.providerOrSynthEventID(event.ProviderEventID),
		Response: protocol.ResponsePayload{
			ID:     event.ResponseID,
			Status: event.Status,
		},
	}
	return []Effect{
		{VoiceCommand: &bind},
		{ClientEvent: created},
	}, nil
}

func (r *Reactor) applyTranscriptDelta(event realtime.Event) ([]Effect, error) {
	if !r.currentResponseEvent(event) || r.state.Response != ResponseStreaming {
		return nil, nil
	}
	mapped := protocol.ResponseOutputAudioTranscriptDeltaEvent{
		Type:       "response.output_audio_transcript.delta",
		EventID:    r.providerOrSynthEventID(event.ProviderEventID),
		ResponseID: event.ResponseID,
		ItemID:     event.ItemID,
		Delta:      event.Text,
	}
	return []Effect{{ClientEvent: mapped}}, nil
}

func (r *Reactor) applyInputTranscriptionDelta(event realtime.Event) ([]Effect, error) {
	if err := r.observeTranscription(event.ItemID, false); err != nil {
		return nil, err
	}
	mapped := protocol.ConversationItemInputAudioTranscriptionDeltaEvent{
		Type:    "conversation.item.input_audio_transcription.delta",
		EventID: r.providerOrSynthEventID(event.ProviderEventID),
		ItemID:  event.ItemID,
		Delta:   event.Text,
	}
	return []Effect{{ClientEvent: mapped}}, nil
}

func (r *Reactor) applyInputTranscriptionCompleted(event realtime.Event) ([]Effect, error) {
	if err := validateDurableText("user transcript", event.Text); err != nil {
		return nil, err
	}
	if err := validateDurableID("transcript item ID", event.ItemID); err != nil {
		return nil, err
	}
	if r.activeTurn != nil && r.activeTurn.key.ID == event.ItemID && r.activeTurn.transcriptComplete && r.activeTurn.transcript == event.Text {
		mapped := protocol.ConversationItemInputAudioTranscriptionCompletedEvent{
			Type: "conversation.item.input_audio_transcription.completed", EventID: r.providerOrSynthEventID(event.ProviderEventID),
			ItemID: event.ItemID, Transcript: event.Text,
		}
		return []Effect{{ClientEvent: mapped}}, nil
	}
	if err := r.observeTranscription(event.ItemID, true); err != nil {
		return nil, err
	}
	mapped := protocol.ConversationItemInputAudioTranscriptionCompletedEvent{
		Type:       "conversation.item.input_audio_transcription.completed",
		EventID:    r.providerOrSynthEventID(event.ProviderEventID),
		ItemID:     event.ItemID,
		Transcript: event.Text,
	}
	if r.activeTurn == nil {
		return []Effect{{ClientEvent: mapped}}, nil
	}
	r.activeTurn.transcript = event.Text
	r.transcriptSequence++
	r.activeTurn.durableSequence = r.transcriptSequence
	transcript := TranscriptRequestedEffect{
		Turn: r.activeTurn.key, Sequence: r.transcriptSequence, Role: "user", FinalText: event.Text,
	}
	return []Effect{{ClientEvent: mapped, Transcript: &transcript}}, nil
}

func (r *Reactor) applyAudioDone(event realtime.Event) ([]Effect, error) {
	if !r.currentResponseEvent(event) || r.state.Response != ResponseStreaming {
		return nil, nil
	}
	mapped := protocol.ResponseOutputAudioDoneEvent{
		Type:       "response.output_audio.done",
		EventID:    r.providerOrSynthEventID(event.ProviderEventID),
		ResponseID: event.ResponseID,
		ItemID:     event.ItemID,
	}
	return []Effect{{ClientEvent: mapped}}, nil
}

func (r *Reactor) applyTranscriptDone(event realtime.Event) ([]Effect, error) {
	if !r.currentResponseEvent(event) || r.state.Response != ResponseStreaming {
		return nil, nil
	}
	if err := validateDurableText("assistant transcript", event.Text); err != nil {
		return nil, err
	}
	if err := validateDurableID("transcript item ID", event.ItemID); err != nil {
		return nil, err
	}
	if err := validateDurableID("transcript response ID", event.ResponseID); err != nil {
		return nil, err
	}
	mapped := protocol.ResponseOutputAudioTranscriptDoneEvent{
		Type:       "response.output_audio_transcript.done",
		EventID:    r.providerOrSynthEventID(event.ProviderEventID),
		ResponseID: event.ResponseID,
		ItemID:     event.ItemID,
		Transcript: event.Text,
	}
	turn := r.activeTurnKey()
	if turn == nil {
		turn = cloneTurn(r.state.CommittedTurn)
	}
	if turn == nil {
		return []Effect{{ClientEvent: mapped}}, nil
	}
	key := event.ResponseID + "\x00" + event.ItemID
	if finalized, exists := r.finalizedAssistantTexts[key]; exists {
		if finalized.text == event.Text {
			return []Effect{{ClientEvent: mapped}}, nil
		}
		turn = cloneTurn(&finalized.turn)
	}
	retained := len(key) + len(event.Text) + len(turn.ID)
	if retained > PendingBackgroundToolBytes {
		return nil, ErrPendingToolLimit
	}
	r.transcriptSequence++
	transcript := TranscriptRequestedEffect{
		Turn: *turn, Sequence: r.transcriptSequence, Role: "assistant", FinalText: event.Text,
	}
	if previous, exists := r.finalizedAssistantTexts[key]; exists {
		r.finalizedAssistantBytes -= len(key) + len(previous.text) + len(previous.turn.ID)
		delete(r.finalizedAssistantTexts, key)
		for index, orderedKey := range r.finalizedAssistantOrder {
			if orderedKey == key {
				r.finalizedAssistantOrder = append(r.finalizedAssistantOrder[:index], r.finalizedAssistantOrder[index+1:]...)
				break
			}
		}
	}
	for len(r.finalizedAssistantOrder) >= PendingBackgroundToolLimit || retained > PendingBackgroundToolBytes-r.finalizedAssistantBytes {
		if len(r.finalizedAssistantOrder) == 0 {
			return nil, ErrPendingToolLimit
		}
		oldKey := r.finalizedAssistantOrder[0]
		r.finalizedAssistantOrder = r.finalizedAssistantOrder[1:]
		if previous, exists := r.finalizedAssistantTexts[oldKey]; exists {
			r.finalizedAssistantBytes -= len(oldKey) + len(previous.text) + len(previous.turn.ID)
			delete(r.finalizedAssistantTexts, oldKey)
		}
	}
	r.finalizedAssistantTexts[key] = finalizedTranscript{turn: *turn, text: event.Text}
	r.finalizedAssistantOrder = append(r.finalizedAssistantOrder, key)
	r.finalizedAssistantBytes += retained
	return []Effect{{ClientEvent: mapped, Transcript: &transcript}}, nil
}

func (r *Reactor) applyFunctionCallDone(event realtime.Event) ([]Effect, error) {
	if !r.currentResponseEvent(event) || r.state.Response != ResponseStreaming {
		return nil, nil
	}
	if event.CallID == "" || event.Name == "" || len(event.Arguments) == 0 || !json.Valid(event.Arguments) {
		return nil, errors.New("function call requires call ID, name, and valid arguments")
	}
	isReserved := isReservedHandoffTool(event.Name)
	if isReserved {
		for name, value := range map[string]string{
			"response ID":  event.ResponseID,
			"call ID":      event.CallID,
			"tool item ID": event.ItemID,
			"tool name":    event.Name,
		} {
			if err := validateDurableID(name, value); err != nil {
				return nil, err
			}
		}
		if len(event.Arguments) > DurableArgumentsBytes {
			return nil, ErrPendingToolLimit
		}
	}
	existingCall := r.handoffCalls[event.CallID]
	committed := r.committedTurnForCurrentRevision()
	isPendingStart := event.Name == StartBackgroundTaskTool && committed == nil
	if isPendingStart {
		if len(r.pendingTools) >= PendingBackgroundToolLimit ||
			len(event.Arguments) > PendingBackgroundToolBytes-r.pendingToolBytes {
			return nil, ErrPendingToolLimit
		}
	}

	mapped := protocol.ResponseFunctionCallArgumentsDoneEvent{
		Type:       "response.function_call_arguments.done",
		EventID:    r.providerOrSynthEventID(event.ProviderEventID),
		ResponseID: event.ResponseID,
		ItemID:     event.ItemID,
		CallID:     event.CallID,
		Name:       event.Name,
		Arguments:  string(event.Arguments),
	}
	effects := []Effect{{ClientEvent: mapped}}
	if !isReserved {
		return effects, nil
	}
	if existingCall != nil {
		return effects, nil
	}
	if len(r.handoffCalls) >= PendingHandoffResultLimit {
		return nil, ErrPendingToolLimit
	}
	correlationTurn := identity.TurnKey{}
	if committed != nil {
		correlationTurn = *committed
	} else if active := r.activeTurnKey(); active != nil {
		correlationTurn = *active
	}
	callRetained := len(event.ResponseID) + len(event.CallID) + len(correlationTurn.ID)
	if callRetained > PendingHandoffResultBytes-r.handoffCallBytes {
		return nil, ErrPendingToolLimit
	}
	r.handoffCalls[event.CallID] = &handoffCall{
		originEpoch: event.Epoch, turn: correlationTurn, responseID: event.ResponseID, callID: event.CallID, bytes: callRetained,
	}
	r.handoffCallBytes += callRetained
	r.handoffCallOrder = append(r.handoffCallOrder, event.CallID)
	if event.Name != StartBackgroundTaskTool {
		handoffEffect := HandoffRequestedEffect{
			OriginEpoch: event.Epoch, ResponseID: event.ResponseID, CallID: event.CallID, ToolName: event.Name,
			Arguments: cloneRaw(event.Arguments), Turn: correlationTurn,
		}
		if committed == nil {
			handoffEffect.RejectReason = "missing_committed_turn"
		} else {
			handoffEffect.Transcript = r.committedTranscript
		}
		effects[0].Handoff = &handoffEffect
		return effects, nil
	}
	if committed != nil {
		async, err := makeToolAsyncEffect(pendingTool{
			responseID: event.ResponseID,
			callID:     event.CallID,
			name:       event.Name,
			arguments:  cloneRaw(event.Arguments),
		}, *committed, "")
		if err != nil {
			return nil, err
		}
		async.Sequence = r.allocateSequence()
		handoffEffect := HandoffRequestedEffect{
			OriginEpoch: event.Epoch, ResponseID: event.ResponseID, CallID: event.CallID, ToolName: event.Name,
			Arguments: cloneRaw(event.Arguments), Turn: *committed, Transcript: r.committedTranscript,
		}
		effects = append(effects, Effect{Async: &async, Handoff: &handoffEffect})
		return effects, nil
	}

	r.pendingTools = append(r.pendingTools, pendingTool{
		responseID: event.ResponseID,
		callID:     event.CallID,
		name:       event.Name,
		arguments:  cloneRaw(event.Arguments),
		turn:       r.activeTurnKey(),
	})
	r.pendingToolBytes += len(event.Arguments)
	return effects, nil
}

func isReservedHandoffTool(name string) bool {
	switch name {
	case StartBackgroundTaskTool, SteerBackgroundTaskTool, CancelBackgroundTaskTool:
		return true
	default:
		return false
	}
}

func (r *Reactor) applyResponseDone(event realtime.Event) ([]Effect, error) {
	if !r.currentResponseEvent(event) {
		return nil, nil
	}
	if r.terminal != nil && r.terminal.responseID == event.ResponseID && r.terminal.epoch == event.Epoch {
		return nil, nil
	}
	if r.state.Response != ResponseStreaming && r.state.Response != ResponseCancelRequested {
		return nil, fmt.Errorf("%w: response.done from state %d", ErrInvalidTransition, r.state.Response)
	}

	r.state.Response = ResponseTerminal
	r.terminal = &terminalMarker{responseID: event.ResponseID, epoch: event.Epoch}
	r.rememberTerminalResponse(event.ResponseID)
	done := protocol.ResponseDoneEvent{
		Type:    "response.done",
		EventID: r.providerOrSynthEventID(event.ProviderEventID),
		Response: protocol.ResponsePayload{
			ID:     event.ResponseID,
			Status: event.Status,
		},
	}
	effects := []Effect{{ClientEvent: done}}
	acceptance, err := r.emitNextReadyHandoffAcceptance()
	if err != nil {
		return nil, err
	}
	return append(effects, acceptance...), nil
}

func (r *Reactor) emitNextReadyHandoffAcceptance() ([]Effect, error) {
	for _, callID := range r.handoffCallOrder {
		call := r.handoffCalls[callID]
		if call == nil || call.completed == nil || call.emitted {
			continue
		}
		if _, terminal := r.terminalResponses[call.responseID]; !terminal {
			continue
		}
		acceptance, err := r.emitHandoffAcceptance(call)
		if err != nil {
			return nil, err
		}
		if len(acceptance) != 0 {
			return acceptance, nil
		}
	}
	return nil, nil
}

func (r *Reactor) rememberTerminalResponse(responseID string) {
	for _, call := range r.handoffCalls {
		if call != nil && call.responseID == responseID {
			r.terminalResponses[responseID] = struct{}{}
			return
		}
	}
}

func (r *Reactor) applyVoiceCommandCompleted(input VoiceCommandCompletedInput) ([]Effect, error) {
	if input.Kind == realtime.CommandSessionUpdate {
		pending, ok := r.pendingSessionUpdates[input.Sequence]
		if !ok {
			return nil, nil
		}
		delete(r.pendingSessionUpdates, input.Sequence)
		r.pendingSessionUpdateBytes -= pending.retainedBytes
		updated := protocol.SessionUpdatedEvent{
			Type:    "session.updated",
			EventID: r.providerOrSynthEventID(""),
			Session: cloneRaw(pending.session),
		}
		return []Effect{{ClientEvent: updated}}, nil
	}
	if input.Kind != realtime.CommandCancelResponse ||
		input.Sequence == 0 ||
		input.Sequence != r.pendingCancelSequence {
		return nil, nil
	}
	r.pendingCancelSequence = 0
	if r.activeResponseID == "" || r.state.Response == ResponseTerminal {
		return nil, nil
	}
	if r.state.Response != ResponseCancelRequested {
		return nil, nil
	}
	return r.synthesizeResponseDone("cancelled")
}

func (r *Reactor) clearPendingSessionUpdates() {
	clear(r.pendingSessionUpdates)
	r.pendingSessionUpdateBytes = 0
}

func (r *Reactor) synthesizeResponseDone(status string) ([]Effect, error) {
	if r.activeResponseID == "" {
		return nil, nil
	}
	if r.terminal != nil && r.terminal.responseID == r.activeResponseID && r.terminal.epoch == r.activeResponseEpoch {
		return nil, nil
	}
	r.state.Response = ResponseTerminal
	r.terminal = &terminalMarker{responseID: r.activeResponseID, epoch: r.activeResponseEpoch}
	r.rememberTerminalResponse(r.activeResponseID)
	done := protocol.ResponseDoneEvent{
		Type:    "response.done",
		EventID: r.providerOrSynthEventID(""),
		Response: protocol.ResponsePayload{
			ID:     r.activeResponseID,
			Status: status,
		},
	}
	effects := []Effect{{ClientEvent: done}}
	acceptance, err := r.emitNextReadyHandoffAcceptance()
	if err != nil {
		return nil, err
	}
	return append(effects, acceptance...), nil
}

func (r *Reactor) applyProviderError(event realtime.Event) ([]Effect, error) {
	if event.Epoch != 0 && event.Epoch != r.fence.Current() {
		return nil, nil
	}
	if event.ResponseID != "" && !r.currentResponseEvent(event) {
		return nil, nil
	}
	message := event.Reason
	mapped := protocol.ErrorEvent{
		Type:    "error",
		EventID: r.providerOrSynthEventID(event.ProviderEventID),
		Error: protocol.ProtocolError{
			Type:    "server_error",
			Code:    "provider_error",
			Message: message,
		},
	}
	effects := []Effect{{ClientEvent: mapped}}
	if r.state.Response == ResponsePending && r.activeResponseID == "" {
		r.state.Response = ResponseTerminal
		acceptance, err := r.emitNextReadyHandoffAcceptance()
		if err != nil {
			return nil, err
		}
		effects = append(effects, acceptance...)
	}
	if r.activeResponseID != "" &&
		(r.state.Response == ResponseStreaming || r.state.Response == ResponseCancelRequested) {
		terminal, err := r.synthesizeResponseDone("failed")
		if err != nil {
			return nil, err
		}
		effects = append(effects, terminal...)
	}
	return effects, nil
}

func (r *Reactor) applyFailed(input FailedInput) ([]Effect, error) {
	source := defaultString(input.Source, "session")
	message := source + " failed"
	if input.Err != nil {
		message += ": " + input.Err.Error()
	}
	mapped := protocol.ErrorEvent{
		Type:    "error",
		EventID: r.providerOrSynthEventID(""),
		Error: protocol.ProtocolError{
			Type:    "server_error",
			Code:    "session_failed",
			Message: message,
		},
	}
	effects := []Effect{{ClientEvent: mapped}}
	if r.activeResponseID != "" &&
		(r.state.Response == ResponseStreaming || r.state.Response == ResponseCancelRequested) {
		terminal, err := r.synthesizeResponseDone("failed")
		if err != nil {
			return nil, err
		}
		effects = append(effects, terminal...)
	}
	effects = append(effects, Effect{Close: &CloseEffect{Code: 1011, Reason: message}})
	return effects, nil
}

func (r *Reactor) currentResponseEvent(event realtime.Event) bool {
	return event.Epoch != 0 &&
		event.Epoch == r.fence.Current() &&
		event.Epoch == r.activeResponseEpoch &&
		event.ResponseID != "" &&
		event.ResponseID == r.activeResponseID
}

func (r *Reactor) acceptIncarnation(incarnation identity.Incarnation) bool {
	return incarnation != 0 && incarnation == r.ids.Incarnation
}

func (r *Reactor) allocateSequence() uint64 {
	sequence := r.nextSeq
	r.nextSeq++
	return sequence
}

func (r *Reactor) providerOrSynthEventID(providerID string) string {
	if providerID != "" {
		return providerID
	}
	r.synthEventSequence++
	return fmt.Sprintf("session-%s-%06d", r.ids.SessionID, r.synthEventSequence)
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func validateDurableID(name, value string) error {
	if strings.TrimSpace(value) == "" || !utf8.ValidString(value) || len(value) > DurableIdentifierBytes {
		return fmt.Errorf("%s exceeds durable-work bounds", name)
	}
	return nil
}

func validateDurableText(name, value string) error {
	if !utf8.ValidString(value) || len(value) > DurableTranscriptBytes {
		return fmt.Errorf("%s exceeds durable-work bounds", name)
	}
	return nil
}

func validateVoiceEvent(event realtime.Event) error {
	require := func(value, field string) error {
		if value == "" {
			return fmt.Errorf("%w: event kind %d requires %s", ErrInvalidVoiceEvent, event.Kind, field)
		}
		return nil
	}
	requireResponse := func() error {
		return require(event.ResponseID, "response ID")
	}
	requireItem := func() error {
		return require(event.ItemID, "item ID")
	}

	switch event.Kind {
	case realtime.EventSessionCreated, realtime.EventSessionUpdated:
		return nil
	case realtime.EventSpeechStarted, realtime.EventSpeechStopped,
		realtime.EventTranscriptionDelta, realtime.EventTranscriptionCompleted:
		return requireItem()
	case realtime.EventResponseCreated:
		if err := requireResponse(); err != nil {
			return err
		}
		return require(event.Status, "status")
	case realtime.EventAudioDelta:
		if err := requireResponse(); err != nil {
			return err
		}
		if err := requireItem(); err != nil {
			return err
		}
		if len(event.Audio) == 0 {
			return fmt.Errorf("%w: event kind %d requires audio", ErrInvalidVoiceEvent, event.Kind)
		}
		return nil
	case realtime.EventAudioDone, realtime.EventTranscriptDelta, realtime.EventTranscriptDone:
		if err := requireResponse(); err != nil {
			return err
		}
		return requireItem()
	case realtime.EventFunctionCallDone:
		if err := requireResponse(); err != nil {
			return err
		}
		if err := requireItem(); err != nil {
			return err
		}
		if err := require(event.CallID, "call ID"); err != nil {
			return err
		}
		if err := require(event.Name, "function name"); err != nil {
			return err
		}
		if len(event.Arguments) == 0 || !json.Valid(event.Arguments) {
			return fmt.Errorf("%w: event kind %d requires valid arguments", ErrInvalidVoiceEvent, event.Kind)
		}
		return nil
	case realtime.EventResponseDone:
		if err := requireResponse(); err != nil {
			return err
		}
		return require(event.Status, "status")
	case realtime.EventError:
		return require(event.Reason, "reason")
	default:
		return nil
	}
}

func normalizeResponseConfig(config *protocol.ResponseConfig) (realtime.ResponseConfig, error) {
	if config == nil {
		return realtime.ResponseConfig{}, nil
	}
	if config.Conversation != "" {
		return realtime.ResponseConfig{}, fmt.Errorf("%w: response conversation", ErrUnsupportedField)
	}
	if len(config.OutputModalities) > 0 {
		return realtime.ResponseConfig{}, fmt.Errorf("%w: response output modalities", ErrUnsupportedField)
	}
	if len(config.Tools) > 0 {
		return realtime.ResponseConfig{}, fmt.Errorf("%w: response tools", ErrUnsupportedField)
	}
	if err := validateOptionalJSON(config.ToolChoice, "response tool choice"); err != nil {
		return realtime.ResponseConfig{}, err
	}
	if err := validateOptionalJSON(config.Input, "response input"); err != nil {
		return realtime.ResponseConfig{}, err
	}

	var metadata map[string]json.RawMessage
	if len(config.Metadata) > 0 {
		if err := json.Unmarshal(config.Metadata, &metadata); err != nil {
			return realtime.ResponseConfig{}, fmt.Errorf("response metadata must be an object: %w", err)
		}
		if metadata == nil {
			return realtime.ResponseConfig{}, errors.New("response metadata must be an object")
		}
		for key, value := range metadata {
			trimmed := bytes.TrimSpace(value)
			var decoded string
			if len(trimmed) == 0 || trimmed[0] != '"' || json.Unmarshal(trimmed, &decoded) != nil {
				return realtime.ResponseConfig{}, fmt.Errorf("response metadata value %q must be a JSON string", key)
			}
			metadata[key] = cloneRaw(value)
		}
	}

	return realtime.ResponseConfig{
		Instructions: config.Instructions,
		ToolChoice:   cloneRaw(config.ToolChoice),
		Metadata:     metadata,
		Input:        cloneRaw(config.Input),
	}, nil
}

func validateOptionalJSON(raw json.RawMessage, label string) error {
	if len(raw) > 0 && !json.Valid(raw) {
		return fmt.Errorf("%s is invalid JSON", label)
	}
	return nil
}

func cloneRaw(raw json.RawMessage) json.RawMessage {
	return append(json.RawMessage(nil), raw...)
}

type sessionToolDTO struct {
	Type        string          `json:"type"`
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

func normalizeSessionUpdate(update protocol.SessionUpdate) (realtime.SessionConfig, error) {
	if update.Type != "realtime" {
		return realtime.SessionConfig{}, fmt.Errorf("session type must be realtime, got %q", update.Type)
	}
	if update.Model != "" {
		return realtime.SessionConfig{}, fmt.Errorf("%w: session model", ErrUnsupportedField)
	}
	if len(update.OutputModalities) > 0 {
		return realtime.SessionConfig{}, fmt.Errorf("%w: session output modalities", ErrUnsupportedField)
	}
	if len(update.Metadata) > 0 {
		return realtime.SessionConfig{}, fmt.Errorf("%w: session metadata", ErrUnsupportedField)
	}
	if update.Audio != nil {
		if update.Audio.Input != nil {
			if len(update.Audio.Input.Transcription) > 0 {
				return realtime.SessionConfig{}, fmt.Errorf("%w: input transcription", ErrUnsupportedField)
			}
			if len(update.Audio.Input.NoiseReduction) > 0 {
				return realtime.SessionConfig{}, fmt.Errorf("%w: input noise reduction", ErrUnsupportedField)
			}
		}
		if update.Audio.Output != nil && update.Audio.Output.Speed != 0 {
			return realtime.SessionConfig{}, fmt.Errorf("%w: output speed", ErrUnsupportedField)
		}
	}
	if err := validateOptionalJSON(update.ToolChoice, "session tool choice"); err != nil {
		return realtime.SessionConfig{}, err
	}

	var tools []realtime.Tool
	if len(update.Tools) > 0 {
		var decoded []sessionToolDTO
		decoder := json.NewDecoder(bytes.NewReader(update.Tools))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&decoded); err != nil {
			return realtime.SessionConfig{}, fmt.Errorf("session tools must be an array: %w", err)
		}
		if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
			return realtime.SessionConfig{}, errors.New("session tools must contain exactly one JSON value")
		}
		if decoded == nil {
			return realtime.SessionConfig{}, errors.New("session tools must be an array")
		}
		tools = make([]realtime.Tool, len(decoded))
		for index, tool := range decoded {
			if tool.Type != "function" {
				return realtime.SessionConfig{}, fmt.Errorf("%w: session tool %d type must be function", ErrInvalidSessionTool, index)
			}
			if strings.TrimSpace(tool.Name) == "" {
				return realtime.SessionConfig{}, fmt.Errorf("%w: session tool %d name is required", ErrInvalidSessionTool, index)
			}
			trimmedParameters := bytes.TrimSpace(tool.Parameters)
			if len(trimmedParameters) == 0 || trimmedParameters[0] != '{' || !json.Valid(trimmedParameters) {
				return realtime.SessionConfig{}, fmt.Errorf("%w: session tool %d parameters must be a JSON object", ErrInvalidSessionTool, index)
			}
			tools[index] = realtime.Tool{
				Type:        tool.Type,
				Name:        tool.Name,
				Description: tool.Description,
				Parameters:  cloneRaw(tool.Parameters),
			}
		}
	}

	config := realtime.SessionConfig{
		Instructions: update.Instructions,
		Tools:        tools,
		ToolChoice:   cloneRaw(update.ToolChoice),
	}
	if update.Audio != nil {
		if update.Audio.Input != nil {
			format, err := normalizeAudioFormat(update.Audio.Input.Format)
			if err != nil {
				return realtime.SessionConfig{}, fmt.Errorf("input audio format: %w", err)
			}
			if err := validateOptionalJSON(update.Audio.Input.TurnDetection, "turn detection"); err != nil {
				return realtime.SessionConfig{}, err
			}
			config.InputFormat = format
			config.TurnDetection = cloneRaw(update.Audio.Input.TurnDetection)
		}
		if update.Audio.Output != nil {
			format, err := normalizeAudioFormat(update.Audio.Output.Format)
			if err != nil {
				return realtime.SessionConfig{}, fmt.Errorf("output audio format: %w", err)
			}
			config.OutputFormat = format
			config.Voice = update.Audio.Output.Voice
		}
	}
	return config, nil
}

func normalizeAudioFormat(format *protocol.AudioFormat) (realtime.AudioFormat, error) {
	if format == nil {
		return realtime.AudioFormat{}, nil
	}
	if format.Type != "audio/pcm" || format.Rate != 24_000 {
		return realtime.AudioFormat{}, errors.New("must be mono PCM at 24 kHz")
	}
	return realtime.AudioFormat{Encoding: "pcm16le", SampleRate: 24_000, Channels: 1}, nil
}

type turnPayload struct {
	Turn     *identity.TurnKey `json:"turn"`
	ID       string            `json:"id"`
	Revision uint32            `json:"revision"`
}

func decodeTurnPayload(raw json.RawMessage) (identity.TurnKey, error) {
	if len(raw) == 0 || !json.Valid(raw) {
		return identity.TurnKey{}, errors.New("turn payload must be valid JSON")
	}
	var decoded turnPayload
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return identity.TurnKey{}, fmt.Errorf("decode turn payload: %w", err)
	}
	turn := identity.TurnKey{ID: decoded.ID, Revision: decoded.Revision}
	if decoded.Turn != nil {
		turn = *decoded.Turn
	}
	if turn.ID == "" {
		return identity.TurnKey{}, errors.New("turn payload requires an ID")
	}
	if turn.Revision == 0 {
		return identity.TurnKey{}, errors.New("turn payload requires a nonzero revision")
	}
	return turn, nil
}

func (r *Reactor) committedTurnForCurrentRevision() *identity.TurnKey {
	committed := r.state.CommittedTurn
	if committed == nil || r.activeTurn == nil || !r.activeTurn.committed {
		return nil
	}
	if r.activeTurn.key.Revision != r.inputTurnRevision {
		return nil
	}
	if r.activeTurn.key != *committed {
		return nil
	}
	return cloneTurn(committed)
}

func (r *Reactor) nextInputTurnRevision() (uint32, error) {
	if r.inputTurnRevision == ^uint32(0) {
		return 0, ErrTurnRevisionExhausted
	}
	r.inputTurnRevision++
	return r.inputTurnRevision, nil
}

func (r *Reactor) beginInputTurn(id string) error {
	revision, err := r.nextInputTurnRevision()
	if err != nil {
		return err
	}
	if id == "" {
		r.activeTurn = nil
		return nil
	}
	r.activeTurn = &activeInputTurn{key: identity.TurnKey{ID: id, Revision: revision}}
	return nil
}

func (r *Reactor) observeTranscription(id string, complete bool) error {
	needsRevision := r.activeTurn == nil ||
		r.activeTurn.key.ID != id ||
		r.activeTurn.committed ||
		r.activeTurn.transcriptComplete
	if needsRevision {
		revision, err := r.nextInputTurnRevision()
		if err != nil {
			return err
		}
		if id == "" {
			r.activeTurn = nil
			return nil
		}
		r.activeTurn = &activeInputTurn{
			key:                identity.TurnKey{ID: id, Revision: revision},
			transcriptComplete: complete,
		}
		return nil
	}
	r.activeTurn.committed = false
	r.activeTurn.transcriptComplete = complete
	return nil
}

func (r *Reactor) activeTurnKey() *identity.TurnKey {
	if r.activeTurn == nil {
		return nil
	}
	return cloneTurn(&r.activeTurn.key)
}

type toolAsyncPayload struct {
	ResponseID string           `json:"response_id"`
	CallID     string           `json:"call_id"`
	Name       string           `json:"name"`
	Arguments  json.RawMessage  `json:"arguments"`
	Turn       identity.TurnKey `json:"turn"`
	Reason     string           `json:"reason,omitempty"`
}

func makeToolAsyncEffect(tool pendingTool, turn identity.TurnKey, reason string) (AsyncEffect, error) {
	payload, err := json.Marshal(toolAsyncPayload{
		ResponseID: tool.responseID,
		CallID:     tool.callID,
		Name:       tool.name,
		Arguments:  cloneRaw(tool.arguments),
		Turn:       turn,
		Reason:     reason,
	})
	if err != nil {
		return AsyncEffect{}, fmt.Errorf("encode pending tool effect: %w", err)
	}
	kind := AsyncKindStartBackgroundTask
	if reason != "" {
		kind = AsyncKindStartBackgroundTaskRejected
	}
	return AsyncEffect{Kind: kind, Payload: cloneRaw(payload)}, nil
}

func (r *Reactor) preparePendingToolEffects(turn identity.TurnKey) ([]AsyncEffect, error) {
	effects := make([]AsyncEffect, 0, len(r.pendingTools))
	for _, tool := range r.pendingTools {
		reason := r.pendingToolRejectionReason(tool, turn)
		effectTurn := turn
		if reason != "" && tool.turn != nil {
			effectTurn = *tool.turn
		}
		effect, err := makeToolAsyncEffect(tool, effectTurn, reason)
		if err != nil {
			return nil, err
		}
		effects = append(effects, effect)
	}
	return effects, nil
}

func (r *Reactor) pendingToolRejectionReason(tool pendingTool, committed identity.TurnKey) string {
	if tool.turn == nil {
		return "missing_active_turn"
	}
	if r.activeTurn == nil || r.activeTurn.key.ID != tool.turn.ID {
		return "superseded_turn"
	}
	if tool.turn.Revision != r.inputTurnRevision {
		return "superseded_turn_revision"
	}
	if r.activeTurn.key.Revision != tool.turn.Revision {
		return "superseded_turn_revision"
	}
	if committed.ID != tool.turn.ID {
		return "superseded_turn"
	}
	if committed.Revision != tool.turn.Revision {
		return "superseded_turn_revision"
	}
	return ""
}
