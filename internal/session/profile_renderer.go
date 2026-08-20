package session

import (
	"context"
	"errors"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/prompttmpl"
)

var (
	ErrProfileRenderQueueFull = errors.New("profile_render_queue_full")
	ErrProfileRenderClosed    = errors.New("profile_render_closed")
	ErrProfileRenderInvalid   = errors.New("profile_render_invalid")
)

type promptRenderCompletion func(context.Context, PromptRenderCompletedInput) error

type profileRenderWork struct {
	ctx      context.Context
	runtime  *ProfileRuntime
	request  PromptRenderRequestedEffect
	complete promptRenderCompletion
}

// ProfileRenderDispatcher owns a fixed worker set and a bounded FIFO. Dispatch
// never waits for rendering or queue capacity, so provider speech fences remain
// independent from prompt compilation and execution.
type ProfileRenderDispatcher struct {
	engine  prompttmpl.Engine
	limits  prompttmpl.Limits
	ctx     context.Context
	cancel  context.CancelFunc
	queue   chan profileRenderWork
	done    chan struct{}
	workers int

	mu      sync.RWMutex
	closed  bool
	started bool
	wg      sync.WaitGroup
}

func NewProfileRenderDispatcher(engine prompttmpl.Engine, workers, capacity int, limits prompttmpl.Limits) (*ProfileRenderDispatcher, error) {
	validated, err := limits.Validate()
	if err != nil || isNilInterface(engine) || prompttmpl.ValidateEngineIdentity(engine.Version()) != nil || workers <= 0 || capacity <= 0 || workers > 64 || capacity > 1024 {
		return nil, ErrProfileRenderInvalid
	}
	ctx, cancel := context.WithCancel(context.Background())
	dispatcher := &ProfileRenderDispatcher{
		engine: engine, limits: validated, ctx: ctx, cancel: cancel,
		queue: make(chan profileRenderWork, capacity), done: make(chan struct{}), workers: workers,
	}
	return dispatcher, nil
}

// Dispatch owns all request strings before returning. Runtime is session-owned
// immutable state and remains live until the session context is cancelled.
func (dispatcher *ProfileRenderDispatcher) Dispatch(ctx context.Context, runtime *ProfileRuntime, request PromptRenderRequestedEffect, complete promptRenderCompletion) error {
	if dispatcher == nil || ctx == nil || runtime == nil || complete == nil || request.Incarnation == 0 || request.Epoch == 0 ||
		request.InputRevision == 0 || request.Turn.ID == "" || request.Turn.Revision != request.InputRevision ||
		request.SnapshotHash == "" || request.SnapshotHash != runtime.Profile.Snapshot.SnapshotHash ||
		len(request.Transcript) == 0 || len(request.Transcript) > DurableTranscriptBytes || !utf8.ValidString(request.Transcript) {
		return ErrProfileRenderInvalid
	}
	owned := request
	owned.SnapshotHash = strings.Clone(request.SnapshotHash)
	owned.Turn.ID = strings.Clone(request.Turn.ID)
	owned.Transcript = strings.Clone(request.Transcript)
	work := profileRenderWork{ctx: ctx, runtime: runtime, request: owned, complete: complete}

	dispatcher.mu.Lock()
	defer dispatcher.mu.Unlock()
	if dispatcher.closed {
		return ErrProfileRenderClosed
	}
	dispatcher.startLocked()
	select {
	case dispatcher.queue <- work:
		return nil
	default:
		return ErrProfileRenderQueueFull
	}
}

func (dispatcher *ProfileRenderDispatcher) startLocked() {
	if dispatcher.started {
		return
	}
	dispatcher.started = true
	dispatcher.wg.Add(dispatcher.workers)
	for range dispatcher.workers {
		go dispatcher.worker()
	}
	go func() {
		dispatcher.wg.Wait()
		close(dispatcher.done)
	}()
}

func (dispatcher *ProfileRenderDispatcher) worker() {
	defer dispatcher.wg.Done()
	for {
		if dispatcher.ctx.Err() != nil {
			return
		}
		select {
		case <-dispatcher.ctx.Done():
			return
		case work := <-dispatcher.queue:
			dispatcher.render(work)
		}
	}
}

func (dispatcher *ProfileRenderDispatcher) render(work profileRenderWork) {
	result := PromptRenderCompletedInput{
		Incarnation: work.request.Incarnation, SnapshotHash: strings.Clone(work.request.SnapshotHash),
		Epoch: work.request.Epoch, InputRevision: work.request.InputRevision,
		Turn: work.request.Turn,
	}
	ctx, cancel := context.WithCancel(work.ctx)
	stop := context.AfterFunc(dispatcher.ctx, cancel)
	defer func() {
		stop()
		cancel()
		_ = work.complete(work.ctx, result)
	}()
	if err := ctx.Err(); err != nil {
		result.ErrClass = renderErrorClass(ctx, err)
		return
	}
	if work.runtime.Profile.Validate() != nil || work.runtime.Profile.Snapshot.EngineVersion != dispatcher.engine.Version() {
		result.ErrClass = "render_failed"
		return
	}
	compiled, err := dispatcher.engine.Compile(ctx, work.runtime.Profile.Bundle, dispatcher.limits)
	if err != nil || isNilInterface(compiled) || compiled.EngineVersion() != work.runtime.Profile.Snapshot.EngineVersion ||
		dispatcher.engine.Version() != work.runtime.Profile.Snapshot.EngineVersion {
		result.ErrClass = renderErrorClass(ctx, err)
		return
	}
	input := turnRenderContext(work.runtime, work.request.Transcript)
	validated, err := prompttmpl.ValidateContext(work.runtime.Profile.Bundle.Schema, input, "turn_instruction.j2")
	if err != nil {
		result.ErrClass = "render_failed"
		return
	}
	rendered, err := compiled.Render(ctx, "turn_instruction.j2", validated)
	if err != nil {
		result.ErrClass = renderErrorClass(ctx, err)
		return
	}
	if len(rendered) == 0 || len(rendered) > dispatcher.limits.OutputBytes || !utf8.ValidString(rendered) {
		result.ErrClass = "render_limit_exceeded"
		return
	}
	result.Instructions = strings.Clone(rendered)
}

func turnRenderContext(runtime *ProfileRuntime, transcript string) prompttmpl.Context {
	input := make(prompttmpl.Context)
	for name, field := range runtime.Profile.Bundle.Schema.Fields {
		if !profileFieldAllows(field, "turn_instruction.j2") {
			continue
		}
		if field.Source == "finalized_transcript" {
			input[name] = strings.Clone(transcript)
			continue
		}
		if value, ok := runtime.SessionContext[name]; ok {
			input[name] = value
		}
	}
	return input
}

func profileFieldAllows(field prompttmpl.Field, template string) bool {
	if len(field.Templates) == 0 {
		return true
	}
	for _, allowed := range field.Templates {
		if allowed == template {
			return true
		}
	}
	return false
}

func renderErrorClass(ctx context.Context, err error) string {
	if ctx != nil {
		switch ctx.Err() {
		case context.Canceled:
			return "render_cancelled"
		case context.DeadlineExceeded:
			return "render_timeout"
		}
	}
	switch {
	case errors.Is(err, context.Canceled):
		return "render_cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		return "render_timeout"
	case errors.Is(err, prompttmpl.ErrRenderLimit), errors.Is(err, prompttmpl.ErrLimitsInvalid), errors.Is(err, prompttmpl.ErrTemplateInvalid):
		return "render_limit_exceeded"
	default:
		return "render_failed"
	}
}

// Close prevents new admission, cancels active work, and joins all workers.
func (dispatcher *ProfileRenderDispatcher) Close(ctx context.Context) error {
	if dispatcher == nil {
		return nil
	}
	if ctx == nil {
		return context.Canceled
	}
	dispatcher.mu.Lock()
	if !dispatcher.closed {
		dispatcher.closed = true
		dispatcher.cancel()
		if !dispatcher.started {
			dispatcher.started = true
			close(dispatcher.done)
		}
	}
	dispatcher.mu.Unlock()
	select {
	case <-dispatcher.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
