// Package ws exposes the downstream realtime WebSocket transport.
package ws

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"reflect"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/protocol"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/coder/websocket"
)

const (
	defaultMaxFrameBytes = int64(1 << 20)
	defaultMaxJSONDepth  = 32
	defaultMaxAudioBytes = 1 << 20
	defaultReadTimeout   = 30 * time.Second
	defaultWriteTimeout  = 10 * time.Second
)

type Options struct {
	MaxFrameBytes int64
	MaxJSONDepth  int
	MaxAudioBytes int
	ReadTimeout   time.Duration
	WriteTimeout  time.Duration
}

type Admission interface {
	Admit(context.Context, *http.Request) (session.Subject, func(), error)
}

type AdmissionFunc func(context.Context, *http.Request) (session.Subject, func(), error)

func (f AdmissionFunc) Admit(ctx context.Context, request *http.Request) (session.Subject, func(), error) {
	return f(ctx, request)
}

type handler struct {
	opts      Options
	limits    protocol.Limits
	manager   *session.Manager
	admission Admission
	err       error
}

func NewHandler(opts Options, manager *session.Manager, admission Admission) http.Handler {
	normalized, limits, err := normalizeOptions(opts)
	if manager == nil {
		err = errors.New("session manager is required")
	}
	if isNilAdmission(admission) {
		err = errors.New("admission is required")
	}
	return &handler{opts: normalized, limits: limits, manager: manager, admission: admission, err: err}
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/v1/realtime/capabilities":
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}
		if h.err != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(protocol.Capabilities(h.limits))
	case "/v1/realtime":
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}
		if h.err != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		h.serveRealtime(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (h *handler) serveRealtime(w http.ResponseWriter, r *http.Request) {
	subject, release, err := h.admission.Admit(r.Context(), r)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}
	if release == nil {
		release = func() {}
	}
	defer release()

	sessionCtx, cancel := context.WithCancel(r.Context())
	defer cancel()
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	client := newClientConn(conn, h.limits, h.opts.ReadTimeout, h.opts.WriteTimeout)
	runErr := h.manager.Run(sessionCtx, session.OpenRequest{
		Subject:        subject,
		RequestedModel: r.URL.Query().Get("model"),
	}, client)
	_ = client.finish(runErr)
}

func normalizeOptions(opts Options) (Options, protocol.Limits, error) {
	if opts.MaxFrameBytes < 0 || opts.MaxJSONDepth < 0 || opts.MaxAudioBytes < 0 || opts.ReadTimeout < 0 || opts.WriteTimeout < 0 {
		return opts, protocol.Limits{}, errors.New("websocket options must not be negative")
	}
	if opts.MaxFrameBytes == 0 {
		opts.MaxFrameBytes = defaultMaxFrameBytes
	}
	if opts.MaxFrameBytes > math.MaxInt {
		return opts, protocol.Limits{}, errors.New("maximum frame bytes exceed platform integer size")
	}
	if opts.MaxJSONDepth == 0 {
		opts.MaxJSONDepth = defaultMaxJSONDepth
	}
	if opts.MaxAudioBytes == 0 {
		opts.MaxAudioBytes = defaultMaxAudioBytes
	}
	if opts.ReadTimeout == 0 {
		opts.ReadTimeout = defaultReadTimeout
	}
	if opts.WriteTimeout == 0 {
		opts.WriteTimeout = defaultWriteTimeout
	}
	limits := protocol.Limits{MaxFrameBytes: int(opts.MaxFrameBytes), MaxJSONDepth: opts.MaxJSONDepth, MaxAudioBytes: opts.MaxAudioBytes}
	return opts, limits, nil
}

func isNilAdmission(value Admission) bool {
	if value == nil {
		return true
	}
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}
