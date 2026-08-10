// Package ws exposes the downstream realtime WebSocket transport.
package ws

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/admission"
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
	MaxFrameBytes         int64
	MaxJSONDepth          int
	MaxAudioBytes         int
	ReadTimeout           time.Duration
	WriteTimeout          time.Duration
	CanaryRuntimeIdentity RuntimeIdentity
}

type RuntimeIdentity struct {
	ConfigHash   string
	Region       string
	GatewayImage string
}

func (identity RuntimeIdentity) Validate() error { return validateRuntimeIdentity(identity) }

type Admission interface {
	Admit(context.Context, *http.Request) (session.Subject, func(), error)
}

type AdmissionFunc func(context.Context, *http.Request) (session.Subject, func(), error)

func (f AdmissionFunc) Admit(ctx context.Context, request *http.Request) (session.Subject, func(), error) {
	return f(ctx, request)
}

type handler struct {
	opts                     Options
	limits                   protocol.Limits
	manager                  *session.Manager
	admission                Admission
	canaryRuntimeIdentity    RuntimeIdentity
	canaryRuntimeIdentityErr error
	err                      error
}

func NewHandler(opts Options, manager *session.Manager, admission Admission) http.Handler {
	normalized, limits, err := normalizeOptions(opts)
	if manager == nil {
		err = errors.New("session manager is required")
	}
	if isNilAdmission(admission) {
		err = errors.New("admission is required")
	}
	identityErr := validateRuntimeIdentity(normalized.CanaryRuntimeIdentity)
	return &handler{opts: normalized, limits: limits, manager: manager, admission: admission, canaryRuntimeIdentity: normalized.CanaryRuntimeIdentity, canaryRuntimeIdentityErr: identityErr, err: err}
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
	w.Header().Set("Cache-Control", "no-store")
	subject, release, err := h.admission.Admit(r.Context(), r)
	if err != nil {
		writeAdmissionError(w, err)
		return
	}
	if release == nil {
		release = func() {}
	}
	defer release()
	if !r.ProtoAtLeast(1, 1) || !validHandshake(r.Header) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	exactOrigin := false
	if _, ok := h.admission.(*admission.Controller); ok {
		exactOrigin = true
	}
	if !exactOrigin && !sameOrigin(r) {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	sessionCtx, cancel := context.WithCancel(r.Context())
	defer cancel()
	acceptOptions := &websocket.AcceptOptions{}
	if exactOrigin {
		acceptOptions.InsecureSkipVerify = true
	}
	if subject.Ephemeral {
		if h.canaryRuntimeIdentityErr != nil {
			http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("X-Live-Config-Hash", h.canaryRuntimeIdentity.ConfigHash)
		w.Header().Set("X-Live-Region", h.canaryRuntimeIdentity.Region)
		w.Header().Set("X-Live-Gateway-Image", h.canaryRuntimeIdentity.GatewayImage)
	}
	conn, err := websocket.Accept(w, r, acceptOptions)
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

func validateRuntimeIdentity(identity RuntimeIdentity) error {
	if !exactConfigHash(identity.ConfigHash) || !exactRegion(identity.Region) || !exactGatewayImage(identity.GatewayImage) {
		return errors.New("complete canonical synthetic canary runtime identity is required")
	}
	return nil
}

func exactConfigHash(value string) bool {
	return len(value) == 67 && strings.HasPrefix(value, "v1:") && lowercaseHex(value[3:])
}

func exactRegion(value string) bool {
	if value == "" || len(value) > 64 || value[0] == '-' || value[len(value)-1] == '-' {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '-' {
			return false
		}
	}
	return strings.Count(value, "-") >= 2
}

func exactGatewayImage(value string) bool {
	const prefix = "ghcr.io/chuanmingliu/openai-gpt-live/gateway@sha256:"
	return len(value) == len(prefix)+64 && strings.HasPrefix(value, prefix) && lowercaseHex(value[len(prefix):])
}

func lowercaseHex(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func sameOrigin(r *http.Request) bool {
	raw := r.Header.Get("Origin")
	if raw == "" {
		return true
	}
	origin, err := url.Parse(raw)
	if err != nil {
		return false
	}
	return strings.EqualFold(origin.Host, r.Host)
}

func writeAdmissionError(w http.ResponseWriter, err error) {
	w.Header().Set("Cache-Control", "no-store")
	status := http.StatusServiceUnavailable
	switch admission.ErrorClass(err) {
	case admission.ClassMalformed:
		status = http.StatusBadRequest
	case admission.ClassUnauthorized:
		status = http.StatusUnauthorized
		w.Header().Set("WWW-Authenticate", `Bearer realm="realtime"`)
	case admission.ClassForbidden:
		status = http.StatusForbidden
	case admission.ClassLimited:
		status = http.StatusTooManyRequests
		w.Header().Set("Retry-After", "1")
	case admission.ClassUnavailable:
		status = http.StatusServiceUnavailable
		w.Header().Set("Retry-After", "1")
	}
	w.WriteHeader(status)
}

func validHandshake(header http.Header) bool {
	if !headerHasToken(header.Values("Connection"), "upgrade") {
		return false
	}
	upgrade := header.Values("Upgrade")
	if len(upgrade) != 1 || !strings.EqualFold(strings.TrimSpace(upgrade[0]), "websocket") {
		return false
	}
	version := header.Values("Sec-WebSocket-Version")
	if len(version) != 1 || version[0] != "13" {
		return false
	}
	keys := header.Values("Sec-WebSocket-Key")
	if len(keys) != 1 || len(keys[0]) > 64 {
		return false
	}
	decoded, err := base64.StdEncoding.DecodeString(keys[0])
	return err == nil && len(decoded) == 16
}
func headerHasToken(values []string, want string) bool {
	if len(values) == 0 || len(values) > 8 {
		return false
	}
	for _, value := range values {
		if len(value) > 512 {
			return false
		}
		for _, token := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(token), want) {
				return true
			}
		}
	}
	return false
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
