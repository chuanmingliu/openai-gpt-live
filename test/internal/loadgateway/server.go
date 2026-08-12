// Package loadgateway provides a disposable real gateway boundary backed by a
// deterministic identity-preserving realtime provider. It is test-only and
// never dials an external provider.
package loadgateway

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/loadgen"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	transportws "github.com/chuanmingliu/openai-gpt-live/internal/transport/ws"
)

const watermarkFrameBytes = 960

// WatermarkPCM returns one full 20 ms PCM frame carrying a deterministic
// sequence watermark in every four-byte lane. The bytes are synthetic test
// data; they are never persisted or logged.
func WatermarkPCM(sequence uint32) []byte {
	frame := make([]byte, watermarkFrameBytes)
	for offset := 0; offset < len(frame); offset += 4 {
		binary.LittleEndian.PutUint32(frame[offset:offset+4], sequence)
	}
	return frame
}

// WatermarkSequence validates an entire synthetic PCM frame before returning
// its sequence. This catches truncation, reordering, and cross-frame mixing.
func WatermarkSequence(frame []byte) (uint32, bool) {
	if len(frame) != watermarkFrameBytes {
		return 0, false
	}
	sequence := binary.LittleEndian.Uint32(frame[:4])
	for offset := 4; offset < len(frame); offset += 4 {
		if binary.LittleEndian.Uint32(frame[offset:offset+4]) != sequence {
			return 0, false
		}
	}
	return sequence, true
}

type Server struct {
	http      *httptest.Server
	connector *echoConnector
	metrics   *metrics
}

const loadIdentityLifetime = 24 * time.Hour

func Start() *Server {
	return StartWithCancelDelay(0)
}

func StartWithCancelDelay(delay time.Duration) *Server {
	connector := &echoConnector{cancelDelay: delay}
	metrics := newMetrics()
	manager := session.NewManager(session.Config{
		IngressFrames: 8, EgressFrames: 8, ControlEvents: 32, WriterControlEvents: 16,
		IngressBlockTimeout: time.Second, EgressBlockTimeout: time.Second,
	}, connector, &ids{})
	manager.SetMetricsHooks(session.MetricsHooks{
		QueueOverflow:      metrics.overflow,
		AudioTransit:       metrics.transit.Record,
		BargeinSuppression: metrics.bargein.Record,
	})
	handler := transportws.NewHandler(transportws.Options{
		MaxFrameBytes: 64 << 10, MaxAudioBytes: 8 << 10,
		ReadTimeout: 30 * time.Second, WriteTimeout: 5 * time.Second,
	}, manager, transportws.AdmissionFunc(func(context.Context, *http.Request) (session.Subject, func(), error) {
		return session.Subject{TenantID: "load", UserID: "load", Scopes: map[string]struct{}{"realtime": {}}, ExpiresAt: time.Now().Add(loadIdentityLifetime)}, func() {}, nil
	}))
	return &Server{http: httptest.NewServer(handler), connector: connector, metrics: metrics}
}

func (s *Server) URL() string { return "ws" + strings.TrimPrefix(s.http.URL, "http") + "/v1/realtime" }

func (s *Server) Snapshot() loadgen.GatewayTelemetry {
	return loadgen.GatewayTelemetry{Transit: s.metrics.transit.summary(), Bargein: s.metrics.bargein.summary(), QueueOverflow: s.metrics.overflows.Load()}
}

func (s *Server) MaximumProviderSessions() int { return int(s.connector.maximum.Load()) }

func (s *Server) StaleFramesInjected() int { return int(s.connector.staleInjected.Load()) }

func (s *Server) Close() {
	s.http.CloseClientConnections()
	s.http.Close()
}

type ids struct{ next atomic.Uint64 }

func (i *ids) NewSessionID() identity.SessionID {
	return identity.SessionID(fmt.Sprintf("load-session-%d", i.next.Add(1)))
}
func (i *ids) NewConversationID() identity.ConversationID {
	return identity.ConversationID(fmt.Sprintf("load-conversation-%d", i.next.Add(1)))
}

type echoConnector struct {
	active, maximum, staleInjected atomic.Int64
	cancelDelay                    time.Duration
}

func (c *echoConnector) Connect(ctx context.Context, _ realtime.OpenRequest) (realtime.Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	active := c.active.Add(1)
	for {
		maximum := c.maximum.Load()
		if active <= maximum || c.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	s := &echoSession{events: make(chan realtime.Event, 8), done: make(chan struct{}), connector: c, cancelDelay: c.cancelDelay, release: func() { c.active.Add(-1) }}
	s.events <- realtime.Event{Kind: realtime.EventSessionCreated, ProviderEventID: "created"}
	return s, nil
}

type echoSession struct {
	mu          sync.Mutex
	pending     []byte
	response    uint64
	activeID    string
	activeItem  string
	activeEpoch identity.ResponseEpoch
	activeAudio []byte
	events      chan realtime.Event
	done        chan struct{}
	connector   *echoConnector
	cancelDelay time.Duration
	closeOnce   sync.Once
	release     func()
}

func (s *echoSession) Send(ctx context.Context, command realtime.Command) error {
	select {
	case <-s.done:
		return errors.New("echo provider closed")
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch command.Kind {
	case realtime.CommandAppendAudio:
		s.pending = append(s.pending[:0], command.Audio...)
	case realtime.CommandCreateResponse:
		s.response++
		responseID := fmt.Sprintf("response-%d", s.response)
		itemID := fmt.Sprintf("item-%d", s.response)
		events := []realtime.Event{
			{Kind: realtime.EventResponseCreated, Epoch: command.Epoch, ProviderEventID: "response-created", ResponseID: responseID, Status: "in_progress"},
			{Kind: realtime.EventAudioDelta, Epoch: command.Epoch, ProviderEventID: "delta", ResponseID: responseID, ItemID: itemID, Audio: append([]byte(nil), s.pending...)},
			{Kind: realtime.EventAudioDone, Epoch: command.Epoch, ProviderEventID: "done", ResponseID: responseID, ItemID: itemID},
		}
		if _, fenceProbe := command.Response.Metadata["live_load_fence_probe"]; fenceProbe {
			s.activeID = responseID
			s.activeItem = itemID
			s.activeEpoch = command.Epoch
			s.activeAudio = append(s.activeAudio[:0], s.pending...)
		} else {
			events = append(events, realtime.Event{Kind: realtime.EventResponseDone, Epoch: command.Epoch, ProviderEventID: "response-done", ResponseID: responseID, Status: "completed"})
		}
		for _, event := range events {
			select {
			case s.events <- event:
			case <-ctx.Done():
				return ctx.Err()
			case <-s.done:
				return errors.New("echo provider closed")
			}
		}
	case realtime.CommandCancelResponse:
		if s.activeID == "" || command.ResponseID != s.activeID {
			return errors.New("echo provider cancel names inactive response")
		}
		responseID := s.activeID
		itemID, epoch := s.activeItem, s.activeEpoch
		staleAudio := append([]byte(nil), s.activeAudio...)
		s.activeID = ""
		if s.cancelDelay > 0 {
			timer := time.NewTimer(s.cancelDelay)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
				return ctx.Err()
			case <-s.done:
				return errors.New("echo provider closed")
			}
		}
		select {
		case s.events <- realtime.Event{Kind: realtime.EventResponseDone, Epoch: command.Epoch, ProviderEventID: "response-cancelled", ResponseID: responseID, Status: "cancelled"}:
		case <-ctx.Done():
			return ctx.Err()
		case <-s.done:
			return errors.New("echo provider closed")
		}
		select {
		case s.events <- realtime.Event{Kind: realtime.EventAudioDelta, Epoch: epoch, ProviderEventID: "stale-after-cancel", ResponseID: responseID, ItemID: itemID, Audio: staleAudio}:
			s.connector.staleInjected.Add(1)
		case <-ctx.Done():
			return ctx.Err()
		case <-s.done:
			return errors.New("echo provider closed")
		}
	}
	return nil
}

func (s *echoSession) Receive(ctx context.Context) (realtime.Event, error) {
	select {
	case event := <-s.events:
		return event, nil
	case <-ctx.Done():
		return realtime.Event{}, ctx.Err()
	case <-s.done:
		return realtime.Event{}, errors.New("echo provider closed")
	}
}

func (s *echoSession) Close(context.Context) error {
	s.closeOnce.Do(func() { close(s.done); s.release() })
	return nil
}

type lockedHistogram struct {
	mu    sync.Mutex
	value *loadgen.Histogram
}

func (h *lockedHistogram) Record(value time.Duration) {
	h.mu.Lock()
	h.value.Record(value)
	h.mu.Unlock()
}
func (h *lockedHistogram) summary() loadgen.LatencySummary {
	h.mu.Lock()
	defer h.mu.Unlock()
	return loadgen.LatencySummary{Count: h.value.Count(), P50: loadgen.Duration(h.value.Percentile(50)), P95: loadgen.Duration(h.value.Percentile(95)), P99: loadgen.Duration(h.value.Percentile(99)), Max: loadgen.Duration(h.value.Max())}
}

type metrics struct {
	transit, bargein *lockedHistogram
	overflows        atomic.Int64
}

func newMetrics() *metrics {
	return &metrics{transit: &lockedHistogram{value: loadgen.NewHistogram(time.Minute, 3)}, bargein: &lockedHistogram{value: loadgen.NewHistogram(time.Minute, 3)}}
}
func (m *metrics) overflow(string) { m.overflows.Add(1) }

var _ realtime.Connector = (*echoConnector)(nil)
var _ realtime.Session = (*echoSession)(nil)
