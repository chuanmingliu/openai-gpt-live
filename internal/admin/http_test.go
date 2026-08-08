package admin

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/chuanmingliu/openai-gpt-live/internal/auth"
	"github.com/chuanmingliu/openai-gpt-live/internal/feature"
	"github.com/chuanmingliu/openai-gpt-live/internal/lifecycle"
)

type staticAuth struct{}

func requireTrue(t *testing.T, condition bool) {
	t.Helper()
	if !condition {
		t.Fatal("condition is false")
	}
}

func (staticAuth) Authenticate(context.Context, string) (auth.Principal, error) {
	return auth.Principal{UserID: "operator-7", Scopes: map[string]struct{}{AdminScope: {}}}, nil
}

type noScopeAuth struct{}

func (noScopeAuth) Authenticate(context.Context, string) (auth.Principal, error) {
	return auth.Principal{Scopes: map[string]struct{}{}}, nil
}
func request(method, path string) *http.Request {
	r := httptest.NewRequest(method, path, nil)
	r.RemoteAddr = "127.0.0.1:1234"
	r.Header.Set("Authorization", "Bearer valid")
	return r
}
func TestDrainRequiresPostJSONAndIsIdempotent(t *testing.T) {
	d := lifecycle.NewDrainer()
	h, err := NewHandler(Config{Authenticator: staticAuth{}, Drainer: d})
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, request(http.MethodGet, "/admin/v1/drain"))
	if w.Code != http.StatusMethodNotAllowed || d.IsDraining() {
		t.Fatal("GET mutated drain")
	}
	for range 2 {
		r := request(http.MethodPost, "/admin/v1/drain")
		r.Body = ioNop("{}")
		r.ContentLength = 2
		r.Header.Set("Content-Type", "application/json")
		w = httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusAccepted {
			t.Fatalf("code %d", w.Code)
		}
	}
}
func TestAdminRejectsNonLoopback(t *testing.T) {
	d := lifecycle.NewDrainer()
	h, _ := NewHandler(Config{Authenticator: staticAuth{}, Drainer: d})
	r := request(http.MethodGet, "/admin/v1/sessions")
	r.RemoteAddr = "203.0.113.1:80"
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusForbidden {
		t.Fatal(w.Code)
	}
}
func TestAdminRejectsAuthenticatedPrincipalWithoutScope(t *testing.T) {
	h, _ := NewHandler(Config{Authenticator: noScopeAuth{}, Drainer: lifecycle.NewDrainer()})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, request(http.MethodGet, "/admin/v1/sessions"))
	if w.Code != http.StatusForbidden || w.Body.Len() != 0 {
		t.Fatalf("code=%d body=%q", w.Code, w.Body.String())
	}
}

type featureStoreFake struct {
	mu               sync.Mutex
	name, actor, why string
	enabled          bool
	snapshot         feature.Snapshot
	err              error
}

func (f *featureStoreFake) Load(context.Context) (feature.Snapshot, error) { return f.snapshot, f.err }
func (f *featureStoreFake) Set(_ context.Context, name string, enabled bool, actor, reason string) (feature.Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.name, f.enabled, f.actor, f.why = name, enabled, actor, reason
	return f.snapshot, f.err
}

func TestHandoffFeaturePUTUsesAuthenticatedActorAndAppliesSnapshot(t *testing.T) {
	store := &featureStoreFake{snapshot: feature.Snapshot{Version: 2, Values: map[string]bool{feature.HandoffsEnabled: false, feature.ShadowMode: false}}}
	gates := feature.New(nil)
	h, err := NewHandler(Config{Authenticator: staticAuth{}, Drainer: lifecycle.NewDrainer(), FeatureStore: store, FeatureGates: gates})
	if err != nil {
		t.Fatal(err)
	}
	r := request(http.MethodPut, "/admin/v1/features/handoffs")
	r.Body = ioNop(`{"enabled":false,"reason":"incident-123"}`)
	r.ContentLength = int64(len(`{"enabled":false,"reason":"incident-123"}`))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("code=%d body=%q", w.Code, w.Body.String())
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.name != feature.HandoffsEnabled || store.enabled || store.actor != "operator-7" || store.why != "incident-123" {
		t.Fatalf("mutation=%q %v %q %q", store.name, store.enabled, store.actor, store.why)
	}
	if gates.Enabled(feature.HandoffsEnabled) || gates.Snapshot().Version != 2 {
		t.Fatal("returned authority snapshot was not applied")
	}
}

func handoffFeatureRequest(body string) *http.Request {
	r := request(http.MethodPut, "/admin/v1/features/handoffs")
	r.Body, r.ContentLength = ioNop(body), int64(len(body))
	r.Header.Set("Content-Type", "application/json")
	return r
}

func TestHandoffFeaturePUTAcknowledgesCommittedSnapshotAlreadySupersededLocally(t *testing.T) {
	committed := feature.Snapshot{Version: 2, Values: map[string]bool{feature.HandoffsEnabled: false, feature.ShadowMode: false}}
	store := &featureStoreFake{snapshot: committed}
	gates := feature.New(nil)
	requireTrue(t, gates.Apply(feature.Snapshot{Version: 3, Values: map[string]bool{feature.HandoffsEnabled: true, feature.ShadowMode: true}}))
	h, err := NewHandler(Config{Authenticator: staticAuth{}, Drainer: lifecycle.NewDrainer(), FeatureStore: store, FeatureGates: gates})
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, handoffFeatureRequest(`{"enabled":false,"reason":"committed-before-later-writer"}`))
	if w.Code != http.StatusOK {
		t.Fatalf("code=%d body=%q", w.Code, w.Body.String())
	}
	var response struct {
		Name    string `json:"name"`
		Enabled bool   `json:"enabled"`
		Version uint64 `json:"version"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Name != feature.HandoffsEnabled || response.Enabled || response.Version != 2 {
		t.Fatalf("response=%+v", response)
	}
	current := gates.Snapshot()
	if current.Version != 3 || !current.Values[feature.HandoffsEnabled] || !current.Values[feature.ShadowMode] {
		t.Fatalf("newer local authority was rolled back: %+v", current)
	}
}

func TestHandoffFeaturePUTEqualSnapshotRequiresExactIdentity(t *testing.T) {
	tests := []struct {
		name       string
		committed  feature.Snapshot
		wantStatus int
	}{
		{
			name: "identical",
			committed: feature.Snapshot{Version: 2, Values: map[string]bool{
				feature.HandoffsEnabled: false, feature.ShadowMode: true,
			}},
			wantStatus: http.StatusOK,
		},
		{
			name: "conflicting",
			committed: feature.Snapshot{Version: 2, Values: map[string]bool{
				feature.HandoffsEnabled: true, feature.ShadowMode: true,
			}},
			wantStatus: http.StatusServiceUnavailable,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gates := feature.New(nil)
			requireTrue(t, gates.Apply(feature.Snapshot{Version: 2, Values: map[string]bool{
				feature.HandoffsEnabled: false, feature.ShadowMode: true,
			}}))
			h, err := NewHandler(Config{Authenticator: staticAuth{}, Drainer: lifecycle.NewDrainer(), FeatureStore: &featureStoreFake{snapshot: test.committed}, FeatureGates: gates})
			if err != nil {
				t.Fatal(err)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, handoffFeatureRequest(`{"enabled":false,"reason":"equal-version"}`))
			if w.Code != test.wantStatus {
				t.Fatalf("code=%d body=%q", w.Code, w.Body.String())
			}
			current := gates.Snapshot()
			if current.Version != 2 || current.Values[feature.HandoffsEnabled] || !current.Values[feature.ShadowMode] {
				t.Fatalf("equal-version handling mutated local authority: %+v", current)
			}
		})
	}
}

func TestHandoffFeaturePUTRejectsInvalidCommittedSnapshotWhenLocalAuthorityIsLower(t *testing.T) {
	tests := []feature.Snapshot{
		{Version: 0, Values: map[string]bool{feature.HandoffsEnabled: false, feature.ShadowMode: false}},
		{Version: 3, Values: map[string]bool{feature.HandoffsEnabled: false}},
		{Version: 3, Values: map[string]bool{feature.HandoffsEnabled: false, feature.ShadowMode: false, "unknown": true}},
	}
	for index, committed := range tests {
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			gates := feature.New(nil)
			requireTrue(t, gates.Apply(feature.Snapshot{Version: 1, Values: map[string]bool{feature.HandoffsEnabled: true, feature.ShadowMode: false}}))
			h, err := NewHandler(Config{Authenticator: staticAuth{}, Drainer: lifecycle.NewDrainer(), FeatureStore: &featureStoreFake{snapshot: committed}, FeatureGates: gates})
			if err != nil {
				t.Fatal(err)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, handoffFeatureRequest(`{"enabled":false,"reason":"invalid-store-result"}`))
			if w.Code != http.StatusServiceUnavailable || w.Body.Len() != 0 {
				t.Fatalf("code=%d body=%q", w.Code, w.Body.String())
			}
			if gates.Snapshot().Version != 1 || !gates.Enabled(feature.HandoffsEnabled) {
				t.Fatal("invalid committed snapshot changed lower local authority")
			}
		})
	}
}

type concurrentFeatureStore struct {
	firstEntered chan struct{}
	releaseFirst chan struct{}
}

func (s *concurrentFeatureStore) Load(context.Context) (feature.Snapshot, error) {
	return feature.Snapshot{}, errors.New("unused")
}

func (s *concurrentFeatureStore) Set(_ context.Context, _ string, enabled bool, _, reason string) (feature.Snapshot, error) {
	if reason == "first" {
		close(s.firstEntered)
		<-s.releaseFirst
		return feature.Snapshot{Version: 2, Values: map[string]bool{feature.HandoffsEnabled: enabled, feature.ShadowMode: false}}, nil
	}
	return feature.Snapshot{Version: 3, Values: map[string]bool{feature.HandoffsEnabled: enabled, feature.ShadowMode: true}}, nil
}

func TestConcurrentHandoffFeaturePUTsAcknowledgeEachCommitWithoutLocalRollback(t *testing.T) {
	store := &concurrentFeatureStore{firstEntered: make(chan struct{}), releaseFirst: make(chan struct{})}
	gates := feature.New(nil)
	h, err := NewHandler(Config{Authenticator: staticAuth{}, Drainer: lifecycle.NewDrainer(), FeatureStore: store, FeatureGates: gates})
	if err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, handoffFeatureRequest(`{"enabled":false,"reason":"first"}`))
		firstDone <- w
	}()
	<-store.firstEntered
	second := httptest.NewRecorder()
	h.ServeHTTP(second, handoffFeatureRequest(`{"enabled":true,"reason":"second"}`))
	if second.Code != http.StatusOK {
		t.Fatalf("second code=%d body=%q", second.Code, second.Body.String())
	}
	close(store.releaseFirst)
	first := <-firstDone
	if first.Code != http.StatusOK {
		t.Fatalf("first code=%d body=%q", first.Code, first.Body.String())
	}
	var firstResponse, secondResponse struct {
		Version uint64 `json:"version"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &firstResponse); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(second.Body.Bytes(), &secondResponse); err != nil {
		t.Fatal(err)
	}
	if firstResponse.Version != 2 || secondResponse.Version != 3 {
		t.Fatalf("versions first=%d second=%d", firstResponse.Version, secondResponse.Version)
	}
	current := gates.Snapshot()
	if current.Version != 3 || !current.Values[feature.HandoffsEnabled] || !current.Values[feature.ShadowMode] {
		t.Fatalf("later local authority was rolled back: %+v", current)
	}
}

func TestHandoffFeaturePUTRejectsDuplicateUnknownTrailingAndHEAD(t *testing.T) {
	store := &featureStoreFake{snapshot: feature.Snapshot{Version: 2, Values: map[string]bool{feature.HandoffsEnabled: false, feature.ShadowMode: false}}}
	h, _ := NewHandler(Config{Authenticator: staticAuth{}, Drainer: lifecycle.NewDrainer(), FeatureStore: store, FeatureGates: feature.New(nil)})
	for _, body := range []string{
		`{"enabled":false,"enabled":true,"reason":"incident"}`,
		`{"enabled":false,"reason":"incident","actor":"forged"}`,
		`{"enabled":false,"reason":"incident"}{}`,
		`{"enabled":false}`,
	} {
		r := request(http.MethodPut, "/admin/v1/features/handoffs")
		r.Body, r.ContentLength = ioNop(body), int64(len(body))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusBadRequest || w.Body.Len() != 0 {
			t.Fatalf("body=%q code=%d response=%q", body, w.Code, w.Body.String())
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, request(http.MethodHead, "/admin/v1/features/handoffs"))
	if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") != "PUT" {
		t.Fatal(w.Code)
	}
}

func TestHandoffFeatureStoreFailureIsContentFree(t *testing.T) {
	store := &featureStoreFake{err: errors.New("private database host and incident content")}
	h, _ := NewHandler(Config{Authenticator: staticAuth{}, Drainer: lifecycle.NewDrainer(), FeatureStore: store, FeatureGates: feature.New(nil)})
	body := `{"enabled":false,"reason":"incident"}`
	r := request(http.MethodPut, "/admin/v1/features/handoffs")
	r.Body, r.ContentLength = ioNop(body), int64(len(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusServiceUnavailable || w.Body.Len() != 0 || strings.Contains(w.Body.String(), "private") {
		t.Fatalf("code=%d body=%q", w.Code, w.Body.String())
	}
}

type remoteAddrConn struct {
	net.Conn
	remote net.Addr
}

func (c remoteAddrConn) RemoteAddr() net.Addr { return c.remote }

type stringAddr string

func (a stringAddr) Network() string { return "tcp" }
func (a stringAddr) String() string  { return string(a) }

func drainPipe(t *testing.T, remote string, authorization chan<- string) net.Conn {
	t.Helper()
	client, server := net.Pipe()
	go func() {
		defer server.Close()
		request, err := http.ReadRequest(bufio.NewReader(server))
		if err != nil {
			return
		}
		if authorization != nil {
			authorization <- request.Header.Get("Authorization")
		}
		_, _ = io.WriteString(server, "HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
	}()
	return remoteAddrConn{Conn: client, remote: stringAddr(remote)}
}

func TestDrainURLPinsResolutionAndIgnoresProxyEnvironment(t *testing.T) {
	t.Setenv("HTTP_PROXY", "http://203.0.113.10:8080")
	resolutions := 0
	var dialed string
	authorization := make(chan string, 1)
	err := drainURLWithNetwork(t.Context(), nil, "http://drain.internal:4321/admin/v1/drain", "top-secret",
		func(context.Context, string) ([]net.IPAddr, error) {
			resolutions++
			if resolutions == 1 {
				return []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}, nil
			}
			return []net.IPAddr{{IP: net.ParseIP("203.0.113.1")}}, nil
		},
		func(_ context.Context, _, address string) (net.Conn, error) {
			dialed = address
			return drainPipe(t, "127.0.0.1:4321", authorization), nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if resolutions != 1 || dialed != "127.0.0.1:4321" {
		t.Fatalf("resolutions=%d dialed=%q", resolutions, dialed)
	}
	if got := <-authorization; got != "Bearer top-secret" {
		t.Fatal("authorization was not delivered only to the pinned peer")
	}
}

func TestDrainURLRejectsNonLoopbackConnectedPeerBeforeSendingBearer(t *testing.T) {
	peer, other := net.Pipe()
	defer other.Close()
	peer = remoteAddrConn{Conn: peer, remote: stringAddr("203.0.113.9:4321")}
	err := drainURLWithNetwork(t.Context(), nil, "http://127.0.0.1:4321", "top-secret",
		func(context.Context, string) ([]net.IPAddr, error) {
			t.Fatal("literal address must not resolve")
			return nil, nil
		},
		func(context.Context, string, string) (net.Conn, error) { return peer, nil })
	if err == nil || !strings.Contains(err.Error(), "drain request failed") {
		t.Fatalf("error=%v", err)
	}
}

func TestDrainURLPinsIPv4AndIPv6Literals(t *testing.T) {
	for _, test := range []struct {
		name, rawURL, dialed, remote string
	}{
		{"ipv4", "http://127.0.0.1:9001", "127.0.0.1:9001", "127.0.0.1:9001"},
		{"ipv6", "http://[::1]:9002", "[::1]:9002", "[::1]:9002"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var got string
			err := drainURLWithNetwork(t.Context(), nil, test.rawURL, "",
				func(context.Context, string) ([]net.IPAddr, error) {
					t.Fatal("literal address must not resolve")
					return nil, nil
				},
				func(_ context.Context, _, address string) (net.Conn, error) {
					got = address
					return drainPipe(t, test.remote, nil), nil
				})
			if err != nil || got != test.dialed {
				t.Fatalf("error=%v dialed=%q", err, got)
			}
		})
	}
}

func TestDrainURLReplacesCustomTLSDialerThatCouldBypassPinning(t *testing.T) {
	legacyDialed := false
	pinnedDialed := false
	client := &http.Client{Transport: &http.Transport{DialTLSContext: func(context.Context, string, string) (net.Conn, error) {
		legacyDialed = true
		return nil, errors.New("unsafe TLS dialer")
	}}}
	err := drainURLWithNetwork(t.Context(), client, "https://127.0.0.1:9443", "",
		func(context.Context, string) ([]net.IPAddr, error) {
			t.Fatal("literal address must not resolve")
			return nil, nil
		},
		func(context.Context, string, string) (net.Conn, error) {
			pinnedDialed = true
			return nil, errors.New("pinned dial stopped")
		})
	if err == nil || legacyDialed || !pinnedDialed {
		t.Fatalf("error=%v legacy=%v pinned=%v", err, legacyDialed, pinnedDialed)
	}
}

func ioNop(s string) io.ReadCloser { return io.NopCloser(strings.NewReader(s)) }
