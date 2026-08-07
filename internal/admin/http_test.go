package admin

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/chuanmingliu/openai-gpt-live/internal/auth"
	"github.com/chuanmingliu/openai-gpt-live/internal/lifecycle"
)

type staticAuth struct{}

func (staticAuth) Authenticate(context.Context, string) (auth.Principal, error) {
	return auth.Principal{Scopes: map[string]struct{}{AdminScope: {}}}, nil
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
