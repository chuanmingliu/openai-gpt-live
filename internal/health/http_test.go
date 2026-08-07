package health

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/lifecycle"
)

func TestReadinessFailsDuringDrain(t *testing.T) {
	d := lifecycle.NewDrainer()
	r := NewRegistry()
	if err := r.RegisterDrainer(d); err != nil {
		t.Fatal(err)
	}
	d.Begin()
	w := httptest.NewRecorder()
	r.ReadyHandler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if w.Code != http.StatusServiceUnavailable || w.Body.Len() != 0 {
		t.Fatalf("code=%d body=%q", w.Code, w.Body.String())
	}
}

func TestLiveIsFixedAndDoesNotRunChecks(t *testing.T) {
	w := httptest.NewRecorder()
	LiveHandler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health/live", nil))
	if w.Code != 200 || w.Body.String() != "ok\n" || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("unexpected response: %#v %q", w.Result(), w.Body.String())
	}
}

func TestSlowAndPanickingChecksAreBounded(t *testing.T) {
	r := NewRegistry(Options{CheckTimeout: time.Millisecond, AggregateTimeout: 5 * time.Millisecond})
	if err := r.RegisterRequired("panic", CheckFunc(func(context.Context) error { panic("private") })); err != nil {
		t.Fatal(err)
	}
	if err := r.RegisterRequired("stuck", CheckFunc(func(context.Context) error { select {} })); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if r.Ready(context.Background()) {
		t.Fatal("unexpected ready")
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("readiness exceeded bound")
	}
}
