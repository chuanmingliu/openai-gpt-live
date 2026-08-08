package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestExecuteAuthenticatesDrainAndObservesReadinessFall(t *testing.T) {
	const token = "test-secret-never-log"
	var polls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/admin/v1/drain":
			if r.Header.Get("Authorization") != "Bearer "+token {
				t.Error("missing drain authorization")
			}
			w.WriteHeader(http.StatusAccepted)
		case "/health/ready":
			if polls.Add(1) == 1 {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	drain := func(ctx context.Context, rawURL, bearer string) error {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, rawURL, nil)
		req.Header.Set("Authorization", "Bearer "+bearer)
		response, err := server.Client().Do(req)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusAccepted {
			return errors.New("drain rejected")
		}
		return nil
	}
	if err := execute(ctx, server.Client(), server.URL, token, time.Millisecond, drain); err != nil {
		t.Fatal(err)
	}
}

func TestExecuteFailsWithinBudgetWhenReadinessNeverFalls(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer server.Close()
	budget := 80 * time.Millisecond
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	err := execute(ctx, server.Client(), server.URL, "secret", 5*time.Millisecond, func(context.Context, string, string) error { return nil })
	if err == nil {
		t.Fatal("readiness-never-falls unexpectedly succeeded")
	}
	if elapsed := time.Since(started); elapsed > budget+150*time.Millisecond {
		t.Fatalf("elapsed %s exceeded bounded margin", elapsed)
	}
}

func TestExecuteCancelsHangingReadinessWithinBudget(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()
	budget := 80 * time.Millisecond
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	err := execute(ctx, &http.Client{Timeout: time.Second}, server.URL, "secret", time.Millisecond, func(context.Context, string, string) error { return nil })
	if err == nil {
		t.Fatal("hanging readiness unexpectedly succeeded")
	}
	if elapsed := time.Since(started); elapsed > budget+150*time.Millisecond {
		t.Fatalf("elapsed %s exceeded bounded margin", elapsed)
	}
}
