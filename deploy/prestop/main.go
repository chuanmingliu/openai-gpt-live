// Command live-prestop performs an authenticated loopback drain and waits for
// readiness to fall within one total elapsed-time budget.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/admin"
)

const (
	healthPort = 9090
	pollEvery  = time.Second
)

type drainFunc func(context.Context, string, string) error

func main() {
	flags := flag.NewFlagSet("live-prestop", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	port := flags.Int("health-port", 0, "loopback diagnostics port")
	budget := flags.Duration("budget", 0, "total elapsed-time budget")
	if err := flags.Parse(os.Args[1:]); err != nil || flags.NArg() != 0 || *port != healthPort || (*budget != 35*time.Second && *budget != 125*time.Second) {
		fmt.Fprintln(os.Stderr, "invalid live-prestop contract")
		os.Exit(64)
	}
	token, ok := os.LookupEnv("LIVE_ADMIN_BEARER_TOKEN")
	if !ok || token == "" {
		fmt.Fprintln(os.Stderr, "live-prestop admin credential is unavailable")
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *budget)
	defer cancel()
	baseURL := "http://127.0.0.1:" + strconv.Itoa(*port)
	client := &http.Client{Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	err := execute(ctx, client, baseURL, token, pollEvery, func(ctx context.Context, rawURL, bearer string) error {
		return admin.DrainURL(ctx, nil, rawURL, bearer)
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "live-prestop failed within its elapsed-time budget")
		os.Exit(1)
	}
}

func execute(ctx context.Context, client *http.Client, baseURL, token string, interval time.Duration, drain drainFunc) error {
	if ctx == nil || client == nil || token == "" || interval <= 0 || drain == nil {
		return errors.New("invalid preStop execution contract")
	}
	if err := drain(ctx, baseURL+"/admin/v1/drain", token); err != nil {
		return err
	}
	readyURL := baseURL + "/health/ready"
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, readyURL, nil)
		if err != nil {
			return err
		}
		response, err := client.Do(request)
		if err != nil {
			if ctx.Err() != nil {
				return errors.New("preStop elapsed-time budget exhausted")
			}
			return nil
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		_ = response.Body.Close()
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return nil
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.New("preStop elapsed-time budget exhausted")
		case <-timer.C:
		}
	}
}
