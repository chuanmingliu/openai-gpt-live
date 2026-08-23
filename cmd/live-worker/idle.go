package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/health"
)

const idleWorkerListenAddress = "127.0.0.1:9091"

var errIdleWorkerNotReady = errors.New("durable worker backend is disabled")

type idleRuntimeBuilder func(context.Context) (serviceRuntime, error)

type idleRuntime struct {
	server    *http.Server
	listener  net.Listener
	closeOnce sync.Once
}

func idleWorkerHandler() http.Handler {
	ready := health.NewRegistry()
	_ = ready.RegisterRequired("backend", health.CheckFunc(func(context.Context) error {
		return errIdleWorkerNotReady
	}))
	mux := http.NewServeMux()
	mux.Handle("/health/live", health.LiveHandler())
	mux.Handle("/health/ready", ready.ReadyHandler())
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		mux.ServeHTTP(w, req)
	})
}

func newIdleRuntime(listener net.Listener) *idleRuntime {
	return &idleRuntime{
		server: &http.Server{
			Handler:           idleWorkerHandler(),
			ReadHeaderTimeout: 5 * time.Second,
		},
		listener: listener,
	}
}

func buildIdleRuntime(context.Context) (serviceRuntime, error) {
	listener, err := net.Listen("tcp", idleWorkerListenAddress)
	if err != nil {
		return nil, errors.New("listen idle worker diagnostics")
	}
	return newIdleRuntime(listener), nil
}

func (r *idleRuntime) Run(ctx context.Context) error {
	if r == nil || r.server == nil || r.listener == nil || ctx == nil {
		return errors.New("idle worker runtime is invalid")
	}
	serveDone := make(chan error, 1)
	go func() {
		err := r.server.Serve(r.listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		serveDone <- err
	}()

	select {
	case err := <-serveDone:
		if err == nil {
			return errors.New("idle worker diagnostics stopped unexpectedly")
		}
		return err
	case <-ctx.Done():
		r.Close()
		<-serveDone
		return ctx.Err()
	}
}

func (r *idleRuntime) Close() {
	if r == nil {
		return
	}
	r.closeOnce.Do(func() {
		if r.server != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = r.server.Shutdown(ctx)
			cancel()
		}
		if r.listener != nil {
			_ = r.listener.Close()
		}
	})
}
