package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"
)

const idleWorkerListenAddress = "127.0.0.1:9091"

const (
	idleWorkerShutdownTimeout   = 5 * time.Second
	idleWorkerReadHeaderTimeout = 5 * time.Second
)

type idleRuntimeBuilder func(context.Context) (serviceRuntime, error)

type idleRuntime struct {
	listener        net.Listener
	server          *http.Server
	shutdownTimeout time.Duration
	closeOnce       sync.Once
}

func idleWorkerHandler() http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/health/live":
			if request.Method != http.MethodGet && request.Method != http.MethodHead {
				response.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			response.WriteHeader(http.StatusOK)
			if request.Method == http.MethodGet {
				_, _ = response.Write([]byte("ok\n"))
			}
		case "/health/ready":
			if request.Method != http.MethodGet && request.Method != http.MethodHead {
				response.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			response.WriteHeader(http.StatusServiceUnavailable)
		default:
			response.WriteHeader(http.StatusNotFound)
		}
	})
}

func newIdleRuntime(listener net.Listener) *idleRuntime {
	return &idleRuntime{
		listener:        listener,
		server:          &http.Server{Handler: idleWorkerHandler(), ReadHeaderTimeout: idleWorkerReadHeaderTimeout},
		shutdownTimeout: idleWorkerShutdownTimeout,
	}
}

func buildIdleRuntime(context.Context) (serviceRuntime, error) {
	listener, err := net.Listen("tcp", idleWorkerListenAddress)
	if err != nil {
		return nil, err
	}
	return newIdleRuntime(listener), nil
}

func (r *idleRuntime) Run(ctx context.Context) error {
	errorsOut := make(chan error, 1)
	go func() { errorsOut <- r.server.Serve(r.listener) }()

	select {
	case err := <-errorsOut:
		if errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		r.Close()
		<-errorsOut
		return ctx.Err()
	}
}

func (r *idleRuntime) Close() {
	if r == nil {
		return
	}
	r.closeOnce.Do(func() {
		shutdownTimeout := r.shutdownTimeout
		if shutdownTimeout <= 0 {
			shutdownTimeout = idleWorkerShutdownTimeout
		}
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if r.server != nil {
			if r.server.Shutdown(ctx) != nil {
				_ = r.server.Close()
			}
		}
		if r.listener != nil {
			_ = r.listener.Close()
		}
	})
}
