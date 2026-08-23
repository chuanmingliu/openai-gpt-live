package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		log.Fatal("demo configuration failed")
	}
	bearer, err := resolveBearer(cfg, os.LookupEnv)
	if err != nil {
		log.Fatal(err)
	}
	defer clear(bearer)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg, bearer); err != nil {
		clear(bearer)
		log.Fatal("demo server stopped")
	}
}

func run(ctx context.Context, cfg demoConfig, bearer []byte) error {
	handler, err := newHandler(cfg, bearer, upstreamDependencies{})
	if err != nil {
		return errors.New("demo server setup failed")
	}
	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return errors.New("demo server listen failed")
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()

	select {
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return errors.New("demo server serve failed")
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return errors.New("demo server shutdown failed")
		}
		if err := <-serveErr; !errors.Is(err, http.ErrServerClosed) {
			return errors.New("demo server serve failed")
		}
		return nil
	}
}
