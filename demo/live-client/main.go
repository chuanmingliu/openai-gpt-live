package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		log.Error("configuration_failed")
		os.Exit(1)
	}
	bearer, err := resolveBearer(cfg, os.LookupEnv)
	if err != nil {
		log.Error("bearer_unavailable")
		os.Exit(1)
	}
	defer clear(bearer)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Info("listening", "mode", cfg.Mode, "addr", cfg.ListenAddr)
	if err := run(ctx, cfg, bearer); err != nil {
		log.Error("server_stopped")
		os.Exit(1)
	}
}
