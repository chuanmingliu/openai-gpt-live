package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/config"
	"github.com/chuanmingliu/openai-gpt-live/internal/gateway"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime/s2sws"
)

const providerConnectTimeout = 10 * time.Second

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:], os.LookupEnv); err != nil {
		log.Printf("live gateway stopped: %v", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, lookup func(string) (string, bool)) error {
	flags := flag.NewFlagSet("live-gateway", flag.ContinueOnError)
	configPath := flags.String("config", "", "path to gateway YAML configuration")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *configPath == "" {
		return errors.New("-config is required")
	}
	cfg, err := config.Load(*configPath, lookup)
	if err != nil {
		return fmt.Errorf("load gateway configuration: %w", err)
	}
	apiKey := ""
	if cfg.Gateway.Voice.APIKeyEnv != "" {
		value, ok := lookup(cfg.Gateway.Voice.APIKeyEnv)
		if !ok {
			return errors.New("configured provider credential is unavailable")
		}
		apiKey = value
	}
	connector, err := s2sws.New(s2sws.Config{
		URL:            cfg.Gateway.Voice.URL,
		APIKey:         apiKey,
		Model:          cfg.Gateway.Voice.Model,
		ConnectTimeout: providerConnectTimeout,
		MaxFrameBytes:  int64(cfg.Gateway.WebSocket.MaxFrameBytes),
	})
	if err != nil {
		return fmt.Errorf("configure realtime connector: %w", err)
	}
	app, err := gateway.New(cfg, connector)
	if err != nil {
		return fmt.Errorf("configure gateway: %w", err)
	}
	return app.Run(ctx)
}
