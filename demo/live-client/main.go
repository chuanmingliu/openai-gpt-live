package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
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
		log.Fatal("demo server stopped")
	}
}

func run(context.Context, demoConfig, []byte) error {
	return errors.New("demo server is not implemented")
}
