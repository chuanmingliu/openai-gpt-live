package main

import (
	"context"
	"errors"
	"log"
	"os"
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
	if err := run(context.Background(), cfg, bearer); err != nil {
		log.Fatal("demo server stopped")
	}
}

func run(context.Context, demoConfig, []byte) error {
	return errors.New("demo server is not implemented")
}
