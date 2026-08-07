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

	"github.com/chuanmingliu/openai-gpt-live/internal/admission"
	"github.com/chuanmingliu/openai-gpt-live/internal/auth"
	"github.com/chuanmingliu/openai-gpt-live/internal/config"
	"github.com/chuanmingliu/openai-gpt-live/internal/gateway"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime/s2sws"
	"github.com/jackc/pgx/v5/pgxpool"
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

func run(ctx context.Context, args []string, lookup func(string) (string, bool)) (runErr error) {
	flags := flag.NewFlagSet("live-gateway", flag.ContinueOnError)
	configPath := flags.String("config", "", "path to gateway YAML configuration")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *configPath == "" {
		return errors.New("-config is required")
	}
	cfg, err := config.LoadUnresolved(*configPath)
	if err != nil {
		return fmt.Errorf("load gateway configuration: %w", err)
	}
	if err := validateComposition(cfg); err != nil {
		return err
	}
	if err := cfg.ResolveSecrets(lookup); err != nil {
		return fmt.Errorf("load gateway configuration: %w", err)
	}
	admissionController, closeAdmission, err := composeAdmission(ctx, cfg, lookup)
	if err != nil {
		return err
	}
	if closeAdmission != nil {
		defer func() {
			runErr = finishAdmission(runErr, closeAdmission)
		}()
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
	var appOptions []gateway.Option
	if admissionController != nil {
		appOptions = append(appOptions, gateway.WithAdmission(admissionController))
	}
	app, err := gateway.New(cfg, connector, appOptions...)
	if err != nil {
		return fmt.Errorf("configure gateway: %w", err)
	}
	return app.Run(ctx)
}

func finishAdmission(runErr error, closeAdmission func() error) error {
	if closeAdmission == nil {
		return runErr
	}
	if err := closeAdmission(); err != nil {
		return errors.Join(runErr, fmt.Errorf("close admission storage: %w", err))
	}
	return runErr
}

func composeAdmission(ctx context.Context, cfg config.Config, lookup func(string) (string, bool)) (*admission.Controller, func() error, error) {
	var authenticator auth.Authenticator
	requiredScopes := cfg.Auth.OIDCRequiredScopes
	if cfg.Mode == "local" && cfg.Auth.StaticBearerEnv != "" {
		token, ok := lookup(cfg.Auth.StaticBearerEnv)
		if !ok || token == "" {
			return nil, nil, errors.New("configured static credential is unavailable")
		}
		scopes := make(map[string]struct{}, len(cfg.Auth.StaticScopes))
		for _, scope := range cfg.Auth.StaticScopes {
			scopes[scope] = struct{}{}
		}
		authenticator = auth.NewStatic([]byte(token), auth.Principal{TenantID: cfg.Auth.StaticTenantID, UserID: cfg.Auth.StaticUserID, Scopes: scopes})
		if len(requiredScopes) == 0 {
			requiredScopes = []string{"realtime"}
		}
	} else if cfg.Auth.OIDCIssuer != "" {
		oidcAuthenticator, err := auth.NewOIDC(ctx, auth.OIDCConfig{Issuer: cfg.Auth.OIDCIssuer, Audience: cfg.Auth.OIDCAudience, TenantClaim: cfg.Auth.OIDCTenantClaim, UserClaim: cfg.Auth.OIDCUserClaim, ScopeClaim: cfg.Auth.OIDCScopeClaim, RequiredScopes: cfg.Auth.OIDCRequiredScopes, MaxKeySetStaleness: cfg.Auth.OIDCCacheOutage, HTTPTimeout: cfg.Auth.OIDCHTTPTimeout})
		if err != nil {
			return nil, nil, errors.New("OIDC authentication is unavailable")
		}
		authenticator = oidcAuthenticator
	} else if cfg.Mode == "local" {
		return nil, nil, nil
	} else {
		return nil, nil, errors.New("production authentication is unavailable")
	}
	controllerConfig := admission.Config{RequireTLS: cfg.Mode == "production", BehindIngress: cfg.Gateway.BehindIngress, TrustedProxyCIDRs: cfg.Gateway.TrustedProxies, AllowedOrigins: cfg.Auth.AllowedOrigins, AllowEmptyOrigin: cfg.Auth.AllowNonBrowser, GlobalRate: cfg.Admission.GlobalRate, GlobalBurst: cfg.Admission.GlobalBurst, IPRate: cfg.Admission.IPRate, IPBurst: cfg.Admission.IPBurst, IPKeyTTL: cfg.Admission.IPKeyTTL, MaxIPKeys: cfg.Admission.MaxIPKeys, RequiredScopes: requiredScopes, Limits: admission.Limits{Global: cfg.Admission.MaxSessions, PerTenant: cfg.Admission.MaxSessionsPerTenant, PerUser: cfg.Admission.MaxSessionsPerUser}, RequireStore: cfg.Mode == "production"}
	if cfg.Mode != "production" {
		controller, err := admission.New(authenticator, controllerConfig)
		if err != nil {
			return nil, nil, fmt.Errorf("configure admission: %w", err)
		}
		return controller, nil, nil
	}
	databaseURL := cfg.Store.URL
	if cfg.Store.DatabaseURLEnv != "" {
		value, ok := lookup(cfg.Store.DatabaseURLEnv)
		if !ok || value == "" {
			return nil, nil, errors.New("configured database credential is unavailable")
		}
		databaseURL = value
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, nil, errors.New("configure admission storage")
	}
	closePool := func() { pool.Close() }
	if err = pool.Ping(ctx); err != nil {
		closePool()
		return nil, nil, errors.New("admission storage is unavailable")
	}
	instanceID := cfg.Admission.InstanceID
	if instanceID == "" {
		instanceID, err = admission.RandomInstanceID()
		if err != nil {
			closePool()
			return nil, nil, errors.New("generate admission instance identity")
		}
	}
	store, err := admission.NewPostgres(pool, admission.PostgresConfig{InstanceID: instanceID, LeaseDuration: cfg.Admission.LeaseDuration, HeartbeatInterval: cfg.Admission.HeartbeatInterval})
	if err != nil {
		closePool()
		return nil, nil, fmt.Errorf("configure admission storage: %w", err)
	}
	controller, err := admission.New(authenticator, controllerConfig, store)
	if err != nil {
		closeErr := store.Close()
		closePool()
		if closeErr != nil {
			return nil, nil, errors.Join(fmt.Errorf("configure admission: %w", err), fmt.Errorf("close admission storage: %w", closeErr))
		}
		return nil, nil, fmt.Errorf("configure admission: %w", err)
	}
	return controller, func() error {
		err := store.Close()
		closePool()
		return err
	}, nil
}

func validateComposition(cfg config.Config) error {
	if cfg.Features.Handoffs {
		// Task 18 supplies the configured safety policy. Authenticated admission is
		// composed above, but durable delivery remains closed until result safety is.
		return errors.New("handoff delivery is unavailable until configured presentation safety is installed")
	}
	return nil
}
