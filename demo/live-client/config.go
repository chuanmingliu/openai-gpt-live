package main

import (
	"errors"
	"flag"
	"io"
	"net"
	"net/url"
	"strings"
	"unicode/utf8"
)

const (
	defaultListenAddr    = "127.0.0.1:3000"
	defaultGatewayURL    = "ws://127.0.0.1:18080/v1/realtime"
	defaultModel         = "gpt-live-simulator"
	defaultOrigin        = "http://127.0.0.1:3000"
	defaultBearerEnv     = "LIVE_DEMO_BEARER"
	defaultMaxFrameBytes = int64(64 << 10)
	maxBearerBytes       = 16 << 10
	maxIdentityBytes     = 128
)

type demoConfig struct {
	ListenAddr    string
	GatewayURL    string
	Model         string
	Origin        string
	BearerEnv     string
	MaxFrameBytes int64
}

func parseConfig(args []string) (demoConfig, error) {
	cfg := demoConfig{
		ListenAddr:    defaultListenAddr,
		GatewayURL:    defaultGatewayURL,
		Model:         defaultModel,
		Origin:        defaultOrigin,
		BearerEnv:     defaultBearerEnv,
		MaxFrameBytes: defaultMaxFrameBytes,
	}
	flags := flag.NewFlagSet("live-client", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&cfg.ListenAddr, "listen", cfg.ListenAddr, "loopback HTTP listen address")
	flags.StringVar(&cfg.GatewayURL, "gateway-url", cfg.GatewayURL, "GPT Live WebSocket URL without query")
	flags.StringVar(&cfg.Model, "model", cfg.Model, "advertised gateway model")
	flags.StringVar(&cfg.Origin, "origin", cfg.Origin, "exact browser origin")
	flags.StringVar(&cfg.BearerEnv, "bearer-env", cfg.BearerEnv, "environment variable containing the bearer")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return demoConfig{}, errors.New("invalid demo arguments")
	}
	if err := cfg.validate(); err != nil {
		return demoConfig{}, err
	}
	return cfg, nil
}

func (cfg demoConfig) validate() error {
	if cfg.MaxFrameBytes <= 0 || cfg.MaxFrameBytes > 1<<20 {
		return errors.New("demo frame limit is invalid")
	}
	if err := validateLoopbackAddr(cfg.ListenAddr); err != nil {
		return err
	}
	if err := validateGatewayURL(cfg.GatewayURL); err != nil {
		return err
	}
	if err := validateOrigin(cfg.Origin, cfg.ListenAddr); err != nil {
		return err
	}
	if !boundedToken(cfg.Model, maxIdentityBytes) || strings.ContainsAny(cfg.Model, " \t\r\n") {
		return errors.New("demo model is invalid")
	}
	if !environmentName(cfg.BearerEnv) {
		return errors.New("demo bearer environment name is invalid")
	}
	return nil
}

func (cfg demoConfig) upstreamURL() (*url.URL, error) {
	parsed, err := url.Parse(cfg.GatewayURL)
	if err != nil {
		return nil, errors.New("demo gateway URL is invalid")
	}
	query := parsed.Query()
	query.Set("model", cfg.Model)
	parsed.RawQuery = query.Encode()
	return parsed, nil
}

func (cfg demoConfig) websocketOrigin() string {
	parsed, err := url.Parse(cfg.Origin)
	if err != nil {
		return ""
	}
	scheme := "ws"
	if parsed.Scheme == "https" {
		scheme = "wss"
	}
	return scheme + "://" + parsed.Host
}

func resolveBearer(cfg demoConfig, lookup func(string) (string, bool)) ([]byte, error) {
	if lookup == nil {
		return nil, errors.New("configured demo bearer is unavailable")
	}
	value, ok := lookup(cfg.BearerEnv)
	if !ok || value == "" || len(value) > maxBearerBytes || !utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return nil, errors.New("configured demo bearer is unavailable")
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return nil, errors.New("configured demo bearer is unavailable")
		}
	}
	return []byte(value), nil
}

func validateLoopbackAddr(raw string) error {
	host, port, err := net.SplitHostPort(raw)
	if err != nil || port == "" {
		return errors.New("demo listen address must be loopback host:port")
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return errors.New("demo listen address must be loopback")
	}
	return nil
}

func validateGatewayURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Opaque != "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.ForceQuery || parsed.OmitHost {
		return errors.New("demo gateway URL is invalid")
	}
	if parsed.Scheme != "ws" && parsed.Scheme != "wss" {
		return errors.New("demo gateway URL must be ws or wss")
	}
	if parsed.Path != "/v1/realtime" {
		return errors.New("demo gateway path must be /v1/realtime")
	}
	return nil
}

func validateOrigin(raw, listenAddr string) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "http" || parsed.Host == "" || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Opaque != "" {
		return errors.New("demo origin is invalid")
	}
	originHost, originPort, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		if parsed.Scheme == "http" {
			originHost, originPort = parsed.Hostname(), "80"
		} else {
			return errors.New("demo origin is invalid")
		}
	}
	listenHost, listenPort, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return errors.New("demo listen address must be loopback host:port")
	}
	if !sameLoopbackHost(originHost, listenHost) || originPort != listenPort {
		return errors.New("demo origin must match the listen address")
	}
	return nil
}

func sameLoopbackHost(left, right string) bool {
	leftIP, rightIP := net.ParseIP(left), net.ParseIP(right)
	return leftIP != nil && rightIP != nil && leftIP.IsLoopback() && rightIP.IsLoopback() && leftIP.Equal(rightIP)
}

func environmentName(value string) bool {
	if value == "" || len(value) > maxIdentityBytes || value[0] < 'A' || value[0] > 'Z' {
		return false
	}
	for i := 1; i < len(value); i++ {
		b := value[i]
		if b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '_' {
			continue
		}
		return false
	}
	return true
}

func boundedToken(value string, limit int) bool {
	return value != "" && len(value) <= limit && utf8.ValidString(value)
}

func clear(value []byte) {
	for i := range value {
		value[i] = 0
	}
}
