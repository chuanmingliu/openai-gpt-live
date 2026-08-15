package main

import (
	"errors"
	"flag"
	"io"
	"net"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	defaultListenAddr    = "127.0.0.1:3000"
	defaultGatewayURL    = "ws://127.0.0.1:18080/v1/realtime"
	defaultModel         = "gpt-live-simulator"
	defaultOrigin        = "http://127.0.0.1:3000"
	defaultBearerEnv     = "LIVE_DEMO_BEARER"
	defaultMaxFrameBytes = int64(64 << 10)
	defaultMaxSessions   = 8
	defaultHandshake     = 10 * time.Second
	defaultIdleTimeout   = 5 * time.Minute
	defaultMaxSession    = 2 * time.Hour
	defaultDrainTimeout  = 15 * time.Second
	maxBearerBytes       = 16 << 10
	maxIdentityBytes     = 128
	modeLocal            = "local"
	modeProduction       = "production"
)

type demoConfig struct {
	Mode          string
	ListenAddr    string
	GatewayURL    string
	Model         string
	Origin        string
	BearerEnv     string
	TLSCertFile   string
	TLSKeyFile    string
	BehindIngress bool
	MaxFrameBytes int64
	MaxSessions   int
	Handshake     time.Duration
	IdleTimeout   time.Duration
	MaxSession    time.Duration
	DrainTimeout  time.Duration
}

func parseConfig(args []string) (demoConfig, error) {
	cfg := demoConfig{
		Mode:          modeLocal,
		ListenAddr:    defaultListenAddr,
		GatewayURL:    defaultGatewayURL,
		Model:         defaultModel,
		Origin:        defaultOrigin,
		BearerEnv:     defaultBearerEnv,
		MaxFrameBytes: defaultMaxFrameBytes,
		MaxSessions:   defaultMaxSessions,
		Handshake:     defaultHandshake,
		IdleTimeout:   defaultIdleTimeout,
		MaxSession:    defaultMaxSession,
		DrainTimeout:  defaultDrainTimeout,
	}
	flags := flag.NewFlagSet("live-client", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&cfg.Mode, "mode", cfg.Mode, "local or production")
	flags.StringVar(&cfg.ListenAddr, "listen", cfg.ListenAddr, "HTTP listen address")
	flags.StringVar(&cfg.GatewayURL, "gateway-url", cfg.GatewayURL, "GPT Live WebSocket URL without query")
	flags.StringVar(&cfg.Model, "model", cfg.Model, "advertised gateway model")
	flags.StringVar(&cfg.Origin, "origin", cfg.Origin, "exact browser origin")
	flags.StringVar(&cfg.BearerEnv, "bearer-env", cfg.BearerEnv, "environment variable containing the bearer")
	flags.StringVar(&cfg.TLSCertFile, "tls-cert", "", "TLS certificate file")
	flags.StringVar(&cfg.TLSKeyFile, "tls-key", "", "TLS key file")
	flags.BoolVar(&cfg.BehindIngress, "behind-ingress", false, "TLS terminates at ingress")
	flags.IntVar(&cfg.MaxSessions, "max-sessions", cfg.MaxSessions, "maximum concurrent browser sessions")
	flags.DurationVar(&cfg.Handshake, "handshake-timeout", cfg.Handshake, "upstream dial timeout")
	flags.DurationVar(&cfg.IdleTimeout, "idle-timeout", cfg.IdleTimeout, "session idle timeout")
	flags.DurationVar(&cfg.MaxSession, "max-session", cfg.MaxSession, "maximum session lifetime")
	flags.DurationVar(&cfg.DrainTimeout, "drain-timeout", cfg.DrainTimeout, "shutdown drain timeout")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return demoConfig{}, errors.New("invalid demo arguments")
	}
	if err := cfg.validate(); err != nil {
		return demoConfig{}, err
	}
	return cfg, nil
}

func (cfg demoConfig) validate() error {
	if cfg.Mode != modeLocal && cfg.Mode != modeProduction {
		return errors.New("mode must be local or production")
	}
	if cfg.MaxFrameBytes <= 0 || cfg.MaxFrameBytes > 1<<20 {
		return errors.New("demo frame limit is invalid")
	}
	if cfg.MaxSessions <= 0 || cfg.MaxSessions > 1024 {
		return errors.New("demo session limit is invalid")
	}
	if cfg.Handshake < time.Second || cfg.Handshake > time.Minute ||
		cfg.IdleTimeout < 5*time.Second || cfg.IdleTimeout > 10*time.Minute ||
		cfg.MaxSession < time.Minute || cfg.MaxSession > 2*time.Hour ||
		cfg.DrainTimeout < time.Second || cfg.DrainTimeout > 2*time.Minute {
		return errors.New("demo timeouts are out of bounds")
	}
	if (cfg.TLSCertFile == "") != (cfg.TLSKeyFile == "") {
		return errors.New("TLS certificate and key must be configured together")
	}
	if err := validateListenAddr(cfg); err != nil {
		return err
	}
	if err := validateGatewayURL(cfg); err != nil {
		return err
	}
	if err := validateOrigin(cfg); err != nil {
		return err
	}
	if !boundedToken(cfg.Model, maxIdentityBytes) || strings.ContainsAny(cfg.Model, " \t\r\n") {
		return errors.New("demo model is invalid")
	}
	if !environmentName(cfg.BearerEnv) {
		return errors.New("demo bearer environment name is invalid")
	}
	if cfg.Mode == modeProduction {
		if !cfg.BehindIngress && cfg.TLSCertFile == "" {
			return errors.New("production requires TLS files or behind-ingress")
		}
		if cfg.BehindIngress && cfg.TLSCertFile != "" {
			return errors.New("production ingress mode must not set TLS files")
		}
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

func validateListenAddr(cfg demoConfig) error {
	host, port, err := net.SplitHostPort(cfg.ListenAddr)
	if err != nil || port == "" {
		return errors.New("demo listen address must be host:port")
	}
	if host == "" {
		if cfg.Mode != modeProduction {
			return errors.New("demo listen address must be loopback")
		}
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return errors.New("demo listen address must be an IP")
	}
	if cfg.Mode == modeLocal && !ip.IsLoopback() {
		return errors.New("demo listen address must be loopback")
	}
	return nil
}

func validateGatewayURL(cfg demoConfig) error {
	parsed, err := url.Parse(cfg.GatewayURL)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Opaque != "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.ForceQuery || parsed.OmitHost {
		return errors.New("demo gateway URL is invalid")
	}
	if parsed.Path != "/v1/realtime" {
		return errors.New("demo gateway path must be /v1/realtime")
	}
	host := parsed.Hostname()
	ip := net.ParseIP(host)
	switch cfg.Mode {
	case modeLocal:
		if parsed.Scheme != "ws" && parsed.Scheme != "wss" {
			return errors.New("demo gateway URL must be ws or wss")
		}
		if ip == nil || !ip.IsLoopback() {
			return errors.New("local gateway must be loopback")
		}
	default:
		if parsed.Scheme != "wss" && !(parsed.Scheme == "ws" && ip != nil && (ip.IsLoopback() || ip.IsPrivate())) {
			return errors.New("production gateway must be wss or private ws")
		}
	}
	return nil
}

func validateOrigin(cfg demoConfig) error {
	parsed, err := url.Parse(cfg.Origin)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Opaque != "" {
		return errors.New("demo origin is invalid")
	}
	if cfg.Mode == modeLocal && parsed.Scheme != "http" {
		return errors.New("local origin must be http")
	}
	if cfg.Mode == modeProduction && parsed.Scheme != "https" {
		return errors.New("production origin must be https")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return errors.New("demo origin is invalid")
	}
	if cfg.Mode == modeLocal {
		originHost, originPort, err := net.SplitHostPort(parsed.Host)
		if err != nil {
			originHost, originPort = parsed.Hostname(), "80"
		}
		listenHost, listenPort, err := net.SplitHostPort(cfg.ListenAddr)
		if err != nil {
			return errors.New("demo listen address must be host:port")
		}
		if !sameLoopbackHost(originHost, listenHost) || originPort != listenPort {
			return errors.New("demo origin must match the listen address")
		}
	}
	if strings.Contains(cfg.Origin, "*") {
		return errors.New("wildcard origins are not allowed")
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
