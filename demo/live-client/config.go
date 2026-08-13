package main

import (
	"errors"
	"flag"
	"io"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

const maximumBearerBytes = 16 << 10

var (
	modelPattern       = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	environmentPattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,127}$`)
)

type demoConfig struct {
	ListenAddr    string
	GatewayURL    string
	Model         string
	Origin        string
	BearerEnv     string
	MaxFrameBytes int64
	Operator      operatorConfig
}

func parseConfig(args []string) (demoConfig, error) {
	cfg := demoConfig{
		ListenAddr:    "127.0.0.1:3000",
		GatewayURL:    "ws://127.0.0.1:18080/v1/realtime",
		Model:         "gpt-live-real",
		Origin:        "http://127.0.0.1:3000",
		BearerEnv:     "LIVE_DEMO_BEARER",
		MaxFrameBytes: 64 << 10,
		Operator: operatorConfig{Readiness: map[operatorComponent]string{
			componentSpeech:  "http://127.0.0.1:8766/v1/pool",
			componentGateway: "http://127.0.0.1:19090/health/ready",
			componentWorker:  "http://127.0.0.1:19091/health/ready",
			componentClient:  "http://127.0.0.1:3000/health/ready",
		}},
	}
	flags := flag.NewFlagSet("live-client", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	speechReady := cfg.Operator.Readiness[componentSpeech]
	gatewayReady := cfg.Operator.Readiness[componentGateway]
	workerReady := cfg.Operator.Readiness[componentWorker]
	clientReady := cfg.Operator.Readiness[componentClient]
	flags.StringVar(&cfg.ListenAddr, "listen", cfg.ListenAddr, "loopback HTTP listen address")
	flags.StringVar(&cfg.GatewayURL, "gateway-url", cfg.GatewayURL, "GPT Live WebSocket URL without query")
	flags.StringVar(&cfg.Model, "model", cfg.Model, "advertised gateway model")
	flags.StringVar(&cfg.Origin, "origin", cfg.Origin, "exact browser origin")
	flags.StringVar(&cfg.BearerEnv, "bearer-env", cfg.BearerEnv, "environment variable containing the bearer")
	flags.BoolVar(&cfg.Operator.Enabled, "operator", false, "enable fixed local demo operator controls")
	flags.StringVar(&cfg.Operator.StateDir, "operator-state-dir", "", "private operator state directory")
	flags.StringVar(&cfg.Operator.RepositoryRoot, "operator-repository-root", "", "fixed repository root")
	flags.StringVar(&cfg.Operator.SpeechRoot, "operator-speech-root", "", "fixed external speech trust root")
	flags.StringVar(&cfg.Operator.SpeechCommand, "operator-speech-command", "", "fixed speech launcher")
	flags.StringVar(&cfg.Operator.SpeechProfile, "operator-speech-profile", "", "fixed speech profile")
	flags.StringVar(&speechReady, "operator-speech-ready-url", speechReady, "fixed speech readiness endpoint")
	flags.StringVar(&gatewayReady, "operator-gateway-ready-url", gatewayReady, "fixed gateway readiness endpoint")
	flags.StringVar(&workerReady, "operator-worker-ready-url", workerReady, "fixed worker readiness endpoint")
	flags.StringVar(&clientReady, "operator-client-ready-url", clientReady, "fixed browser BFF readiness endpoint")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return demoConfig{}, errors.New("invalid demo arguments")
	}
	cfg.Operator.Readiness[componentSpeech] = speechReady
	cfg.Operator.Readiness[componentGateway] = gatewayReady
	cfg.Operator.Readiness[componentWorker] = workerReady
	cfg.Operator.Readiness[componentClient] = clientReady
	if err := cfg.validate(); err != nil {
		return demoConfig{}, err
	}
	return cfg, nil
}

func (cfg demoConfig) validate() error {
	host, port, err := net.SplitHostPort(cfg.ListenAddr)
	if err != nil {
		return errors.New("demo listen address must be loopback")
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return errors.New("demo listen address must be loopback")
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return errors.New("demo listen port is invalid")
	}

	origin, err := url.Parse(cfg.Origin)
	if err != nil || origin.Scheme != "http" || origin.User != nil || origin.Opaque != "" || origin.RawQuery != "" || origin.Fragment != "" || origin.Path != "" {
		return errors.New("demo origin is invalid")
	}
	if origin.Host != net.JoinHostPort(host, port) {
		return errors.New("demo origin does not match listen address")
	}

	gateway, err := url.Parse(cfg.GatewayURL)
	if err != nil || (gateway.Scheme != "ws" && gateway.Scheme != "wss") || gateway.Host == "" || gateway.User != nil || gateway.Opaque != "" || gateway.RawQuery != "" || gateway.Fragment != "" || gateway.Path != "/v1/realtime" {
		return errors.New("demo gateway URL is invalid")
	}
	if err := validateLoopbackWebSocket(gateway); err != nil {
		return err
	}
	if !modelPattern.MatchString(cfg.Model) {
		return errors.New("demo model is invalid")
	}
	if !environmentPattern.MatchString(cfg.BearerEnv) {
		return errors.New("demo bearer environment name is invalid")
	}
	if cfg.MaxFrameBytes < 1024 || cfg.MaxFrameBytes > 1<<20 {
		return errors.New("demo frame limit is invalid")
	}
	if cfg.Operator.Enabled {
		if err := cfg.Operator.validate(); err != nil {
			return errors.New("demo configuration is invalid")
		}
	}
	return nil
}

func validateLoopbackWebSocket(endpoint *url.URL) error {
	host := endpoint.Hostname()
	if host == "" || endpoint.Port() == "" {
		return errors.New("demo gateway URL is invalid")
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return errors.New("demo gateway URL must be loopback")
	}
	return nil
}

func (cfg demoConfig) upstreamURL() (*url.URL, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	upstream, err := url.Parse(cfg.GatewayURL)
	if err != nil {
		return nil, errors.New("demo gateway URL is invalid")
	}
	query := url.Values{}
	query.Set("model", cfg.Model)
	upstream.RawQuery = query.Encode()
	return upstream, nil
}

func resolveBearer(cfg demoConfig, lookup func(string) (string, bool)) ([]byte, error) {
	value, ok := lookup(cfg.BearerEnv)
	if !ok || value == "" {
		return nil, errors.New("configured demo bearer is unavailable")
	}
	if len(value) > maximumBearerBytes || strings.IndexFunc(value, unicode.IsSpace) >= 0 || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return nil, errors.New("configured demo bearer is invalid")
	}
	return []byte(value), nil
}
