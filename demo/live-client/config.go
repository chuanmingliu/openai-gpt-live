package main

import (
	"errors"
	"flag"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
)

type demoConfig struct {
	ListenAddr    string
	GatewayURL    string
	Model         string
	Origin        string
	BearerEnv     string
	MaxFrameBytes int64
}

func defaultConfig() demoConfig {
	return demoConfig{
		ListenAddr:    "127.0.0.1:3000",
		GatewayURL:    "ws://127.0.0.1:8080/v1/realtime",
		Model:         "local",
		Origin:        "http://127.0.0.1:3000",
		MaxFrameBytes: 1 << 20,
	}
}

func parseConfig(args []string) (demoConfig, error) {
	cfg := defaultConfig()
	flags := flag.NewFlagSet("live-client", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&cfg.ListenAddr, "listen", cfg.ListenAddr, "loopback HTTP listen address")
	flags.StringVar(&cfg.GatewayURL, "gateway-url", cfg.GatewayURL, "GPT Live WebSocket URL without query")
	flags.StringVar(&cfg.Model, "model", cfg.Model, "advertised gateway model")
	flags.StringVar(&cfg.Origin, "origin", cfg.Origin, "exact browser origin")
	flags.StringVar(&cfg.BearerEnv, "bearer-env", cfg.BearerEnv, "environment variable containing the bearer")
	flags.Int64Var(&cfg.MaxFrameBytes, "max-frame-bytes", cfg.MaxFrameBytes, "maximum WebSocket frame size")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return demoConfig{}, errors.New("invalid demo arguments")
	}
	if err := cfg.validate(); err != nil {
		return demoConfig{}, err
	}
	return cfg, nil
}

func (cfg demoConfig) validate() error {
	listenHost, _, err := parseLoopbackAddr(cfg.ListenAddr)
	if err != nil {
		return errors.New("invalid demo listen address")
	}

	origin, err := url.Parse(cfg.Origin)
	if err != nil || origin.Scheme != "http" || origin.Host == "" || origin.Host != cfg.ListenAddr ||
		origin.User != nil || origin.Path != "" || origin.RawPath != "" || origin.RawQuery != "" ||
		origin.ForceQuery || origin.Fragment != "" || origin.RawFragment != "" || origin.Opaque != "" {
		return errors.New("invalid demo origin")
	}
	if origin.Hostname() != listenHost {
		return errors.New("invalid demo origin")
	}

	if _, err := parseGatewayURL(cfg.GatewayURL); err != nil {
		return err
	}
	if !validModelName(cfg.Model) {
		return errors.New("invalid demo model")
	}
	if cfg.BearerEnv != "" && !validEnvName(cfg.BearerEnv) {
		return errors.New("invalid demo bearer environment name")
	}
	if cfg.MaxFrameBytes < 960 || cfg.MaxFrameBytes > 1<<20 {
		return errors.New("invalid demo max frame size")
	}
	return nil
}

func parseLoopbackAddr(addr string) (string, string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return "", "", errors.New("invalid loopback address")
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return "", "", errors.New("invalid port")
	}
	return host, port, nil
}

func parseGatewayURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "ws" && u.Scheme != "wss") || u.Host == "" || u.Hostname() == "" ||
		u.User != nil || u.Path != "/v1/realtime" || u.RawPath != "" || u.RawQuery != "" ||
		u.ForceQuery || u.Fragment != "" || u.RawFragment != "" || u.Opaque != "" {
		return nil, errors.New("invalid demo gateway URL")
	}
	return u, nil
}

func validModelName(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-' || c == '.' {
			continue
		}
		return false
	}
	return true
}

func validEnvName(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if i == 0 {
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_') {
				return false
			}
			continue
		}
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
			return false
		}
	}
	return true
}

func resolveBearer(cfg demoConfig, lookup func(string) (string, bool)) ([]byte, error) {
	if cfg.BearerEnv == "" {
		return nil, nil
	}
	value, ok := lookup(cfg.BearerEnv)
	if !ok || value == "" || len(value) > 8<<10 {
		return nil, errors.New("configured demo bearer is unavailable")
	}
	return []byte(value), nil
}

func (cfg demoConfig) upstreamWebSocketURL() (*url.URL, error) {
	u, err := parseGatewayURL(cfg.GatewayURL)
	if err != nil || !validModelName(cfg.Model) {
		return nil, errors.New("invalid demo upstream URL")
	}
	copyURL := *u
	query := copyURL.Query()
	query.Set("model", cfg.Model)
	copyURL.RawQuery = query.Encode()
	return &copyURL, nil
}

func (cfg demoConfig) capabilitiesURL() (*url.URL, error) {
	u, err := parseGatewayURL(cfg.GatewayURL)
	if err != nil {
		return nil, errors.New("invalid demo capabilities URL")
	}
	copyURL := *u
	if copyURL.Scheme == "ws" {
		copyURL.Scheme = "http"
	} else {
		copyURL.Scheme = "https"
	}
	copyURL.Path = strings.TrimSuffix(copyURL.Path, "/") + "/capabilities"
	copyURL.RawPath = ""
	copyURL.RawQuery = ""
	copyURL.ForceQuery = false
	return &copyURL, nil
}
