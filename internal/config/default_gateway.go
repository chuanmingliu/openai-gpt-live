package config

import "fmt"

// DefaultLocalGateway returns the loopback-only, credential-free backend
// configuration used when live-gateway starts without -config.
func DefaultLocalGateway() (Config, error) {
	cfg := Config{
		Mode: "local",
		Gateway: GatewayConfig{
			AdvertisedModel: "local",
			ListenAddr:      "127.0.0.1:8080",
			Voice: VoiceConfig{
				Role:     "realtime_voice",
				Provider: "speech_to_speech",
				URL:      "ws://127.0.0.1:8765/v1/realtime",
				Model:    "local",
			},
		},
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("validate default local gateway configuration: %w", err)
	}
	return cfg, nil
}
