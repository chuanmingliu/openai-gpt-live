package config

// DefaultLocalGateway returns the credential-free, loopback-only configuration
// used by the local gateway process when no durable services are configured.
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
		return Config{}, err
	}
	return cfg, nil
}
