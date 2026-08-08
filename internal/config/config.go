// Package config loads the typed, non-secret service configuration.
package config

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

const (
	defaultIngressFrames       = 25
	defaultEgressFrames        = 75
	defaultControlEvents       = 256
	defaultWriterControlEvents = 64
	defaultSampleRate          = 24_000
	defaultPCMFrameBytes       = 960
	defaultReconnectRetries    = 2
	defaultReconnectRetryBase  = 100 * time.Millisecond
	defaultReconnectRetryMax   = time.Second
)

type Config struct {
	Mode      string          `yaml:"mode"`
	Gateway   GatewayConfig   `yaml:"gateway"`
	Worker    WorkerConfig    `yaml:"worker"`
	Store     StoreConfig     `yaml:"store"`
	Auth      AuthConfig      `yaml:"auth"`
	Admission AdmissionConfig `yaml:"admission"`
	Safety    SafetyConfig    `yaml:"safety"`
	Telemetry TelemetryConfig `yaml:"telemetry"`
	Shutdown  ShutdownConfig  `yaml:"shutdown"`
	Admin     AdminConfig     `yaml:"admin"`
	Features  FeatureConfig   `yaml:"features"`
}

type GatewayConfig struct {
	AdvertisedModel string          `yaml:"advertised_model"`
	ListenAddr      string          `yaml:"listen_addr"`
	TLS             TLSConfig       `yaml:"tls"`
	BehindIngress   bool            `yaml:"behind_ingress"`
	TrustedProxies  []string        `yaml:"trusted_proxies"`
	Voice           VoiceConfig     `yaml:"voice"`
	Audio           AudioConfig     `yaml:"audio"`
	Session         SessionConfig   `yaml:"session"`
	WebSocket       WebSocketConfig `yaml:"websocket"`
	ProviderRetry   RetryConfig     `yaml:"provider_retry"`
	StorageGrace    time.Duration   `yaml:"storage_grace"`
	Reconnect       ReconnectConfig `yaml:"reconnect"`
}

type TLSConfig struct {
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

type VoiceConfig struct {
	Role      string `yaml:"role"`
	Provider  string `yaml:"provider"`
	URL       string `yaml:"url"`
	Model     string `yaml:"model"`
	APIKeyEnv string `yaml:"api_key_env"`
}

type AudioConfig struct {
	SampleRate     int `yaml:"sample_rate"`
	Channels       int `yaml:"channels"`
	BytesPerSample int `yaml:"bytes_per_sample"`
	FrameBytes     int `yaml:"frame_bytes"`
}

type SessionConfig struct {
	IngressFrames       int           `yaml:"ingress_frames"`
	EgressFrames        int           `yaml:"egress_frames"`
	ControlEvents       int           `yaml:"control_events"`
	WriterControlEvents int           `yaml:"writer_control_events"`
	IngressAudioBytes   int           `yaml:"ingress_audio_bytes"`
	EgressAudioBytes    int           `yaml:"egress_audio_bytes"`
	IngressBlockTimeout time.Duration `yaml:"ingress_block_timeout"`
	EgressBlockTimeout  time.Duration `yaml:"egress_block_timeout"`
}

type WebSocketConfig struct {
	MaxFrameBytes int           `yaml:"max_frame_bytes"`
	ReadTimeout   time.Duration `yaml:"read_timeout"`
	WriteTimeout  time.Duration `yaml:"write_timeout"`
}

type RetryConfig struct {
	Base       time.Duration `yaml:"base"`
	Max        time.Duration `yaml:"max"`
	MaxRetries int           `yaml:"max_retries"`
}

type ReconnectConfig struct {
	MaxRetries int           `yaml:"max_retries"`
	RetryBase  time.Duration `yaml:"retry_base"`
	RetryMax   time.Duration `yaml:"retry_max"`
}

type WorkerConfig struct {
	Concurrency       int            `yaml:"concurrency"`
	LeaseDuration     time.Duration  `yaml:"lease_duration"`
	HeartbeatInterval time.Duration  `yaml:"heartbeat_interval"`
	PollInterval      time.Duration  `yaml:"poll_interval"`
	RequestTimeout    time.Duration  `yaml:"request_timeout"`
	RetryBase         time.Duration  `yaml:"retry_base"`
	RetryMax          time.Duration  `yaml:"retry_max"`
	MaxRetries        int            `yaml:"max_retries"`
	MaxOutputBytes    int            `yaml:"max_output_bytes"`
	MaxOutputTokens   int            `yaml:"max_output_tokens"`
	MaxToolRounds     int            `yaml:"max_tool_rounds"`
	Reasoner          ReasonerConfig `yaml:"reasoner"`
}

type ReasonerConfig struct {
	Role      string `yaml:"role"`
	Provider  string `yaml:"provider"`
	BaseURL   string `yaml:"base_url"`
	Model     string `yaml:"model"`
	APIKeyEnv string `yaml:"api_key_env"`
}

type StoreConfig struct {
	DatabaseURLEnv string          `yaml:"database_url_env"`
	URL            string          `yaml:"url"`
	Retention      RetentionConfig `yaml:"retention"`
}

type RetentionConfig struct {
	ConversationTTL time.Duration `yaml:"conversation_ttl"`
	SweepInterval   time.Duration `yaml:"sweep_interval"`
}

type AuthConfig struct {
	OIDCIssuer         string        `yaml:"oidc_issuer"`
	OIDCAudience       string        `yaml:"oidc_audience"`
	OIDCTenantClaim    string        `yaml:"oidc_tenant_claim"`
	OIDCUserClaim      string        `yaml:"oidc_user_claim"`
	OIDCScopeClaim     string        `yaml:"oidc_scope_claim"`
	OIDCRequiredScopes []string      `yaml:"oidc_required_scopes"`
	OIDCCacheOutage    time.Duration `yaml:"oidc_cache_outage"`
	OIDCHTTPTimeout    time.Duration `yaml:"oidc_http_timeout"`
	StaticBearerEnv    string        `yaml:"static_bearer_env"`
	StaticTenantID     string        `yaml:"static_tenant_id"`
	StaticUserID       string        `yaml:"static_user_id"`
	StaticScopes       []string      `yaml:"static_scopes"`
	AllowedOrigins     []string      `yaml:"allowed_origins"`
	AllowNonBrowser    bool          `yaml:"allow_non_browser"`
}

type AdmissionConfig struct {
	MaxSessions          int           `yaml:"max_sessions"`
	MaxSessionsPerTenant int           `yaml:"max_sessions_per_tenant"`
	MaxSessionsPerUser   int           `yaml:"max_sessions_per_user"`
	LeaseDuration        time.Duration `yaml:"lease_duration"`
	HeartbeatInterval    time.Duration `yaml:"heartbeat_interval"`
	GlobalRate           float64       `yaml:"global_rate"`
	GlobalBurst          int           `yaml:"global_burst"`
	IPRate               float64       `yaml:"ip_rate"`
	IPBurst              int           `yaml:"ip_burst"`
	IPKeyTTL             time.Duration `yaml:"ip_key_ttl"`
	MaxIPKeys            int           `yaml:"max_ip_keys"`
	InstanceID           string        `yaml:"instance_id"`
}

type SafetyConfig struct {
	PolicyVersion       string   `yaml:"policy_version"`
	Mode                string   `yaml:"mode"`
	PrivacyHMACKeyEnv   string   `yaml:"privacy_hmac_key_env"`
	MaxInputBytes       int      `yaml:"max_input_bytes"`
	MaxSummaryBytes     int      `yaml:"max_summary_bytes"`
	MaxStructuredBytes  int      `yaml:"max_structured_bytes"`
	RedactedSummary     string   `yaml:"redacted_summary"`
	ClarificationPrompt string   `yaml:"clarification_prompt"`
	RedactionDeniedKeys []string `yaml:"redaction_denied_keys"`
}

type TelemetryConfig struct {
	OTLPEndpoint  string        `yaml:"otlp_endpoint"`
	ServiceName   string        `yaml:"service_name"`
	Protocol      string        `yaml:"protocol"`
	ExportTimeout time.Duration `yaml:"export_timeout"`
}

type ShutdownConfig struct {
	DrainTimeout        time.Duration `yaml:"drain_timeout"` // Deprecated compatibility field.
	GatewayDrainTimeout time.Duration `yaml:"gateway_drain_timeout"`
	WorkerDrainTimeout  time.Duration `yaml:"worker_drain_timeout"`
	CleanupTimeout      time.Duration `yaml:"cleanup_timeout"`
}

type AdminConfig struct {
	ListenAddr string `yaml:"listen_addr"`
}

type FeatureConfig struct {
	SideEffectingTools   bool          `yaml:"side_effecting_tools"`
	ToolEncryptionKeyEnv string        `yaml:"tool_encryption_key_env"`
	ToolEncryptionKeyID  string        `yaml:"tool_encryption_key_id"`
	Handoffs             bool          `yaml:"handoffs"`
	RefreshInterval      time.Duration `yaml:"runtime_refresh_interval"`
}

// Load reads a YAML file, resolves only secret environment names, supplies
// operational defaults, and rejects insecure or incomplete settings.
func Load(path string, lookup func(string) (string, bool)) (Config, error) {
	cfg, err := LoadUnresolved(path)
	if err != nil {
		return Config{}, err
	}
	if err := cfg.ResolveSecrets(lookup); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// LoadUnresolved parses, defaults, and structurally validates configuration
// without performing any environment lookup. Composition roots can therefore
// reject unsupported feature combinations before touching secrets.
func LoadUnresolved(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	if err := rejectPlaintextSecrets(data); err != nil {
		return Config{}, err
	}
	var cfg Config
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	cfg.applyDefaults()
	if err := cfg.validateSecretReferences(); err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// ResolveSecrets validates referenced environment names and values.
func (c Config) ResolveSecrets(lookup func(string) (string, bool)) error {
	return c.validateSecrets(lookup)
}

func (c *Config) applyDefaults() {
	if c.Mode == "" {
		c.Mode = "local"
	}
	if c.Gateway.ListenAddr == "" {
		c.Gateway.ListenAddr = ":8080"
	}
	if c.Gateway.Audio.SampleRate == 0 {
		c.Gateway.Audio.SampleRate = defaultSampleRate
	}
	if c.Gateway.Audio.Channels == 0 {
		c.Gateway.Audio.Channels = 1
	}
	if c.Gateway.Audio.BytesPerSample == 0 {
		c.Gateway.Audio.BytesPerSample = 2
	}
	if c.Gateway.Audio.FrameBytes == 0 {
		c.Gateway.Audio.FrameBytes = defaultPCMFrameBytes
	}
	if c.Gateway.Session.IngressFrames == 0 {
		c.Gateway.Session.IngressFrames = defaultIngressFrames
	}
	if c.Gateway.Session.EgressFrames == 0 {
		c.Gateway.Session.EgressFrames = defaultEgressFrames
	}
	if c.Gateway.Session.ControlEvents == 0 {
		c.Gateway.Session.ControlEvents = defaultControlEvents
	}
	if c.Gateway.Session.WriterControlEvents == 0 {
		c.Gateway.Session.WriterControlEvents = defaultWriterControlEvents
	}
	if c.Gateway.Session.IngressAudioBytes == 0 {
		c.Gateway.Session.IngressAudioBytes = 24_000
	}
	if c.Gateway.Session.EgressAudioBytes == 0 {
		c.Gateway.Session.EgressAudioBytes = 72_000
	}
	if c.Gateway.WebSocket.MaxFrameBytes == 0 {
		c.Gateway.WebSocket.MaxFrameBytes = 1 << 20
	}
	if c.Gateway.StorageGrace == 0 {
		c.Gateway.StorageGrace = 30 * time.Second
	}
	if c.Gateway.Reconnect.MaxRetries == 0 {
		c.Gateway.Reconnect.MaxRetries = defaultReconnectRetries
	}
	if c.Gateway.Reconnect.RetryBase == 0 {
		c.Gateway.Reconnect.RetryBase = defaultReconnectRetryBase
	}
	if c.Gateway.Reconnect.RetryMax == 0 {
		c.Gateway.Reconnect.RetryMax = defaultReconnectRetryMax
	}
	if c.Auth.OIDCTenantClaim == "" {
		c.Auth.OIDCTenantClaim = "tenant_id"
	}
	if c.Auth.OIDCUserClaim == "" {
		c.Auth.OIDCUserClaim = "sub"
	}
	if c.Auth.OIDCScopeClaim == "" {
		c.Auth.OIDCScopeClaim = "scope"
	}
	if c.Auth.OIDCCacheOutage == 0 {
		c.Auth.OIDCCacheOutage = 15 * time.Minute
	}
	if c.Auth.OIDCHTTPTimeout == 0 {
		c.Auth.OIDCHTTPTimeout = 5 * time.Second
	}
	if c.Admission.MaxSessions == 0 {
		c.Admission.MaxSessions = 1000
	}
	if c.Admission.MaxSessionsPerTenant == 0 {
		c.Admission.MaxSessionsPerTenant = 100
	}
	if c.Admission.MaxSessionsPerUser == 0 {
		c.Admission.MaxSessionsPerUser = 10
	}
	if c.Admission.LeaseDuration == 0 {
		c.Admission.LeaseDuration = 30 * time.Second
	}
	if c.Admission.HeartbeatInterval == 0 {
		c.Admission.HeartbeatInterval = 10 * time.Second
	}
	if c.Admission.GlobalRate == 0 {
		c.Admission.GlobalRate = 100
	}
	if c.Admission.GlobalBurst == 0 {
		c.Admission.GlobalBurst = 100
	}
	if c.Admission.IPRate == 0 {
		c.Admission.IPRate = 10
	}
	if c.Admission.IPBurst == 0 {
		c.Admission.IPBurst = 20
	}
	if c.Admission.IPKeyTTL == 0 {
		c.Admission.IPKeyTTL = 10 * time.Minute
	}
	if c.Admission.MaxIPKeys == 0 {
		c.Admission.MaxIPKeys = 10_000
	}
	if c.Worker.Concurrency == 0 {
		c.Worker.Concurrency = 1
	}
	if c.Worker.LeaseDuration == 0 {
		c.Worker.LeaseDuration = 30 * time.Second
	}
	if c.Worker.HeartbeatInterval == 0 {
		c.Worker.HeartbeatInterval = 10 * time.Second
	}
	if c.Worker.PollInterval == 0 {
		c.Worker.PollInterval = time.Second
	}
	if c.Worker.RequestTimeout == 0 {
		c.Worker.RequestTimeout = 30 * time.Second
	}
	if c.Worker.RetryBase == 0 {
		c.Worker.RetryBase = time.Second
	}
	if c.Worker.RetryMax == 0 {
		c.Worker.RetryMax = 30 * time.Second
	}
	if c.Worker.MaxRetries == 0 {
		c.Worker.MaxRetries = 3
	}
	if c.Worker.MaxOutputBytes == 0 {
		c.Worker.MaxOutputBytes = 1 << 20
	}
	if c.Worker.MaxOutputTokens == 0 {
		c.Worker.MaxOutputTokens = 8_192
	}
	if c.Worker.MaxToolRounds == 0 {
		c.Worker.MaxToolRounds = 8
	}
	if c.Safety.PolicyVersion == "" {
		c.Safety.PolicyVersion = "static-v1"
	}
	if c.Telemetry.Protocol == "" {
		c.Telemetry.Protocol = "http/protobuf"
	}
	if c.Telemetry.ExportTimeout == 0 {
		c.Telemetry.ExportTimeout = 5 * time.Second
	}
	if c.Safety.Mode == "" {
		c.Safety.Mode = "enforce"
	}
	if c.Safety.MaxSummaryBytes == 0 {
		c.Safety.MaxSummaryBytes = 16 << 10
	}
	if c.Safety.MaxStructuredBytes == 0 {
		c.Safety.MaxStructuredBytes = 64 << 10
	}
	if c.Safety.RedactedSummary == "" {
		c.Safety.RedactedSummary = "A background task completed with protected details removed."
	}
	if c.Safety.ClarificationPrompt == "" {
		c.Safety.ClarificationPrompt = "Approval is required before this task can continue."
	}
	if c.Shutdown.DrainTimeout == 0 {
		c.Shutdown.DrainTimeout = 10 * time.Second
	}
	if c.Shutdown.GatewayDrainTimeout == 0 {
		c.Shutdown.GatewayDrainTimeout = 30 * time.Second
	}
	if c.Shutdown.WorkerDrainTimeout == 0 {
		c.Shutdown.WorkerDrainTimeout = 120 * time.Second
	}
	if c.Shutdown.CleanupTimeout == 0 {
		c.Shutdown.CleanupTimeout = 5 * time.Second
	}
	if c.Features.SideEffectingTools && c.Features.ToolEncryptionKeyEnv == "" {
		c.Features.ToolEncryptionKeyEnv = "LIVE_TOOL_ENCRYPTION_KEY"
	}
	if c.Features.RefreshInterval == 0 {
		c.Features.RefreshInterval = time.Second
	}
}

func (c Config) Validate() error {
	safetyPolicyVersion := c.Safety.PolicyVersion
	if safetyPolicyVersion == "" {
		safetyPolicyVersion = "static-v1"
	}
	safetyMode := c.Safety.Mode
	if safetyMode == "" {
		safetyMode = "enforce"
	}
	safetyMaxSummary := c.Safety.MaxSummaryBytes
	if safetyMaxSummary == 0 {
		safetyMaxSummary = 16 << 10
	}
	safetyMaxStructured := c.Safety.MaxStructuredBytes
	if safetyMaxStructured == 0 {
		safetyMaxStructured = 64 << 10
	}
	safetyRedactedSummary := c.Safety.RedactedSummary
	if safetyRedactedSummary == "" {
		safetyRedactedSummary = "A background task completed with protected details removed."
	}
	safetyClarificationPrompt := c.Safety.ClarificationPrompt
	if safetyClarificationPrompt == "" {
		safetyClarificationPrompt = "Approval is required before this task can continue."
	}
	if c.Mode != "local" && c.Mode != "production" {
		return fmt.Errorf("mode must be local or production")
	}
	if c.Admin.ListenAddr != "" {
		host, _, err := net.SplitHostPort(c.Admin.ListenAddr)
		if err != nil {
			return errors.New("admin.listen_addr must include a bounded host and port")
		}
		if c.Mode == "production" {
			ip := net.ParseIP(strings.Trim(host, "[]"))
			if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
				return errors.New("production admin.listen_addr must be loopback")
			}
		}
	}
	if c.Gateway.Voice.URL != "" {
		u, err := url.Parse(c.Gateway.Voice.URL)
		if err != nil || (u.Scheme != "ws" && u.Scheme != "wss") || u.Host == "" {
			return fmt.Errorf("gateway.voice.url must be an absolute ws or wss URL")
		}
		if c.Mode == "production" && u.Scheme != "wss" {
			return fmt.Errorf("production gateway.voice.url must use wss")
		}
	}
	if err := validateHTTPURL(c.Worker.Reasoner.BaseURL, "worker.reasoner.base_url", c.Mode == "production"); err != nil {
		return err
	}
	if err := validateHTTPURL(c.Auth.OIDCIssuer, "auth.oidc_issuer", c.Mode == "production"); err != nil {
		return err
	}
	if err := validateHTTPURL(c.Telemetry.OTLPEndpoint, "telemetry.otlp_endpoint", c.Mode == "production"); err != nil {
		return err
	}
	if c.Telemetry.Protocol != "" && c.Telemetry.Protocol != "http/protobuf" || c.Telemetry.ExportTimeout < 0 || c.Telemetry.ExportTimeout > 0 && c.Telemetry.ExportTimeout < 10*time.Millisecond || c.Telemetry.ExportTimeout > time.Minute {
		return errors.New("telemetry protocol or export timeout is invalid")
	}
	if c.Gateway.Audio.SampleRate != defaultSampleRate || c.Gateway.Audio.Channels != 1 || c.Gateway.Audio.BytesPerSample != 2 {
		return errors.New("gateway.audio must be 24kHz mono PCM16LE")
	}
	if c.Gateway.Audio.FrameBytes != defaultPCMFrameBytes {
		return errors.New("gateway.audio.frame_bytes must be 960 for 20ms 24kHz mono PCM16LE")
	}
	frameBytes := c.Gateway.Audio.Channels * c.Gateway.Audio.BytesPerSample
	if c.Gateway.Session.IngressAudioBytes <= 0 || c.Gateway.Session.EgressAudioBytes <= 0 || c.Gateway.Session.IngressAudioBytes%frameBytes != 0 || c.Gateway.Session.EgressAudioBytes%frameBytes != 0 {
		return fmt.Errorf("gateway.session audio byte limits must be frame aligned (ingress_audio_bytes, egress_audio_bytes)")
	}
	if c.Gateway.Session.IngressFrames <= 0 || c.Gateway.Session.EgressFrames <= 0 || c.Gateway.Session.ControlEvents <= 0 || c.Gateway.Session.WriterControlEvents <= 0 || c.Gateway.Session.IngressFrames > 10_000 || c.Gateway.Session.EgressFrames > 10_000 || c.Gateway.Session.ControlEvents > 10_000 || c.Gateway.Session.WriterControlEvents > 10_000 {
		return errors.New("gateway session queue sizes must be positive and bounded")
	}
	if c.Gateway.StorageGrace < 0 || c.Gateway.StorageGrace > 10*time.Minute {
		return errors.New("gateway.storage_grace must be positive and bounded when set")
	}
	if c.Gateway.Reconnect.MaxRetries < 0 || c.Gateway.Reconnect.MaxRetries > 16 ||
		c.Gateway.Reconnect.RetryBase < 0 || c.Gateway.Reconnect.RetryMax < 0 ||
		c.Gateway.Reconnect.RetryBase > 0 && c.Gateway.Reconnect.RetryMax < c.Gateway.Reconnect.RetryBase ||
		c.Gateway.Reconnect.RetryMax > time.Minute {
		return errors.New("gateway reconnect retry limits must be bounded")
	}
	if c.Worker.Concurrency <= 0 {
		return errors.New("worker.concurrency must be positive")
	}
	if c.Worker.LeaseDuration <= 0 || c.Worker.HeartbeatInterval <= 0 || c.Worker.HeartbeatInterval*2 >= c.Worker.LeaseDuration {
		return errors.New("worker.heartbeat_interval must be below half worker.lease_duration")
	}
	if c.Worker.RetryBase <= 0 || c.Worker.RetryMax <= 0 || c.Worker.RetryMax < c.Worker.RetryBase || c.Worker.MaxRetries <= 0 {
		return errors.New("worker retry limits must be positive")
	}
	if c.Worker.MaxOutputBytes <= 0 || c.Worker.MaxOutputTokens <= 0 || c.Worker.MaxToolRounds <= 0 {
		return errors.New("worker output limits must be positive")
	}
	if safetyPolicyVersion != "static-v1" || safetyMode != "enforce" && safetyMode != "redact" ||
		c.Safety.MaxInputBytes < 0 || c.Safety.MaxInputBytes > 1<<20 || safetyMaxSummary <= 0 || safetyMaxSummary > 16<<10 ||
		safetyMaxStructured <= 0 || safetyMaxStructured > 64<<10 ||
		strings.TrimSpace(safetyRedactedSummary) == "" || len(safetyRedactedSummary) > safetyMaxSummary || !utf8.ValidString(safetyRedactedSummary) ||
		strings.TrimSpace(safetyClarificationPrompt) == "" || len(safetyClarificationPrompt) > safetyMaxSummary || !utf8.ValidString(safetyClarificationPrompt) ||
		len(c.Safety.RedactionDeniedKeys) > 128 {
		return errors.New("safety policy configuration is invalid")
	}
	for _, key := range c.Safety.RedactionDeniedKeys {
		if strings.TrimSpace(key) == "" || len(key) > 128 || !utf8.ValidString(key) {
			return errors.New("safety redaction denied keys must be non-empty and bounded")
		}
	}
	if c.Shutdown.DrainTimeout < 0 || c.Shutdown.DrainTimeout > 10*time.Minute {
		return errors.New("shutdown.drain_timeout must be positive and bounded when set")
	}
	if c.Shutdown.GatewayDrainTimeout < 0 || c.Shutdown.GatewayDrainTimeout > 10*time.Minute || c.Shutdown.WorkerDrainTimeout < 0 || c.Shutdown.WorkerDrainTimeout > 10*time.Minute || c.Shutdown.CleanupTimeout < 0 || c.Shutdown.CleanupTimeout > time.Minute {
		return errors.New("shutdown service drain and cleanup timeouts must be positive and bounded")
	}
	if len(c.Auth.AllowedOrigins) > 128 || len(c.Auth.OIDCRequiredScopes) > 64 || len(c.Auth.StaticScopes) > 64 ||
		c.Auth.OIDCCacheOutage < 0 || c.Auth.OIDCCacheOutage > 24*time.Hour || c.Auth.OIDCCacheOutage > 0 && c.Auth.OIDCCacheOutage < time.Second || c.Auth.OIDCHTTPTimeout < 0 || c.Auth.OIDCHTTPTimeout > 30*time.Second || c.Auth.OIDCHTTPTimeout > 0 && c.Auth.OIDCHTTPTimeout < 100*time.Millisecond {
		return errors.New("authentication lists and cache outage must be bounded")
	}
	for _, values := range [][]string{c.Auth.AllowedOrigins, c.Auth.OIDCRequiredScopes, c.Auth.StaticScopes} {
		for _, value := range values {
			if value == "" || len(value) > 2048 || !utf8.ValidString(value) {
				return errors.New("authentication values must be non-empty and bounded")
			}
		}
	}
	for _, origin := range c.Auth.AllowedOrigins {
		u, err := url.Parse(origin)
		if err != nil || strings.Contains(origin, "*") || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
			return errors.New("auth.allowed_origins must contain exact HTTP origins")
		}
	}
	if len(c.Gateway.TrustedProxies) > 128 {
		return errors.New("gateway.trusted_proxies is too large")
	}
	for _, proxy := range c.Gateway.TrustedProxies {
		if _, _, err := net.ParseCIDR(proxy); err != nil {
			return errors.New("gateway.trusted_proxies must contain CIDRs")
		}
	}
	if c.Admission.MaxSessions < 0 || c.Admission.MaxSessionsPerTenant < 0 || c.Admission.MaxSessionsPerUser < 0 ||
		c.Admission.MaxSessions > 1_000_000 || c.Admission.MaxSessions > 0 && c.Admission.MaxSessionsPerTenant > c.Admission.MaxSessions || c.Admission.MaxSessionsPerTenant > 0 && c.Admission.MaxSessionsPerUser > c.Admission.MaxSessionsPerTenant ||
		c.Admission.LeaseDuration < 0 || c.Admission.LeaseDuration > 24*time.Hour || c.Admission.LeaseDuration > 0 && c.Admission.LeaseDuration < 3*time.Second || c.Admission.HeartbeatInterval < 0 || c.Admission.HeartbeatInterval > 0 && c.Admission.HeartbeatInterval < time.Second || c.Admission.LeaseDuration > 0 && c.Admission.HeartbeatInterval*2 >= c.Admission.LeaseDuration ||
		c.Admission.GlobalRate < 0 || c.Admission.IPRate < 0 || c.Admission.GlobalBurst < 0 || c.Admission.IPBurst < 0 || c.Admission.IPKeyTTL < 0 || c.Admission.IPKeyTTL > 24*time.Hour || c.Admission.IPKeyTTL > 0 && c.Admission.IPKeyTTL < time.Second || c.Admission.MaxIPKeys < 0 || c.Admission.MaxIPKeys > 1_000_000 {
		return errors.New("admission limits must be positive, ordered, and bounded")
	}
	if c.Admission.InstanceID != "" && (!validKeyID(c.Admission.InstanceID) || len(c.Admission.InstanceID) > 256) {
		return errors.New("admission.instance_id is invalid")
	}
	if c.Features.ToolEncryptionKeyEnv != "" {
		if !validKeyID(c.Features.ToolEncryptionKeyID) {
			return errors.New("features.tool_encryption_key_id must be a non-empty immutable key identifier")
		}
	} else if c.Features.ToolEncryptionKeyID != "" {
		return errors.New("features.tool_encryption_key_id requires tool_encryption_key_env")
	}
	if featureRefreshInterval(c) < 10*time.Millisecond || featureRefreshInterval(c) > time.Minute {
		return errors.New("features.runtime_refresh_interval must be bounded")
	}
	if c.Mode == "production" {
		hasCert, hasKey := c.Gateway.TLS.CertFile != "", c.Gateway.TLS.KeyFile != ""
		if hasCert != hasKey {
			return errors.New("production direct TLS requires both certificate and key")
		}
		if c.Gateway.BehindIngress {
			if hasCert || len(c.Gateway.TrustedProxies) == 0 {
				return errors.New("production ingress mode requires trusted proxies and no direct TLS files")
			}
		} else if !hasCert || len(c.Gateway.TrustedProxies) > 0 {
			return errors.New("production direct mode requires TLS certificate and key without ingress proxies")
		}
		if strings.Contains(c.Store.URL, "sslmode=disable") {
			return errors.New("production store.url must not disable SSL")
		}
		if c.Auth.StaticBearerEnv != "" {
			return errors.New("production static authentication is not allowed")
		}
		if c.Auth.OIDCIssuer == "" || c.Auth.OIDCAudience == "" || len(c.Auth.OIDCRequiredScopes) == 0 {
			return errors.New("production OIDC issuer, audience, and required scopes are required")
		}
		if len(c.Auth.AllowedOrigins) == 0 && !c.Auth.AllowNonBrowser {
			return errors.New("production origin or non-browser policy is required")
		}
		if c.Store.URL == "" && c.Store.DatabaseURLEnv == "" {
			return errors.New("production admission requires PostgreSQL storage")
		}
		for _, origin := range c.Auth.AllowedOrigins {
			if strings.Contains(origin, "*") {
				return errors.New("production wildcard origins are not allowed")
			}
		}
	}
	if c.Mode == "local" && c.Auth.StaticBearerEnv != "" && (strings.TrimSpace(c.Auth.StaticTenantID) == "" || strings.TrimSpace(c.Auth.StaticUserID) == "" || len(c.Auth.StaticScopes) == 0) {
		return errors.New("static authentication requires configured tenant, user, and scopes")
	}
	return nil
}

func featureRefreshInterval(c Config) time.Duration {
	if c.Features.RefreshInterval == 0 {
		return time.Second
	}
	return c.Features.RefreshInterval
}

func validateHTTPURL(raw, name string, requireHTTPS bool) error {
	if raw == "" {
		return nil
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("%s must be an absolute http or https URL", name)
	}
	if requireHTTPS && u.Scheme != "https" {
		return fmt.Errorf("production %s must use https", name)
	}
	return nil
}

func (c Config) validateSecrets(lookup func(string) (string, bool)) error {
	if err := c.validateSecretReferences(); err != nil {
		return err
	}
	if lookup == nil {
		lookup = os.LookupEnv
	}
	for _, name := range []string{c.Gateway.Voice.APIKeyEnv, c.Worker.Reasoner.APIKeyEnv, c.Store.DatabaseURLEnv, c.Auth.StaticBearerEnv, c.Safety.PrivacyHMACKeyEnv} {
		if name == "" {
			continue
		}
		value, ok := lookup(name)
		if !ok {
			return fmt.Errorf("required secret environment variable %q is not set", name)
		}
		if name == c.Store.DatabaseURLEnv && c.Mode == "production" && strings.Contains(value, "sslmode=disable") {
			return errors.New("production database URL must not disable SSL")
		}
	}
	if c.Features.ToolEncryptionKeyEnv != "" {
		name := c.Features.ToolEncryptionKeyEnv
		value, ok := lookup(name)
		if !ok {
			return fmt.Errorf("required secret environment variable %q is not set", name)
		}
		key, err := base64.StdEncoding.DecodeString(value)
		if err != nil || len(key) != 32 {
			return fmt.Errorf("%s must contain a base64-encoded 32-byte key", name)
		}
	}
	return nil
}

func (c Config) validateSecretReferences() error {
	for _, name := range []string{c.Gateway.Voice.APIKeyEnv, c.Worker.Reasoner.APIKeyEnv, c.Store.DatabaseURLEnv, c.Auth.StaticBearerEnv, c.Safety.PrivacyHMACKeyEnv, c.Features.ToolEncryptionKeyEnv} {
		if name != "" && !environmentName(name) {
			return fmt.Errorf("secret environment variable name %q is invalid", name)
		}
	}
	return nil
}

func environmentName(value string) bool {
	if value == "" || value[0] < 'A' || value[0] > 'Z' {
		return false
	}
	for _, r := range value[1:] {
		if (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' {
			return false
		}
	}
	return true
}

func validKeyID(value string) bool {
	if value == "" || len(value) > 256 || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if (r < 'A' || r > 'Z') && (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' && r != '_' && r != '.' {
			return false
		}
	}
	return true
}

func rejectPlaintextSecrets(data []byte) error {
	var node yaml.Node
	if err := yaml.Unmarshal(data, &node); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	var walk func(*yaml.Node) error
	walk = func(n *yaml.Node) error {
		if n.Kind == yaml.MappingNode {
			for i := 0; i+1 < len(n.Content); i += 2 {
				key, value := n.Content[i], n.Content[i+1]
				lower := strings.ToLower(key.Value)
				if (strings.Contains(lower, "secret") || strings.Contains(lower, "password") || strings.Contains(lower, "api_key") || strings.Contains(lower, "token") || strings.Contains(lower, "hmac_key") || strings.Contains(lower, "encryption_key")) && !strings.HasSuffix(lower, "_env") && !strings.HasSuffix(lower, "_id") && value.Kind == yaml.ScalarNode && value.Value != "" {
					return fmt.Errorf("plaintext secret %q is not allowed in YAML; use an environment variable name", key.Value)
				}
				if lower == "url" && value.Kind == yaml.ScalarNode {
					if containsDatabasePassword(value.Value) {
						return fmt.Errorf("plaintext secret in %q is not allowed in YAML; use an environment variable name", key.Value)
					}
				}
				if err := walk(value); err != nil {
					return err
				}
			}
			return nil
		}
		for _, child := range n.Content {
			if err := walk(child); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(&node)
}

var libPQPassword = regexp.MustCompile(`(?i)(?:^|\s)(?:password|pass|passwd|pwd)\s*=`)

func containsDatabasePassword(value string) bool {
	if parsed, err := url.Parse(value); err == nil {
		if parsed.User != nil {
			return true
		}
		for key := range parsed.Query() {
			switch strings.ToLower(key) {
			case "password", "pass", "passwd", "pwd":
				return true
			}
		}
	}
	return libPQPassword.MatchString(value)
}
