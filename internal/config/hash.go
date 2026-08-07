package config

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"sort"
	"time"
)

const HashVersion = "v1"

// canonicalConfig is an intentional allowlist. Secret values, secret reference
// names, database URLs, private-key paths, and certificate contents are absent.
type canonicalConfig struct {
	Version   string             `json:"version"`
	Mode      string             `json:"mode"`
	Gateway   canonicalGateway   `json:"gateway"`
	Worker    canonicalWorker    `json:"worker"`
	Auth      canonicalAuth      `json:"auth"`
	Admission canonicalAdmission `json:"admission"`
	Safety    canonicalSafety    `json:"safety"`
	Telemetry canonicalTelemetry `json:"telemetry"`
	Shutdown  ShutdownConfig     `json:"shutdown"`
	Admin     AdminConfig        `json:"admin"`
	Features  canonicalFeatures  `json:"features"`
	Retention RetentionConfig    `json:"retention"`
	StoreURL  string             `json:"store_url"`
}

type canonicalGateway struct {
	AdvertisedModel string          `json:"advertised_model"`
	ListenAddr      string          `json:"listen_addr"`
	BehindIngress   bool            `json:"behind_ingress"`
	TrustedProxies  []string        `json:"trusted_proxies"`
	VoiceProvider   string          `json:"voice_provider"`
	VoiceURL        string          `json:"voice_url"`
	VoiceModel      string          `json:"voice_model"`
	Audio           AudioConfig     `json:"audio"`
	Session         SessionConfig   `json:"session"`
	WebSocket       WebSocketConfig `json:"websocket"`
	ProviderRetry   RetryConfig     `json:"provider_retry"`
	StorageGrace    time.Duration   `json:"storage_grace"`
	Reconnect       ReconnectConfig `json:"reconnect"`
}
type canonicalWorker struct {
	Concurrency       int           `json:"concurrency"`
	LeaseDuration     time.Duration `json:"lease_duration"`
	HeartbeatInterval time.Duration `json:"heartbeat_interval"`
	PollInterval      time.Duration `json:"poll_interval"`
	RequestTimeout    time.Duration `json:"request_timeout"`
	RetryBase         time.Duration `json:"retry_base"`
	RetryMax          time.Duration `json:"retry_max"`
	MaxRetries        int           `json:"max_retries"`
	MaxOutputBytes    int           `json:"max_output_bytes"`
	MaxOutputTokens   int           `json:"max_output_tokens"`
	MaxToolRounds     int           `json:"max_tool_rounds"`
	ReasonerProvider  string        `json:"reasoner_provider"`
	ReasonerURL       string        `json:"reasoner_url"`
	ReasonerModel     string        `json:"reasoner_model"`
}
type canonicalAuth struct {
	OIDCIssuer         string        `json:"oidc_issuer"`
	OIDCAudience       string        `json:"oidc_audience"`
	OIDCTenantClaim    string        `json:"oidc_tenant_claim"`
	OIDCUserClaim      string        `json:"oidc_user_claim"`
	OIDCScopeClaim     string        `json:"oidc_scope_claim"`
	OIDCRequiredScopes []string      `json:"oidc_required_scopes"`
	OIDCCacheOutage    time.Duration `json:"oidc_cache_outage"`
	OIDCHTTPTimeout    time.Duration `json:"oidc_http_timeout"`
	StaticTenantID     string        `json:"static_tenant_id"`
	StaticUserID       string        `json:"static_user_id"`
	StaticScopes       []string      `json:"static_scopes"`
	AllowedOrigins     []string      `json:"allowed_origins"`
	AllowNonBrowser    bool          `json:"allow_non_browser"`
}
type canonicalSafety struct {
	PolicyVersion       string   `json:"policy_version"`
	Mode                string   `json:"mode"`
	MaxInputBytes       int      `json:"max_input_bytes"`
	MaxSummaryBytes     int      `json:"max_summary_bytes"`
	MaxStructuredBytes  int      `json:"max_structured_bytes"`
	RedactedSummary     string   `json:"redacted_summary"`
	ClarificationPrompt string   `json:"clarification_prompt"`
	RedactionDeniedKeys []string `json:"redaction_denied_keys"`
}
type canonicalAdmission struct {
	MaxSessions, MaxSessionsPerTenant, MaxSessionsPerUser int
	LeaseDuration, HeartbeatInterval                      time.Duration
	GlobalRate                                            float64
	GlobalBurst                                           int
	IPRate                                                float64
	IPBurst                                               int
	IPKeyTTL                                              time.Duration
	MaxIPKeys                                             int
	InstanceID                                            string
}
type canonicalTelemetry struct {
	OTLPEndpoint, ServiceName, Protocol string
	ExportTimeout                       time.Duration
}
type canonicalFeatures struct {
	SideEffectingTools  bool   `json:"side_effecting_tools"`
	ToolEncryptionKeyID string `json:"tool_encryption_key_id"`
	Handoffs            bool   `json:"handoffs"`
}

func Canonical(cfg Config) ([]byte, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	c := canonicalConfig{
		Version: HashVersion, Mode: cfg.Mode,
		Gateway:   canonicalGateway{AdvertisedModel: cfg.Gateway.AdvertisedModel, ListenAddr: cfg.Gateway.ListenAddr, BehindIngress: cfg.Gateway.BehindIngress, TrustedProxies: sorted(cfg.Gateway.TrustedProxies), VoiceProvider: cfg.Gateway.Voice.Provider, VoiceURL: safeURL(cfg.Gateway.Voice.URL), VoiceModel: cfg.Gateway.Voice.Model, Audio: cfg.Gateway.Audio, Session: cfg.Gateway.Session, WebSocket: cfg.Gateway.WebSocket, ProviderRetry: cfg.Gateway.ProviderRetry, StorageGrace: cfg.Gateway.StorageGrace, Reconnect: cfg.Gateway.Reconnect},
		Worker:    canonicalWorker{Concurrency: cfg.Worker.Concurrency, LeaseDuration: cfg.Worker.LeaseDuration, HeartbeatInterval: cfg.Worker.HeartbeatInterval, PollInterval: cfg.Worker.PollInterval, RequestTimeout: cfg.Worker.RequestTimeout, RetryBase: cfg.Worker.RetryBase, RetryMax: cfg.Worker.RetryMax, MaxRetries: cfg.Worker.MaxRetries, MaxOutputBytes: cfg.Worker.MaxOutputBytes, MaxOutputTokens: cfg.Worker.MaxOutputTokens, MaxToolRounds: cfg.Worker.MaxToolRounds, ReasonerProvider: cfg.Worker.Reasoner.Provider, ReasonerURL: safeURL(cfg.Worker.Reasoner.BaseURL), ReasonerModel: cfg.Worker.Reasoner.Model},
		Auth:      canonicalAuth{OIDCIssuer: safeURL(cfg.Auth.OIDCIssuer), OIDCAudience: cfg.Auth.OIDCAudience, OIDCTenantClaim: cfg.Auth.OIDCTenantClaim, OIDCUserClaim: cfg.Auth.OIDCUserClaim, OIDCScopeClaim: cfg.Auth.OIDCScopeClaim, OIDCRequiredScopes: sorted(cfg.Auth.OIDCRequiredScopes), OIDCCacheOutage: cfg.Auth.OIDCCacheOutage, OIDCHTTPTimeout: cfg.Auth.OIDCHTTPTimeout, StaticTenantID: cfg.Auth.StaticTenantID, StaticUserID: cfg.Auth.StaticUserID, StaticScopes: sorted(cfg.Auth.StaticScopes), AllowedOrigins: sorted(cfg.Auth.AllowedOrigins), AllowNonBrowser: cfg.Auth.AllowNonBrowser},
		Admission: canonicalAdmission{MaxSessions: cfg.Admission.MaxSessions, MaxSessionsPerTenant: cfg.Admission.MaxSessionsPerTenant, MaxSessionsPerUser: cfg.Admission.MaxSessionsPerUser, LeaseDuration: cfg.Admission.LeaseDuration, HeartbeatInterval: cfg.Admission.HeartbeatInterval, GlobalRate: cfg.Admission.GlobalRate, GlobalBurst: cfg.Admission.GlobalBurst, IPRate: cfg.Admission.IPRate, IPBurst: cfg.Admission.IPBurst, IPKeyTTL: cfg.Admission.IPKeyTTL, MaxIPKeys: cfg.Admission.MaxIPKeys, InstanceID: cfg.Admission.InstanceID},
		Safety:    canonicalSafety{PolicyVersion: cfg.Safety.PolicyVersion, Mode: cfg.Safety.Mode, MaxInputBytes: cfg.Safety.MaxInputBytes, MaxSummaryBytes: cfg.Safety.MaxSummaryBytes, MaxStructuredBytes: cfg.Safety.MaxStructuredBytes, RedactedSummary: cfg.Safety.RedactedSummary, ClarificationPrompt: cfg.Safety.ClarificationPrompt, RedactionDeniedKeys: sorted(cfg.Safety.RedactionDeniedKeys)},
		Telemetry: canonicalTelemetry{OTLPEndpoint: safeURL(cfg.Telemetry.OTLPEndpoint), ServiceName: cfg.Telemetry.ServiceName, Protocol: cfg.Telemetry.Protocol, ExportTimeout: cfg.Telemetry.ExportTimeout}, Shutdown: cfg.Shutdown, Admin: cfg.Admin,
		Features: canonicalFeatures{SideEffectingTools: cfg.Features.SideEffectingTools, ToolEncryptionKeyID: cfg.Features.ToolEncryptionKeyID, Handoffs: cfg.Features.Handoffs}, Retention: cfg.Store.Retention, StoreURL: safeStoreURL(cfg.Store.URL),
	}
	return json.Marshal(c)
}

func Hash(cfg Config) (string, error) {
	canonical, err := Canonical(cfg)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return HashVersion + ":" + hex.EncodeToString(sum[:]), nil
}

func HashFile(path string) (string, error) {
	cfg, err := LoadUnresolved(path)
	if err != nil {
		return "", err
	}
	return Hash(cfg)
}

func ValidateExpected(actual, expected string) error {
	if len(actual) != 67 || len(expected) != 67 || actual[:3] != HashVersion+":" || expected[:3] != HashVersion+":" {
		return errors.New("expected config hash is malformed")
	}
	if _, err := hex.DecodeString(actual[3:]); err != nil {
		return errors.New("actual config hash is malformed")
	}
	if _, err := hex.DecodeString(expected[3:]); err != nil {
		return errors.New("expected config hash is malformed")
	}
	if subtle.ConstantTimeCompare([]byte(actual), []byte(expected)) != 1 {
		return errors.New("config hash mismatch")
	}
	return nil
}

func sorted(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}
func safeURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	u.User = nil
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
}
func safeStoreURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	u.User = nil
	query := u.Query()
	safe := url.Values{}
	for _, key := range []string{"sslmode", "application_name"} {
		if value := query.Get(key); value != "" {
			safe.Set(key, value)
		}
	}
	u.RawQuery = safe.Encode()
	u.Fragment = ""
	return u.String()
}
