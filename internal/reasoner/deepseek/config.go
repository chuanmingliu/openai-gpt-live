package deepseek

import (
	"fmt"
	"net/url"
	"strings"
	"unicode/utf8"
)

const (
	defaultMaxRequestBytes      = 1 << 20
	defaultMaxOutputBytes       = 1 << 20
	defaultMaxToolCalls         = 64
	defaultMaxToolArgumentBytes = 256 << 10
	maxSSEEventBytes            = 1 << 20
	maxErrorBodyBytes           = 64 << 10
)

type Config struct {
	BaseURL              string
	APIKey               string
	MaxRequestBytes      int
	MaxOutputBytes       int
	MaxToolCalls         int
	MaxToolArgumentBytes int
}

func (config Config) validated() (Config, *url.URL, error) {
	if config.MaxRequestBytes == 0 {
		config.MaxRequestBytes = defaultMaxRequestBytes
	}
	if config.MaxOutputBytes == 0 {
		config.MaxOutputBytes = defaultMaxOutputBytes
	}
	if config.MaxToolCalls == 0 {
		config.MaxToolCalls = defaultMaxToolCalls
	}
	if config.MaxToolArgumentBytes == 0 {
		config.MaxToolArgumentBytes = defaultMaxToolArgumentBytes
	}
	if config.MaxRequestBytes <= 0 || config.MaxRequestBytes > 16<<20 || config.MaxOutputBytes <= 0 || config.MaxOutputBytes > 16<<20 || config.MaxToolCalls <= 0 || config.MaxToolCalls > 1024 || config.MaxToolArgumentBytes <= 0 || config.MaxToolArgumentBytes > 16<<20 {
		return Config{}, nil, fmt.Errorf("deepseek: invalid bounds")
	}
	if len(config.APIKey) > 16<<10 || !validAPIKey(config.APIKey) {
		return Config{}, nil, fmt.Errorf("deepseek: invalid API key")
	}
	base, err := url.Parse(config.BaseURL)
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		return Config{}, nil, fmt.Errorf("deepseek: base URL must be an absolute HTTP URL without credentials, query, or fragment")
	}
	endpoint := *base
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/chat/completions"
	endpoint.RawPath = ""
	return config, &endpoint, nil
}

func validAPIKey(key string) bool {
	if !utf8.ValidString(key) {
		return false
	}
	for index := range len(key) {
		if key[index] <= 0x20 || key[index] == 0x7f {
			return false
		}
	}
	return true
}
