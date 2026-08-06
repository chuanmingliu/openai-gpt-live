package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLoadAppliesRealtimeDefaults catches a loader regression that leaves realtime
// queues or PCM sample rate at zero when the operator omits optional limits.
func TestLoadAppliesRealtimeDefaults(t *testing.T) {
	cfg, err := loadFixture(t, "gateway:\n  voice:\n    url: ws://127.0.0.1:8000/v1/realtime\n", nil)
	require.NoError(t, err)
	assert.Equal(t, 25, cfg.Gateway.Session.IngressFrames)
	assert.Equal(t, 75, cfg.Gateway.Session.EgressFrames)
	assert.Equal(t, 256, cfg.Gateway.Session.ControlEvents)
	assert.Equal(t, 64, cfg.Gateway.Session.WriterControlEvents)
	assert.Equal(t, 24_000, cfg.Gateway.Audio.SampleRate)
}

// TestRejectsNonWebSocketVoiceURL catches a validator regression that admits a
// provider endpoint the WebSocket client cannot use.
func TestRejectsNonWebSocketVoiceURL(t *testing.T) {
	_, err := loadFixture(t, "gateway:\n  voice:\n    url: https://voice.example/v1/realtime\n", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "gateway.voice.url")
}

// TestRejectsNonFrameAlignedAudioLimits catches a validator regression that allows
// queue byte limits to split a 16-bit mono PCM frame.
func TestRejectsNonFrameAlignedAudioLimits(t *testing.T) {
	_, err := loadFixture(t, "gateway:\n  audio:\n    sample_rate: 24000\n    channels: 1\n    bytes_per_sample: 2\n  session:\n    ingress_audio_bytes: 501\n", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ingress_audio_bytes")
}

// TestMissingSecretEnvironmentVariable catches a loader regression that starts a
// configured provider without resolving its named credential.
func TestMissingSecretEnvironmentVariable(t *testing.T) {
	_, err := loadFixture(t, "worker:\n  reasoner:\n    api_key_env: DEEPSEEK_API_KEY\n", func(string) (string, bool) { return "", false })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DEEPSEEK_API_KEY")
}

func loadFixture(t *testing.T, yaml string, lookup func(string) (string, bool)) (Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(strings.TrimSpace(yaml)+"\n"), 0o600))
	return Load(path, lookup)
}
