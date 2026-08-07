package config

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToolEncryptionKeyRequiresImmutableConfiguredID(t *testing.T) {
	key := base64.StdEncoding.EncodeToString(make([]byte, 32))
	lookup := func(name string) (string, bool) { return key, name == "TOOL_KEY" }
	_, err := loadFixture(t, "features:\n  tool_encryption_key_env: TOOL_KEY\n", lookup)
	assert.ErrorContains(t, err, "tool_encryption_key_id")
	cfg, err := loadFixture(t, "features:\n  tool_encryption_key_env: TOOL_KEY\n  tool_encryption_key_id: rotation-2026-v2\n", lookup)
	require.NoError(t, err)
	assert.Equal(t, "rotation-2026-v2", cfg.Features.ToolEncryptionKeyID)
	_, err = loadFixture(t, "features:\n  tool_encryption_key_env: TOOL_KEY\n  tool_encryption_key_id: invalid/key\n", lookup)
	assert.ErrorContains(t, err, "tool_encryption_key_id")
}

func TestShutdownDrainTimeoutDefaultsAndIsBounded(t *testing.T) {
	cfg, err := loadFixture(t, "mode: local\n", nil)
	require.NoError(t, err)
	assert.Equal(t, 10*time.Second, cfg.Shutdown.DrainTimeout)
	_, err = loadFixture(t, "shutdown:\n  drain_timeout: -1s\n", nil)
	assert.ErrorContains(t, err, "drain_timeout")
}

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

// TestRejectsDatabasePasswordInURLQuery catches a secret scanner regression that
// accepts a PostgreSQL URL whose password is supplied as a query parameter.
func TestRejectsDatabasePasswordInURLQuery(t *testing.T) {
	_, err := loadFixture(t, "store:\n  url: postgres://db.example/live?password=secret&sslmode=require\n", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "plaintext secret")
}

// TestRejectsDatabasePasswordInLibPQDSN catches a secret scanner regression that
// accepts a libpq-style PostgreSQL password assignment in checked-in YAML.
func TestRejectsDatabasePasswordInLibPQDSN(t *testing.T) {
	_, err := loadFixture(t, "store:\n  url: host=db.example dbname=live password=secret sslmode=require\n", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "plaintext secret")
}

// TestLoadDefaultsPCMFrameBytes catches a loader regression that omits the fixed
// 20 ms frame size required by the 24 kHz mono PCM16LE transport contract.
func TestLoadDefaultsPCMFrameBytes(t *testing.T) {
	cfg, err := loadFixture(t, "gateway:\n  voice:\n    url: ws://127.0.0.1:8000/v1/realtime\n", nil)
	require.NoError(t, err)
	assert.Equal(t, 960, cfg.Gateway.Audio.FrameBytes)
}

// TestRejectsPCMFrameBytesOutsideTwentyMilliseconds catches a validator
// regression that allows a frame size other than 20 ms of PCM16LE samples.
func TestRejectsPCMFrameBytesOutsideTwentyMilliseconds(t *testing.T) {
	_, err := loadFixture(t, "gateway:\n  audio:\n    sample_rate: 24000\n    channels: 1\n    bytes_per_sample: 2\n    frame_bytes: 480\n", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be 960")
}

func loadFixture(t *testing.T, yaml string, lookup func(string) (string, bool)) (Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(strings.TrimSpace(yaml)+"\n"), 0o600))
	return Load(path, lookup)
}
