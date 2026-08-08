//go:build liveprovider

package liveprovider_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/reasoner"
	"github.com/chuanmingliu/openai-gpt-live/internal/reasoner/deepseek"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const liveProviderTimeout = 45 * time.Second

type liveConfig struct {
	baseURL string
	apiKey  string
	model   string
}

func configuredLiveProvider(t *testing.T) liveConfig {
	t.Helper()
	config := liveConfig{
		baseURL: os.Getenv("DEEPSEEK_BASE_URL"),
		apiKey:  os.Getenv("DEEPSEEK_API_KEY"),
		model:   os.Getenv("DEEPSEEK_MODEL"),
	}
	if config.baseURL == "" || config.apiKey == "" || config.model == "" {
		t.Skip("DeepSeek live-provider suite requires DEEPSEEK_BASE_URL, DEEPSEEK_API_KEY, and DEEPSEEK_MODEL")
	}
	return config
}

func liveClient(t *testing.T, config liveConfig) *deepseek.Client {
	t.Helper()
	client, err := deepseek.New(deepseek.Config{
		BaseURL: config.baseURL, APIKey: config.apiKey,
		MaxRequestBytes: 16 << 10, MaxOutputBytes: 8 << 10, MaxToolCalls: 2, MaxToolArgumentBytes: 1024,
	}, &http.Client{Timeout: liveProviderTimeout})
	if err != nil {
		t.Fatal("DeepSeek live-provider configuration is invalid")
	}
	return client
}

func boundedRequest(config liveConfig) reasoner.Request {
	return reasoner.Request{
		Model:           config.model,
		Messages:        []reasoner.Message{{Role: reasoner.RoleUser, Content: "Return one short synthetic acknowledgement."}},
		MaxOutputTokens: 16,
		SafetyUserID:    "live-provider-contract",
	}
}

func TestDeepSeekAuthenticationModelSSEAndUsage(t *testing.T) {
	config := configuredLiveProvider(t)
	client := liveClient(t, config)
	ctx, cancel := context.WithTimeout(t.Context(), liveProviderTimeout)
	defer cancel()
	var chunks atomic.Int32
	result, err := client.Stream(ctx, boundedRequest(config), func(chunk reasoner.Chunk) error {
		chunks.Add(1)
		// Deliberately inspect only lifecycle and bounds. Provider content is
		// neither logged nor retained by this suite.
		if len(chunk.Text) > 8<<10 || len(chunk.ToolCalls) > 2 {
			return errors.New("bounded provider chunk exceeded contract")
		}
		return nil
	})
	if err != nil {
		t.Fatal("attempted DeepSeek authentication/SSE request failed")
	}
	assert.NotEmpty(t, result.ProviderRequestID)
	assert.NotEmpty(t, result.FinishReason)
	assert.Positive(t, chunks.Load())
	assert.Positive(t, result.Usage.PromptTokens)
	assert.GreaterOrEqual(t, result.Usage.CompletionTokens, 0)
	assert.Equal(t, result.Usage.PromptTokens+result.Usage.CompletionTokens, result.Usage.TotalTokens)
}

func TestDeepSeekCancellationPropagatesAfterRequestStarts(t *testing.T) {
	started := make(chan struct{})
	cancelObserved := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if flush, ok := w.(http.Flusher); ok {
			flush.Flush()
		}
		close(started)
		<-r.Context().Done()
		close(cancelObserved)
	}))
	t.Cleanup(server.Close)
	client, err := deepseek.New(deepseek.Config{BaseURL: server.URL, MaxRequestBytes: 16 << 10, MaxOutputBytes: 8 << 10}, server.Client())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() {
		_, streamErr := client.Stream(ctx, boundedRequest(liveConfig{model: "synthetic-model"}), func(reasoner.Chunk) error { return nil })
		result <- streamErr
	}()
	select {
	case <-started:
		cancel()
	case <-time.After(liveProviderTimeout):
		cancel()
		t.Fatal("deterministic provider request did not start")
	}
	select {
	case err := <-result:
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("DeepSeek cancellation did not terminate promptly")
	}
	select {
	case <-cancelObserved:
	case <-time.After(time.Second):
		t.Fatal("provider transport did not observe request cancellation")
	}
}

func TestDeepSeekLiveCancellationAfterFirstChunk(t *testing.T) {
	config := configuredLiveProvider(t)
	client := liveClient(t, config)
	deadline, stop := context.WithTimeout(t.Context(), liveProviderTimeout)
	defer stop()
	ctx, cancel := context.WithCancel(deadline)
	defer cancel()
	firstChunk := make(chan struct{})
	result := make(chan error, 1)
	var once sync.Once
	go func() {
		_, err := client.Stream(ctx, boundedRequest(config), func(reasoner.Chunk) error {
			once.Do(func() {
				close(firstChunk)
				cancel()
			})
			return nil
		})
		result <- err
	}()
	select {
	case <-firstChunk:
	case err := <-result:
		if err == nil {
			t.Fatal("live request completed before the cancellation boundary")
		}
		t.Fatal("attempted live cancellation request failed before its first chunk")
	case <-deadline.Done():
		t.Fatal("attempted live cancellation request timed out before its first chunk")
	}
	select {
	case err := <-result:
		require.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled))
	case <-time.After(time.Second):
		t.Fatal("live provider cancellation did not terminate promptly")
	}
}
