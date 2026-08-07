package s2sws

import (
	"encoding/json"
	"testing"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
	"github.com/chuanmingliu/openai-gpt-live/internal/realtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPresentationWireKeepsDurableBytesOutOfPrivilegedInstructions(t *testing.T) {
	const durable = "DURABLE_UNTRUSTED_BYTES"
	raw, err := marshalCommand(realtime.Command{
		Kind: realtime.CommandCreateResponse, Epoch: 7,
		Response: realtime.ResponseConfig{
			Instructions: "Fixed presentation instruction.",
			ToolChoice:   json.RawMessage(`"none"`),
			Input:        json.RawMessage(`[{"type":"message","role":"assistant","content":[{"type":"input_text","text":"` + durable + `"}]}]`),
		},
	})
	require.NoError(t, err)
	var wire struct {
		EventID  string `json:"event_id"`
		Response struct {
			Instructions string          `json:"instructions"`
			ToolChoice   json.RawMessage `json:"tool_choice"`
			Input        []struct {
				Role string `json:"role"`
			} `json:"input"`
		} `json:"response"`
	}
	require.NoError(t, json.Unmarshal(raw, &wire))
	assert.Equal(t, "live-response-create-7", wire.EventID)
	assert.NotContains(t, wire.Response.Instructions, durable)
	assert.JSONEq(t, `"none"`, string(wire.Response.ToolChoice))
	require.Len(t, wire.Response.Input, 1)
	assert.Equal(t, "assistant", wire.Response.Input[0].Role)
}

func TestProviderErrorMapsCausingResponseCreateToEpoch(t *testing.T) {
	mapped, err := mapServerEvent([]byte(`{
		"type":"error","event_id":"provider-error",
		"error":{"type":"invalid_request_error","event_id":"live-response-create-19","message":"private detail"}
	}`), 4096)
	require.NoError(t, err)
	assert.Equal(t, realtime.EventError, mapped.event.Kind)
	assert.Equal(t, identity.ResponseEpoch(19), mapped.event.Epoch)
	assert.Equal(t, "private detail", mapped.event.Reason)

	uncorrelated, err := mapServerEvent([]byte(`{"type":"error","event_id":"provider-error","error":{"message":"generic"}}`), 4096)
	require.NoError(t, err)
	assert.Zero(t, uncorrelated.event.Epoch)
}
