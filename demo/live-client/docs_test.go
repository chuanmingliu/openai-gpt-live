package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealtimeBrowserOperatorGuideHasExactStepsWithoutSecrets(t *testing.T) {
	content, err := os.ReadFile("../../docs/demo/realtime-browser-client.md")
	require.NoError(t, err)
	text := string(content)
	for _, heading := range []string{
		"1. Configure the speech service",
		"2. Configure the gateway",
		"3. Start the browser BFF",
		"4. Verify audible playback and barge-in",
		"5. Stop only the demo processes",
	} {
		assert.Contains(t, text, heading)
	}
	assert.NotRegexp(t, `(?m)^(S2S_API_KEY|LIVE_DEMO_BEARER|DEEPSEEK_API_KEY)=\S+`, text)
}
