package main

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealtimeBrowserOperatorGuideHasExactStepsWithoutSecrets(t *testing.T) {
	text := readOperatorGuide(t)
	for _, heading := range []string{
		"1. Configure the speech service",
		"2. Configure the gateway",
		"3. Start the browser BFF",
		"4. Verify audible playback and barge-in",
		"5. Stop only the demo processes",
	} {
		assert.Contains(t, text, heading)
	}
	assert.False(t, hasCredentialLiteralAssignment(text))
}

func TestCredentialLiteralAssignmentDetector(t *testing.T) {
	tests := []struct {
		name string
		text string
		want bool
	}{
		{name: "exported unquoted speech secret", text: "export S2S_API_KEY=real-secret", want: true},
		{name: "indented OpenAI secret", text: "  OPENAI_API_KEY=real-secret", want: true},
		{name: "double quoted gateway secret", text: `LIVE_STATIC_BEARER_TOKEN="real-secret"`, want: true},
		{name: "single quoted demo secret", text: `export LIVE_DEMO_BEARER='real-secret'`, want: true},
		{name: "future phase DeepSeek secret", text: `DEEPSEEK_API_KEY="real-secret"`, want: true},
		{name: "symbolic gateway bearer copy", text: `export LIVE_DEMO_BEARER="$LIVE_STATIC_BEARER_TOKEN"`, want: false},
		{name: "braced symbolic gateway bearer copy", text: `LIVE_DEMO_BEARER="${LIVE_STATIC_BEARER_TOKEN}"`, want: false},
		{name: "hidden read and name-only export", text: "read -r -s \"S2S_API_KEY?Speech bearer: \"\nexport S2S_API_KEY", want: false},
		{name: "unrelated assignment", text: "SPEECH_MODEL=local", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, hasCredentialLiteralAssignment(test.text))
		})
	}
}

func TestRealtimeBrowserOperatorGuideUsesBoundedContentFreeReadinessChecks(t *testing.T) {
	text := readOperatorGuide(t)
	assert.Contains(t, text, "for attempt in {1..20}; do")
	assert.Contains(t, text, "sleep 0.1")
	assert.Contains(t, text, "20 attempts x (0.1-second curl maximum + 0.1-second sleep) = 4.0 seconds")
	assert.Contains(t, text, `wait_for_ready gateway "$GATEWAY_PID" http://127.0.0.1:19090/health/ready`)
	assert.Contains(t, text, `wait_for_ready browser-bff "$BFF_PID" http://127.0.0.1:3000/health/ready`)
	assert.Contains(t, text, "--output /dev/null")
	assert.NotRegexp(t, `(?m)^curl .*http://127\.0\.0\.1:(19090|3000)/health/ready$`, text)
	var readinessCurls []string
	lines := strings.Split(text, "\n")
	for index := 0; index < len(lines); index++ {
		command := strings.TrimSpace(lines[index])
		if strings.HasPrefix(command, "if curl ") {
			command = strings.TrimPrefix(command, "if ")
		} else if !strings.HasPrefix(command, "curl ") {
			continue
		}
		for strings.HasSuffix(command, `\`) && index+1 < len(lines) {
			index++
			command = strings.TrimSuffix(command, `\`) + strings.TrimSpace(lines[index])
		}
		readinessCurls = append(readinessCurls, command)
	}
	require.NotEmpty(t, readinessCurls)
	for _, command := range readinessCurls {
		assert.Contains(t, command, "--connect-timeout 0.05")
		assert.Contains(t, command, "--max-time 0.1")
	}
}

func TestRealtimeBrowserOperatorGuideChecksIdleStateAndBrowserBearerSurfaces(t *testing.T) {
	text := readOperatorGuide(t)
	for _, exact := range []string{
		`http://127.0.0.1:8765/v1/pool`,
		`.in_use == 0`,
		`all(.units[]; .state == "idle")`,
		`http://127.0.0.1:19090/admin/v1/sessions`,
		`.active == 0`,
		`Authorization: Bearer $LIVE_STATIC_BEARER_TOKEN`,
		`Address bar`,
		`Sources`,
		`Network`,
		`Application`,
		`Console`,
		`LIVE_STATIC_BEARER_TOKEN`,
		`LIVE_DEMO_BEARER`,
	} {
		assert.Contains(t, text, exact)
	}
	assert.Contains(t, text, `>/dev/null`)
	assert.Contains(t, text, "Do not paste the bearer value into DevTools")
}

func readOperatorGuide(t *testing.T) string {
	t.Helper()
	content, err := os.ReadFile("../../docs/demo/realtime-browser-client.md")
	require.NoError(t, err)
	return string(content)
}

var credentialAssignmentPattern = regexp.MustCompile(`^[ \t]*(export[ \t]+)?(S2S_API_KEY|OPENAI_API_KEY|LIVE_STATIC_BEARER_TOKEN|LIVE_DEMO_BEARER|DEEPSEEK_API_KEY)[ \t]*=[ \t]*(.*)[ \t]*$`)

func hasCredentialLiteralAssignment(text string) bool {
	for _, line := range strings.Split(text, "\n") {
		match := credentialAssignmentPattern.FindStringSubmatch(line)
		if match == nil {
			continue
		}
		value := strings.TrimSpace(match[3])
		if value != "" && !isSymbolicEnvironmentReference(value) {
			return true
		}
	}
	return false
}

func isSymbolicEnvironmentReference(value string) bool {
	if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
		value = value[1 : len(value)-1]
	} else if strings.HasPrefix(value, `"`) || strings.HasSuffix(value, `"`) {
		return false
	}
	if strings.HasPrefix(value, "${") && strings.HasSuffix(value, "}") {
		value = value[2 : len(value)-1]
	} else if strings.HasPrefix(value, "$") {
		value = value[1:]
	} else {
		return false
	}
	if value == "" || value[0] < 'A' || value[0] > 'Z' {
		return false
	}
	for _, character := range value[1:] {
		if (character < 'A' || character > 'Z') && (character < '0' || character > '9') && character != '_' {
			return false
		}
	}
	return true
}
