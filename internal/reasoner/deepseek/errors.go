package deepseek

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/reasoner"
)

func providerError(code, message string, status int, retryable bool, retryAfter time.Duration) *reasoner.ProviderError {
	return &reasoner.ProviderError{
		Code:       safeProviderText(code, 128),
		Message:    safeProviderText(message, 512),
		HTTPStatus: status,
		Retryable:  retryable,
		RetryAfter: retryAfter,
	}
}

func responseError(response *http.Response, body []byte, oversized, retryable bool, retryAfter time.Duration) *reasoner.ProviderError {
	code, message := "http_error", ""
	if oversized {
		message = "provider error body exceeded limit"
	} else {
		var envelope struct {
			Error struct {
				Code    json.RawMessage `json:"code"`
				Message string          `json:"message"`
			} `json:"error"`
		}
		if json.Unmarshal(body, &envelope) == nil {
			message = envelope.Error.Message
			var stringCode string
			if json.Unmarshal(envelope.Error.Code, &stringCode) == nil {
				code = stringCode
			} else {
				var numberCode json.Number
				if json.Unmarshal(envelope.Error.Code, &numberCode) == nil {
					code = numberCode.String()
				}
			}
		}
	}
	return providerError(code, message, response.StatusCode, retryable, retryAfter)
}

func retryableHTTPStatus(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500 && status <= 599
}

func parseRetryAfter(value, dateValue string, now func() time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.ParseUint(value, 10, 63); err == nil {
		const maxSeconds = uint64((1<<63 - 1) / int64(time.Second))
		if seconds <= maxSeconds {
			return time.Duration(seconds) * time.Second
		}
		return 0
	}
	deadline, err := http.ParseTime(value)
	if err != nil {
		return 0
	}
	reference := now().UTC()
	if parsedDate, dateErr := http.ParseTime(strings.TrimSpace(dateValue)); dateErr == nil {
		reference = parsedDate
	}
	delta := deadline.Sub(reference)
	if delta < 0 {
		return 0
	}
	return delta
}

func safeProviderText(value string, limit int) string {
	value = strings.Map(func(character rune) rune {
		if character < 0x20 || character == 0x7f {
			return -1
		}
		return character
	}, value)
	if !utf8.ValidString(value) {
		return ""
	}
	if len(value) > limit {
		value = value[:limit]
		for !utf8.ValidString(value) {
			value = value[:len(value)-1]
		}
	}
	return value
}
