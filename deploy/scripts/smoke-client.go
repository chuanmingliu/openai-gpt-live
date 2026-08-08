//go:build ignore

// This bounded smoke client establishes one content-free Realtime session. It
// deliberately sends and stores no audio or transcript content.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/coder/websocket"
)

func main() {
	url := os.Getenv("LIVE_SMOKE_GATEWAY_URL")
	token := os.Getenv("LIVE_STATIC_BEARER_TOKEN")
	if url == "" || token == "" {
		panic("LIVE_SMOKE_GATEWAY_URL and LIVE_STATIC_BEARER_TOKEN are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	header := http.Header{"Authorization": []string{"Bearer " + token}, "Origin": []string{"http://127.0.0.1:3000"}}
	conn, response, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: header})
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	if err != nil {
		panic(fmt.Errorf("websocket establishment failed: %w", err))
	}
	defer conn.CloseNow()
	_, payload, err := conn.Read(ctx)
	if err != nil {
		panic(fmt.Errorf("read session event: %w", err))
	}
	var envelope struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		panic(errors.New("session event was not bounded JSON"))
	}
	if envelope.Type != "session.created" {
		panic(fmt.Errorf("unexpected initial event type %q", envelope.Type))
	}
	if err := conn.Close(websocket.StatusNormalClosure, "smoke complete"); err != nil {
		panic(fmt.Errorf("close smoke session: %w", err))
	}
}
