package main

import (
	"context"
	"errors"

	"github.com/coder/websocket"
)

var (
	errBinaryFrame   = errors.New("binary frame")
	errFrameTooLarge = errors.New("frame too large")
)

type relayResult struct {
	err error
}

func proxyWebSocket(ctx context.Context, browser, upstream *websocket.Conn, maxBytes int64) error {
	relayContext, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan relayResult, 2)
	go func() { results <- relayResult{err: copyText(relayContext, upstream, browser, maxBytes)} }()
	go func() { results <- relayResult{err: copyText(relayContext, browser, upstream, maxBytes)} }()

	first := <-results
	cancel()
	code, reason := safeBrowserClose(first.err)
	_ = browser.Close(code, reason)
	_ = upstream.Close(websocket.StatusNormalClosure, "relay closed")
	<-results
	return first.err
}

func copyText(ctx context.Context, destination, source *websocket.Conn, maxBytes int64) error {
	source.SetReadLimit(maxBytes)
	for {
		kind, payload, err := source.Read(ctx)
		if err != nil {
			return err
		}
		if kind != websocket.MessageText {
			return errBinaryFrame
		}
		if int64(len(payload)) > maxBytes {
			return errFrameTooLarge
		}
		if err := destination.Write(ctx, websocket.MessageText, payload); err != nil {
			return err
		}
	}
}

func safeBrowserClose(err error) (websocket.StatusCode, string) {
	if errors.Is(err, errBinaryFrame) {
		return websocket.StatusUnsupportedData, "text frames required"
	}
	if errors.Is(err, errFrameTooLarge) || websocket.CloseStatus(err) == websocket.StatusMessageTooBig {
		return websocket.StatusMessageTooBig, "frame too large"
	}
	code := websocket.CloseStatus(err)
	if code == websocket.StatusNormalClosure || code == websocket.StatusGoingAway {
		return code, "peer closed"
	}
	return websocket.StatusInternalError, "relay unavailable"
}
