package main

import (
	"context"
	"errors"

	"github.com/coder/websocket"
	"golang.org/x/sync/errgroup"
)

var errFrameTooLarge = errors.New("demo frame exceeds limit")

func proxyWebSocket(ctx context.Context, browser, upstream *websocket.Conn, maxBytes int64) error {
	if browser == nil || upstream == nil || maxBytes <= 0 {
		return errors.New("demo proxy dependencies are required")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error { return copyText(groupCtx, upstream, browser, maxBytes) })
	group.Go(func() error { return copyText(groupCtx, browser, upstream, maxBytes) })
	err := group.Wait()
	closePair(browser, upstream, err)
	return err
}

func copyText(ctx context.Context, destination, source *websocket.Conn, max int64) error {
	source.SetReadLimit(max)
	for {
		kind, payload, err := source.Read(ctx)
		if err != nil {
			return err
		}
		if kind != websocket.MessageText {
			return websocket.CloseError{Code: websocket.StatusUnsupportedData, Reason: "text frames required"}
		}
		if int64(len(payload)) > max {
			return errFrameTooLarge
		}
		if err := destination.Write(ctx, websocket.MessageText, payload); err != nil {
			return err
		}
	}
}

func closePair(browser, upstream *websocket.Conn, err error) {
	code, reason := websocket.StatusNormalClosure, "session ended"
	var closeErr websocket.CloseError
	switch {
	case errors.As(err, &closeErr):
		code, reason = closeErr.Code, boundedCloseReason(closeErr.Reason)
	case errors.Is(err, errFrameTooLarge):
		code, reason = websocket.StatusMessageTooBig, "frame too large"
	case err != nil && !errors.Is(err, context.Canceled):
		code, reason = websocket.StatusInternalError, "session failed"
	}
	_ = browser.Close(code, reason)
	_ = upstream.Close(code, reason)
}

func boundedCloseReason(reason string) string {
	if len(reason) > 64 {
		return "session ended"
	}
	return reason
}
