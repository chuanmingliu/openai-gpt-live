package main

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/coder/websocket"
	"golang.org/x/sync/errgroup"
)

var errFrameTooLarge = errors.New("demo frame exceeds limit")

func proxyWebSocket(ctx context.Context, browser, upstream *websocket.Conn, maxBytes int64, idle time.Duration) error {
	if browser == nil || upstream == nil || maxBytes <= 0 {
		return errors.New("demo proxy dependencies are required")
	}
	sessionCtx, bump, stop := withIdle(ctx, idle)
	defer stop()
	group, groupCtx := errgroup.WithContext(sessionCtx)
	group.Go(func() error { return copyText(groupCtx, upstream, browser, maxBytes, bump) })
	group.Go(func() error { return copyText(groupCtx, browser, upstream, maxBytes, bump) })
	err := group.Wait()
	closePair(browser, upstream, err)
	return err
}

func withIdle(parent context.Context, idle time.Duration) (context.Context, func(), func()) {
	if parent == nil {
		parent = context.Background()
	}
	if idle <= 0 {
		return parent, func() {}, func() {}
	}
	ctx, cancel := context.WithCancel(parent)
	timer := time.AfterFunc(idle, cancel)
	var mu sync.Mutex
	bump := func() {
		mu.Lock()
		defer mu.Unlock()
		if timer != nil {
			timer.Reset(idle)
		}
	}
	stop := func() {
		mu.Lock()
		defer mu.Unlock()
		if timer != nil {
			timer.Stop()
			timer = nil
		}
		cancel()
	}
	return ctx, bump, stop
}

func copyText(ctx context.Context, destination, source *websocket.Conn, max int64, bump func()) error {
	source.SetReadLimit(max)
	for {
		kind, payload, err := source.Read(ctx)
		if err != nil {
			return err
		}
		if bump != nil {
			bump()
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
	case err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded):
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
