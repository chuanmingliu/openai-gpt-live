package main

import (
	"context"
	"errors"

	"github.com/coder/websocket"
	"golang.org/x/sync/errgroup"
)

var (
	errTextFramesRequired = errors.New("text frames required")
	errFrameTooLarge      = errors.New("frame too large")
)

func proxyWebSocket(ctx context.Context, browser, upstream *websocket.Conn, maxBytes int64) error {
	group, groupContext := errgroup.WithContext(ctx)
	copyContext, cancel := context.WithCancel(groupContext)
	defer cancel()
	errors := make(chan error, 2)
	group.Go(func() error {
		errors <- copyText(copyContext, upstream, browser, maxBytes)
		return nil
	})
	group.Go(func() error {
		errors <- copyText(copyContext, browser, upstream, maxBytes)
		return nil
	})
	err := <-errors
	closeBrowser(browser, err)
	cancel()
	_ = group.Wait()
	return err
}

func copyText(ctx context.Context, destination, source *websocket.Conn, max int64) error {
	source.SetReadLimit(max)
	for {
		kind, payload, err := source.Read(ctx)
		if err != nil {
			if errors.Is(err, websocket.ErrMessageTooBig) || websocket.CloseStatus(err) == websocket.StatusMessageTooBig {
				return errFrameTooLarge
			}
			return err
		}
		if kind != websocket.MessageText {
			return errTextFramesRequired
		}
		if int64(len(payload)) > max {
			return errFrameTooLarge
		}
		if err := destination.Write(ctx, websocket.MessageText, payload); err != nil {
			return err
		}
	}
}

func closeBrowser(browser *websocket.Conn, err error) {
	status, reason := publicClose(err)
	_ = browser.Close(status, reason)
}

func publicClose(err error) (websocket.StatusCode, string) {
	if errors.Is(err, errTextFramesRequired) {
		return websocket.StatusUnsupportedData, "text frames required"
	}
	if errors.Is(err, errFrameTooLarge) {
		return websocket.StatusMessageTooBig, "frame too large"
	}
	if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
		return websocket.StatusNormalClosure, "normal closure"
	}
	return websocket.StatusTryAgainLater, "upstream unavailable"
}
