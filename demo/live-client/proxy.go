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

type frameReader func(context.Context) (websocket.MessageType, []byte, error)

// bufferedBrowserReader retains at most two bounded frames (one queued and one
// being read) while dialing. The same reader feeds the relay after connection.
func bufferedBrowserReader(ctx context.Context, browser *websocket.Conn, maxBytes int64, cancelDial context.CancelFunc) frameReader {
	type frame struct {
		kind    websocket.MessageType
		payload []byte
		err     error
	}
	frames := make(chan frame, 1)
	browser.SetReadLimit(maxBytes)
	go func() {
		defer close(frames)
		for {
			kind, payload, err := browser.Read(ctx)
			if err != nil {
				cancelDial()
			}
			select {
			case frames <- frame{kind, payload, err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return func(readCtx context.Context) (websocket.MessageType, []byte, error) {
		select {
		case frame, ok := <-frames:
			if !ok {
				return 0, nil, context.Canceled
			}
			return frame.kind, frame.payload, frame.err
		case <-readCtx.Done():
			return 0, nil, readCtx.Err()
		}
	}
}

func proxyWebSocket(ctx context.Context, browser, upstream *websocket.Conn, maxBytes int64, readBrowser frameReader) error {
	group, groupContext := errgroup.WithContext(ctx)
	copyContext, cancel := context.WithCancel(groupContext)
	defer cancel()
	errors := make(chan error, 2)
	group.Go(func() error {
		errors <- copyTextFrom(copyContext, upstream, readBrowser, maxBytes)
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
	return copyTextFrom(ctx, destination, source.Read, max)
}

func copyTextFrom(ctx context.Context, destination *websocket.Conn, read frameReader, max int64) error {
	for {
		kind, payload, err := read(ctx)
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
