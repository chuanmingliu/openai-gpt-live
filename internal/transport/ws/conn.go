package ws

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/protocol"
	"github.com/coder/websocket"
)

var errPeerClosed = errors.New("websocket peer closed")

type clientConn struct {
	conn         *websocket.Conn
	limits       protocol.Limits
	readTimeout  time.Duration
	writeTimeout time.Duration

	writeMu   sync.Mutex
	writeGate chan struct{}
	closeMu   sync.Mutex
	closed    bool
	closeErr  error
}

func newClientConn(conn *websocket.Conn, limits protocol.Limits, readTimeout, writeTimeout time.Duration) *clientConn {
	conn.SetReadLimit(int64(limits.MaxFrameBytes))
	client := &clientConn{
		conn: conn, limits: limits, readTimeout: readTimeout, writeTimeout: writeTimeout,
		writeGate: make(chan struct{}, 1),
	}
	client.writeGate <- struct{}{}
	return client
}

func (c *clientConn) Read(ctx context.Context) (protocol.ClientEvent, error) {
	if ctx == nil {
		return nil, errors.New("read context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	readCtx, cancel := operationContext(ctx, c.readTimeout)
	defer cancel()
	typ, raw, err := c.conn.Read(readCtx)
	if err != nil {
		if code := websocket.CloseStatus(err); code != -1 {
			if code == websocket.StatusMessageTooBig {
				c.rememberClosed(nil)
			}
			if code == websocket.StatusNormalClosure || code == websocket.StatusGoingAway {
				return nil, fmt.Errorf("%w: %v", errPeerClosed, err)
			}
		}
		return nil, err
	}
	if typ != websocket.MessageText {
		_ = c.closeNow(websocket.StatusUnsupportedData, "text messages required")
		return nil, errors.New("binary websocket messages are unsupported")
	}
	raw = append([]byte(nil), raw...)
	if !utf8.Valid(raw) {
		_ = c.closeNow(websocket.StatusInvalidFramePayloadData, "invalid text payload")
		return nil, errors.New("invalid UTF-8 websocket payload")
	}
	event, err := protocol.DecodeClient(raw, c.limits)
	if err != nil {
		code := websocket.StatusInvalidFramePayloadData
		reason := "invalid client event"
		if strings.Contains(err.Error(), "unsupported client event type") {
			code = websocket.StatusPolicyViolation
			reason = "unsupported client event"
		}
		_ = c.closeNow(code, reason)
		return nil, fmt.Errorf("decode client event: %w", err)
	}
	return event, nil
}

func (c *clientConn) Write(ctx context.Context, event protocol.ServerEvent) error {
	if ctx == nil {
		return errors.New("write context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	raw, err := protocol.Encode(event)
	if err != nil {
		return err
	}
	raw = append([]byte(nil), raw...)
	writeCtx, cancel := operationContext(ctx, c.writeTimeout)
	defer cancel()
	select {
	case <-writeCtx.Done():
		return writeCtx.Err()
	case <-c.writeGate:
	}
	defer func() { c.writeGate <- struct{}{} }()
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := writeCtx.Err(); err != nil {
		return err
	}
	if c.isClosed() {
		return errors.New("websocket connection is closed")
	}
	return c.conn.Write(writeCtx, websocket.MessageText, raw)
}

// Close defers the manager's routine normal cleanup until the handler knows
// whether Run succeeded. Non-normal closes are terminal immediately.
func (c *clientConn) Close(ctx context.Context, code int, reason string) error {
	if ctx == nil {
		return errors.New("close context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if websocket.StatusCode(code) == websocket.StatusNormalClosure {
		return nil
	}
	return c.closeNowContext(ctx, websocket.StatusCode(code), reason)
}

func (c *clientConn) closeNowContext(ctx context.Context, code websocket.StatusCode, reason string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.writeGate:
	}
	defer func() { c.writeGate <- struct{}{} }()
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.closeMu.Lock()
	if c.closed {
		err := c.closeErr
		c.closeMu.Unlock()
		return err
	}
	c.closed = true
	c.closeMu.Unlock()
	done := make(chan error, 1)
	go func() { done <- c.conn.Close(code, boundedCloseReason(reason)) }()
	select {
	case err := <-done:
		c.closeMu.Lock()
		c.closeErr = err
		c.closeMu.Unlock()
		return err
	case <-ctx.Done():
		_ = c.conn.CloseNow()
		return ctx.Err()
	}
}

func (c *clientConn) finish(runErr error) error {
	if c.isClosed() || errors.Is(runErr, errPeerClosed) {
		return nil
	}
	if errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded) {
		return c.closeNow(websocket.StatusNormalClosure, "session ended")
	}
	if runErr != nil {
		return c.closeNow(websocket.StatusInternalError, "session failed")
	}
	return c.closeNow(websocket.StatusNormalClosure, "session ended")
}

func (c *clientConn) closeNow(code websocket.StatusCode, reason string) error {
	<-c.writeGate
	defer func() { c.writeGate <- struct{}{} }()
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.closeMu.Lock()
	if c.closed {
		err := c.closeErr
		c.closeMu.Unlock()
		return err
	}
	c.closed = true
	c.closeMu.Unlock()
	err := c.conn.Close(code, boundedCloseReason(reason))
	c.closeMu.Lock()
	c.closeErr = err
	c.closeMu.Unlock()
	return err
}

func (c *clientConn) rememberClosed(err error) {
	c.closeMu.Lock()
	if !c.closed {
		c.closed = true
		c.closeErr = err
	}
	c.closeMu.Unlock()
}

func (c *clientConn) isClosed() bool {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	return c.closed
}

func operationContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, timeout)
}

func boundedCloseReason(reason string) string {
	const maxReasonBytes = 123
	if len(reason) <= maxReasonBytes {
		return reason
	}
	reason = reason[:maxReasonBytes]
	for !utf8.ValidString(reason) {
		reason = reason[:len(reason)-1]
	}
	return reason
}
