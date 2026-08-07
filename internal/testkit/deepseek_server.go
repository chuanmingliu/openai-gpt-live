package testkit

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

type DeepSeekResponse struct {
	Status  int
	Headers http.Header
	Chunks  [][]byte
	Stall   bool
}

type CapturedDeepSeekRequest struct {
	Header http.Header
	Body   []byte
}

type DeepSeekServer struct {
	*httptest.Server
	Requests chan CapturedDeepSeekRequest
	Started  chan struct{}
}

func NewDeepSeekServer(t testing.TB, response DeepSeekResponse) *DeepSeekServer {
	t.Helper()
	requests := make(chan CapturedDeepSeekRequest, 16)
	started := make(chan struct{}, 16)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read DeepSeek request: %v", err)
			return
		}
		requests <- CapturedDeepSeekRequest{Header: request.Header.Clone(), Body: append([]byte(nil), body...)}
		started <- struct{}{}
		for key, values := range response.Headers {
			for _, value := range values {
				writer.Header().Add(key, value)
			}
		}
		status := response.Status
		if status == 0 {
			status = http.StatusOK
		}
		writer.WriteHeader(status)
		if flusher, ok := writer.(http.Flusher); ok {
			flusher.Flush()
		}
		for _, chunk := range response.Chunks {
			if _, err := writer.Write(chunk); err != nil {
				return
			}
			if flusher, ok := writer.(http.Flusher); ok {
				flusher.Flush()
			}
		}
		if response.Stall {
			<-request.Context().Done()
		}
	}))
	t.Cleanup(server.Close)
	return &DeepSeekServer{Server: server, Requests: requests, Started: started}
}

func SplitBytes(raw string, width int) [][]byte {
	if width <= 0 {
		panic("testkit: split width must be positive")
	}
	data := []byte(raw)
	chunks := make([][]byte, 0, (len(data)+width-1)/width)
	for len(data) > 0 {
		take := min(width, len(data))
		chunks = append(chunks, append([]byte(nil), data[:take]...))
		data = data[take:]
	}
	return chunks
}
