package deepseek

import (
	"bufio"
	"bytes"
	"errors"
	"io"
)

var (
	errEventTooLarge       = errors.New("deepseek: SSE event exceeds limit")
	errUnsupportedSSEField = errors.New("deepseek: unsupported SSE field contains content")
)

type sseReader struct {
	reader *bufio.Reader
	limit  int
}

func newSSEReader(reader *bufio.Reader, limit int) *sseReader {
	return &sseReader{reader: reader, limit: limit}
}

func (reader *sseReader) Next() ([]byte, error) {
	var data []byte
	hasData := false
	for {
		line, terminated, err := reader.readLine()
		if err != nil {
			return nil, err
		}
		if !terminated {
			return nil, io.ErrUnexpectedEOF
		}
		line = bytes.TrimSuffix(line, []byte{'\r'})
		if len(line) == 0 {
			if !hasData {
				continue
			}
			return data, nil
		}
		if line[0] == ':' {
			continue
		}
		field, value, found := bytes.Cut(line, []byte{':'})
		if !found {
			field, value = line, nil
		}
		if len(value) > 0 && value[0] == ' ' {
			value = value[1:]
		}
		if bytes.Equal(field, []byte("data")) {
			additional := len(value)
			if hasData {
				additional++
			}
			if additional > reader.limit-len(data) {
				return nil, errEventTooLarge
			}
			if hasData {
				data = append(data, '\n')
			}
			data = append(data, value...)
			hasData = true
			continue
		}
		if len(value) != 0 {
			return nil, errUnsupportedSSEField
		}
	}
}

func (reader *sseReader) readLine() ([]byte, bool, error) {
	line := make([]byte, 0, min(reader.limit+16, 4096))
	for {
		fragment, err := reader.reader.ReadSlice('\n')
		terminated := len(fragment) > 0 && fragment[len(fragment)-1] == '\n'
		if terminated {
			fragment = fragment[:len(fragment)-1]
		}
		if len(line)+len(fragment) > reader.limit+16 {
			return nil, false, errEventTooLarge
		}
		line = append(line, fragment...)
		if terminated {
			return line, true, nil
		}
		if err == nil {
			continue
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if errors.Is(err, io.EOF) {
			if len(line) == 0 {
				return nil, false, io.EOF
			}
			return line, false, nil
		}
		return nil, false, err
	}
}
