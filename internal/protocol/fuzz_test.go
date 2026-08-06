package protocol

import "testing"

func FuzzDecodeClient(f *testing.F) {
	seeds := []string{
		`{"type":"session.update","session":{"type":"realtime","audio":{"input":{"format":{"type":"audio/pcm","rate":24000}}}}}`,
		`{"type":"input_audio_buffer.append","audio":"AAE="}`,
		`{"type":"input_audio_buffer.commit"}`,
		`{"type":"conversation.item.create","item":{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}}`,
		`{"type":"response.create","response":{"instructions":"hi"}}`,
		`{"type":"response.cancel","response_id":"resp-1"}`,
		``,
		`{`,
		`{"type":`,
		`{"type":"input_audio_buffer.append","audio":"%%%"}`,
		`{"type":"response.create","sequence_id":1,"sequence_id":2}`,
		`{"a":[[[[[[[[[[[[[[[[[[[[[[[[[1]]]]]]]]]]]]]]]]]]]]]]]]]}`,
	}
	for _, seed := range seeds {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		_, _ = DecodeClient(raw, Limits{MaxFrameBytes: 4_096, MaxJSONDepth: 12, MaxAudioBytes: 1_920})
	})
}
