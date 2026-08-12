package loadgateway

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadIdentityOutlivesMaximumFormalSoak(t *testing.T) {
	const maximumFormalSoak = 2 * time.Hour
	if loadIdentityLifetime <= maximumFormalSoak {
		t.Fatalf("load identity lifetime %s does not outlive formal soak %s", loadIdentityLifetime, maximumFormalSoak)
	}
}

func TestWatermarkPCMFramesAreFullSizedAndOrdered(t *testing.T) {
	for sequence := uint32(1); sequence <= 50; sequence++ {
		frame := WatermarkPCM(sequence)
		require.Len(t, frame, 960)
		assert.Equal(t, sequence, binary.LittleEndian.Uint32(frame[:4]))
		decoded, ok := WatermarkSequence(frame)
		assert.True(t, ok)
		assert.Equal(t, sequence, decoded)
	}
	_, ok := WatermarkSequence(make([]byte, 959))
	assert.False(t, ok)
}
