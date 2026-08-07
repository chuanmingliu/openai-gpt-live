package session

import (
	"sync/atomic"

	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
)

type EpochFence struct {
	value atomic.Uint64
}

func NewEpochFence(initial identity.ResponseEpoch) *EpochFence {
	fence := &EpochFence{}
	fence.value.Store(uint64(initial))
	return fence
}

func (f *EpochFence) Current() identity.ResponseEpoch {
	if f == nil {
		return 0
	}
	return identity.ResponseEpoch(f.value.Load())
}

func (f *EpochFence) Advance() identity.ResponseEpoch {
	return identity.ResponseEpoch(f.value.Add(1))
}

func (f *EpochFence) Accept(epoch identity.ResponseEpoch) bool {
	return f != nil && f.Current() == epoch
}
