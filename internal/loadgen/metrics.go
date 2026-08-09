package loadgen

import (
	"encoding/json"
	"errors"
	"math"
	"math/bits"
	"sort"
	"time"
)

// Duration has an unambiguous, human-readable JSON representation.
type Duration time.Duration

func (d Duration) Duration() time.Duration { return time.Duration(d) }

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(raw []byte) error {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return err
	}
	*d = Duration(parsed)
	return nil
}

// Histogram is a bounded, logarithmic high-dynamic-range latency recorder.
// Values are quantized to a fixed number of binary sub-buckets and samples are
// never retained.
type Histogram struct {
	maximum    time.Duration
	precision  uint
	counts     []uint64
	total      uint64
	maximumHit time.Duration
}

func NewHistogram(maximum time.Duration, significantBits uint) *Histogram {
	if maximum <= 0 {
		maximum = time.Hour
	}
	// The public precision is decimal significant digits. Six binary bits are
	// sufficient for three decimal digits at the latency gates while retaining
	// a strict 4096-bucket upper bound.
	if significantBits < 1 {
		significantBits = 1
	}
	if significantBits > 3 {
		significantBits = 3
	}
	significantBits += 3
	// 64 magnitudes and at most 64 sub-buckets is a fixed upper bound.
	return &Histogram{maximum: maximum, precision: significantBits, counts: make([]uint64, 64*(1<<significantBits))}
}

func (h *Histogram) BucketCount() int {
	if h == nil {
		return 0
	}
	return len(h.counts)
}

func (h *Histogram) Record(value time.Duration) {
	if h == nil {
		return
	}
	if value < 0 {
		value = 0
	}
	if value > h.maximum {
		value = h.maximum
	}
	if value > h.maximumHit {
		h.maximumHit = value
	}
	h.counts[h.index(value)]++
	h.total++
}

func (h *Histogram) Count() uint64 {
	if h == nil {
		return 0
	}
	return h.total
}

func (h *Histogram) Max() time.Duration {
	if h == nil {
		return 0
	}
	return h.maximumHit
}

func (h *Histogram) Percentile(percent float64) time.Duration {
	if h == nil || h.total == 0 {
		return 0
	}
	if percent < 0 {
		percent = 0
	}
	if percent > 100 {
		percent = 100
	}
	rank := uint64(math.Ceil(float64(h.total) * percent / 100))
	if rank == 0 {
		rank = 1
	}
	var cumulative uint64
	for index, count := range h.counts {
		cumulative += count
		if cumulative >= rank {
			return h.upperBound(index)
		}
	}
	return h.maximumHit
}

func (h *Histogram) index(value time.Duration) int {
	micros := uint64(value / time.Microsecond)
	if micros == 0 {
		return 0
	}
	width := uint(bits.Len64(micros))
	shift := uint(0)
	if width > h.precision {
		shift = width - h.precision
	}
	sub := micros >> shift
	mask := uint64((1 << h.precision) - 1)
	sub &= mask
	index := int(shift)*(1<<h.precision) + int(sub)
	if index >= len(h.counts) {
		return len(h.counts) - 1
	}
	return index
}

func (h *Histogram) upperBound(index int) time.Duration {
	if index == 0 {
		return time.Microsecond
	}
	width := 1 << h.precision
	shift := uint(index / width)
	sub := uint64(index%width) + 1
	micros := sub << shift
	value := time.Duration(micros) * time.Microsecond
	if value > h.maximum {
		return h.maximum
	}
	return value
}

type HeapSample struct {
	At    time.Time `json:"at"`
	Bytes uint64    `json:"bytes"`
}

// HeapSlopePerHour uses the Theil-Sen median pairwise slope. Five samples are
// required so a single allocator/GC outlier cannot define the result.
func HeapSlopePerHour(samples []HeapSample) (float64, error) {
	if len(samples) < 5 {
		return 0, errors.New("at least five heap samples are required")
	}
	ordered := append([]HeapSample(nil), samples...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].At.Before(ordered[j].At) })
	slopes := make([]float64, 0, len(ordered)*(len(ordered)-1)/2)
	for i := 0; i < len(ordered); i++ {
		for j := i + 1; j < len(ordered); j++ {
			hours := ordered[j].At.Sub(ordered[i].At).Hours()
			if hours <= 0 {
				continue
			}
			slopes = append(slopes, (float64(ordered[j].Bytes)-float64(ordered[i].Bytes))/hours)
		}
	}
	if len(slopes) == 0 {
		return 0, errors.New("heap samples require distinct increasing timestamps")
	}
	sort.Float64s(slopes)
	middle := len(slopes) / 2
	if len(slopes)%2 == 0 {
		return (slopes[middle-1] + slopes[middle]) / 2, nil
	}
	return slopes[middle], nil
}
