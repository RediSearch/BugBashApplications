// Package metrics collects throughput counters, a query-latency histogram, and a
// footprint time-series, and derives the "does on-disk footprint plateau?"
// verdict via a linear fit over the steady-state window.
package metrics

import (
	"sync"
	"sync/atomic"
	"time"
)

// Counters are the workload throughput counters (all atomic).
type Counters struct {
	Ingested     atomic.Int64
	IngestErrors atomic.Int64
	Edited       atomic.Int64
	Deleted      atomic.Int64
	Refreshed    atomic.Int64
	Queries      atomic.Int64
	QueryErrors  atomic.Int64
}

// latBoundsMs are the upper bounds (ms) of the latency histogram buckets.
var latBoundsMs = []float64{
	0.25, 0.5, 0.75, 1, 1.5, 2, 3, 4, 6, 8, 12, 16, 24, 32, 48, 64, 96,
	128, 192, 256, 384, 512, 768, 1024, 1536, 2048, 4096, 8192,
}

// Hist is a simple thread-safe bucketed latency histogram.
type Hist struct {
	buckets []atomic.Int64 // len = len(latBoundsMs)+1 (last = overflow)
}

func newHist() *Hist { return &Hist{buckets: make([]atomic.Int64, len(latBoundsMs)+1)} }

// Record adds one observation.
func (h *Hist) Record(d time.Duration) {
	ms := float64(d) / float64(time.Millisecond)
	for i, b := range latBoundsMs {
		if ms <= b {
			h.buckets[i].Add(1)
			return
		}
	}
	h.buckets[len(h.buckets)-1].Add(1)
}

// Percentile returns an upper-bound estimate (ms) for percentile p in [0,1].
func (h *Hist) Percentile(p float64) float64 {
	var total int64
	counts := make([]int64, len(h.buckets))
	for i := range h.buckets {
		counts[i] = h.buckets[i].Load()
		total += counts[i]
	}
	if total == 0 {
		return 0
	}
	target := int64(p * float64(total))
	var cum int64
	for i, c := range counts {
		cum += c
		if cum >= target {
			if i < len(latBoundsMs) {
				return latBoundsMs[i]
			}
			return latBoundsMs[len(latBoundsMs)-1] * 2 // overflow bucket
		}
	}
	return latBoundsMs[len(latBoundsMs)-1] * 2
}

// Sample is one footprint observation over time.
type Sample struct {
	TSec              float64 `json:"t"`
	DiskUsage         int64   `json:"disk_usage"`
	UsedMem           int64   `json:"used_mem"`
	NumDocs           int64   `json:"num_docs"`
	NumRecords        int64   `json:"num_records"`
	InvertedMB        float64 `json:"inverted_mb"`
	AsyncExpired      int64   `json:"async_expired"`
	CompactionCycles  int64   `json:"compaction_cycles"`
	PendingCompaction int64   `json:"pending_compaction"`
}

// Metrics aggregates everything.
type Metrics struct {
	C     Counters
	Lat   *Hist
	start time.Time

	mu       sync.RWMutex
	series   []Sample
	diskMode bool
}

// New returns an initialized Metrics.
func New() *Metrics {
	return &Metrics{Lat: newHist(), start: time.Now()}
}

// Elapsed since start.
func (m *Metrics) Elapsed() time.Duration { return time.Since(m.start) }

// AddSample appends a footprint observation.
func (m *Metrics) AddSample(s Sample, diskMode bool) {
	m.mu.Lock()
	m.series = append(m.series, s)
	if diskMode {
		m.diskMode = true
	}
	m.mu.Unlock()
}

// Series returns a copy of the footprint series.
func (m *Metrics) Series() []Sample {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Sample, len(m.series))
	copy(out, m.series)
	return out
}

// Verdict summarizes the footprint trend.
type Verdict struct {
	Metric        string  `json:"metric"` // "disk_usage" or "used_mem"
	SlopeBytesSec float64 `json:"slope_bytes_sec"`
	MeanBytes     float64 `json:"mean_bytes"`
	Samples       int     `json:"samples"`
	Plateau       bool    `json:"plateau"`
	Note          string  `json:"note"`
}

// Plateau computes a linear fit over the steady-state window (dropping the first
// 20% as warmup) and decides plateau vs growth.
func (m *Metrics) Plateau() Verdict {
	series := m.Series()
	useDisk := m.diskMode
	metric := "used_mem"
	if useDisk {
		metric = "disk_usage"
	}
	v := Verdict{Metric: metric, Samples: len(series)}
	if len(series) < 6 {
		v.Note = "not enough samples for a trend"
		return v
	}
	start := len(series) / 5 // skip first 20%
	window := series[start:]
	// least-squares slope of value vs time
	var n, sx, sy, sxx, sxy float64
	for _, s := range window {
		y := float64(s.UsedMem)
		if useDisk {
			y = float64(s.DiskUsage)
		}
		x := s.TSec
		n++
		sx += x
		sy += y
		sxx += x * x
		sxy += x * y
	}
	denom := n*sxx - sx*sx
	if denom == 0 {
		v.Note = "degenerate time axis"
		return v
	}
	slope := (n*sxy - sx*sy) / denom
	mean := sy / n
	v.SlopeBytesSec = slope
	v.MeanBytes = mean
	// Plateau if projected growth over the window is small vs mean footprint.
	spanSec := window[len(window)-1].TSec - window[0].TSec
	projected := slope * spanSec
	if mean > 0 && projected < 0.10*mean {
		v.Plateau = true
		v.Note = "footprint is flat/declining over the steady-state window"
	} else {
		v.Note = "footprint still trending up — watch for unbounded growth"
	}
	return v
}
