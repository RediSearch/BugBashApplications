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
	SlowQueries  atomic.Int64 // latency >= configured slow threshold (timeout-risk)
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

// Snapshot returns the current per-bucket counts (for windowed percentiles).
func (h *Hist) Snapshot() []int64 {
	out := make([]int64, len(h.buckets))
	for i := range h.buckets {
		out[i] = h.buckets[i].Load()
	}
	return out
}

// PercentileOf returns an upper-bound estimate (ms) for percentile p over the
// given bucket counts (e.g. a windowed delta).
func PercentileOf(counts []int64, p float64) float64 {
	var total int64
	for _, c := range counts {
		total += c
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

// Percentile returns an all-time upper-bound estimate (ms) for percentile p.
func (h *Hist) Percentile(p float64) float64 { return PercentileOf(h.Snapshot(), p) }

// Sample is one observation over time. Rates and latency percentiles are
// computed server-side by the sampler (windowed) so the charts are smooth and
// consistent across page reloads.
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
	IngestRate        float64 `json:"ingest_rate"`
	ExpireRate        float64 `json:"expire_rate"`
	DeleteRate        float64 `json:"delete_rate"`
	QueryRate         float64 `json:"query_rate"`
	P50               float64 `json:"p50"` // windowed (recent) latency, ms
	P99               float64 `json:"p99"`
}

// Metrics aggregates everything.
type Metrics struct {
	C     Counters
	Lat   *Hist
	start time.Time

	mu       sync.RWMutex
	series   []Sample
	diskMode atomic.Bool

	profMu     sync.Mutex
	profCounts map[string]int64

	statusMu sync.RWMutex
	status   Status

	rqMu   sync.Mutex
	recent []QSample // ring buffer of recently-executed queries (sampled)
	rqCap  int
}

// QSample is one recorded (sampled) executed query, for the live query feed.
type QSample struct {
	TSec    float64 `json:"t"`
	Profile string  `json:"profile"`
	Query   string  `json:"query"`
	Ms      float64 `json:"ms"`
	Total   int64   `json:"total"`
	Err     string  `json:"err,omitempty"`
}

// PushQuery records an executed query into the recent-queries ring.
func (m *Metrics) PushQuery(q QSample) {
	m.rqMu.Lock()
	if len(m.recent) < m.rqCap {
		m.recent = append(m.recent, q)
	} else {
		copy(m.recent, m.recent[1:])
		m.recent[len(m.recent)-1] = q
	}
	m.rqMu.Unlock()
}

// RecentQueries returns the recorded queries, newest first.
func (m *Metrics) RecentQueries() []QSample {
	m.rqMu.Lock()
	defer m.rqMu.Unlock()
	out := make([]QSample, len(m.recent))
	for i, q := range m.recent {
		out[len(m.recent)-1-i] = q
	}
	return out
}

// Status is the latest overall index status (FT.INFO + INFO), refreshed by the
// sampler. Fields that are placeholders on Flex are deliberately excluded.
type Status struct {
	// FT.INFO (index-level)
	NumDocs              int64   `json:"num_docs"`
	NumRecords           int64   `json:"num_records"`
	MaxDocID             int64   `json:"max_doc_id"`
	InvertedMB           float64 `json:"inverted_sz_mb"`
	DocTableMB           float64 `json:"doc_table_size_mb"`
	TotalIndexMemMB      float64 `json:"total_index_memory_sz_mb"`
	HashIndexingFailures int64   `json:"hash_indexing_failures"`
	Indexing             int64   `json:"indexing"`
	PercentIndexed       float64 `json:"percent_indexed"`
	Cleaning             int64   `json:"cleaning"`
	// INFO (process / disk)
	DiskMode          bool  `json:"disk_mode"`
	DiskUsage         int64 `json:"disk_usage"`
	UsedMem           int64 `json:"used_mem"`
	AsyncReadsExpired int64 `json:"async_reads_expired"`
	CompactionCycles  int64 `json:"compaction_cycles"`
	PendingCompaction int64 `json:"pending_compaction"`
}

// SetStatus stores the latest index status.
func (m *Metrics) SetStatus(s Status) {
	m.statusMu.Lock()
	m.status = s
	m.statusMu.Unlock()
}

// Status returns the latest index status.
func (m *Metrics) Status() Status {
	m.statusMu.RLock()
	defer m.statusMu.RUnlock()
	return m.status
}

// New returns an initialized Metrics.
func New() *Metrics {
	return &Metrics{Lat: newHist(), start: time.Now(), profCounts: map[string]int64{}, rqCap: 60}
}

// Elapsed since start.
func (m *Metrics) Elapsed() time.Duration { return time.Since(m.start) }

// RecordQuery records one completed query: its latency, the query counter, a
// slow-query tick if it crossed the timeout-risk threshold, and its profile tally.
func (m *Metrics) RecordQuery(profile string, d time.Duration, slowMs int) {
	m.Lat.Record(d)
	m.C.Queries.Add(1)
	if float64(d)/float64(time.Millisecond) >= float64(slowMs) {
		m.C.SlowQueries.Add(1)
	}
	m.profMu.Lock()
	m.profCounts[profile]++
	m.profMu.Unlock()
}

// ProfileCounts returns a copy of the per-profile query tallies.
func (m *Metrics) ProfileCounts() map[string]int64 {
	m.profMu.Lock()
	defer m.profMu.Unlock()
	out := make(map[string]int64, len(m.profCounts))
	for k, v := range m.profCounts {
		out[k] = v
	}
	return out
}

// AddSample appends a footprint observation.
func (m *Metrics) AddSample(s Sample, diskMode bool) {
	m.mu.Lock()
	m.series = append(m.series, s)
	m.mu.Unlock()
	if diskMode {
		m.diskMode.Store(true)
	}
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
	useDisk := m.diskMode.Load()
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
