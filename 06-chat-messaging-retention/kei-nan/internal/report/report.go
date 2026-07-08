// Package report renders the live console status line and the end-of-run summary,
// and writes machine-readable artifacts (metrics.csv, summary.json).
package report

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"chatstress/internal/config"
	"chatstress/internal/metrics"
	"chatstress/internal/oracle"
)

// Reporter tracks deltas between console lines.
type Reporter struct {
	met        *metrics.Metrics
	orc        *oracle.Oracle
	prevIngest int64
	prevQuery  int64
	prevT      time.Time
}

// New builds a Reporter.
func New(met *metrics.Metrics, orc *oracle.Oracle) *Reporter {
	return &Reporter{met: met, orc: orc, prevT: time.Now()}
}

// Line returns a one-line status string, computing rates since the previous call.
func (r *Reporter) Line() string {
	now := time.Now()
	dt := now.Sub(r.prevT).Seconds()
	if dt <= 0 {
		dt = 1
	}
	ing := r.met.C.Ingested.Load()
	q := r.met.C.Queries.Load()
	ingRate := float64(ing-r.prevIngest) / dt
	qRate := float64(q-r.prevQuery) / dt
	r.prevIngest, r.prevQuery, r.prevT = ing, q, now

	var docs, records int64
	var footprint int64
	footLabel := "mem"
	series := r.met.Series()
	if n := len(series); n > 0 {
		last := series[n-1]
		docs, records = last.NumDocs, last.NumRecords
		if last.DiskUsage > 0 {
			footprint, footLabel = last.DiskUsage, "disk"
		} else {
			footprint = last.UsedMem
		}
	}
	ost := r.orc.Stats()
	return fmt.Sprintf(
		"t=%4.0fs | ingest %6.0f/s query %5.0f/s | docs %d recs %d | %s %s | p50 %.1fms p99 %.1fms | stale %d | tracked %d",
		r.met.Elapsed().Seconds(), ingRate, qRate, docs, records,
		footLabel, humanBytes(footprint),
		r.met.Lat.Percentile(0.50), r.met.Lat.Percentile(0.99),
		r.orc.StaleHits(), ost.Tracked,
	)
}

// Summary is the end-of-run report (also written as summary.json).
type Summary struct {
	DurationSec  float64          `json:"duration_sec"`
	Ingested     int64            `json:"ingested"`
	IngestErrors int64            `json:"ingest_errors"`
	Edited       int64            `json:"edited"`
	Deleted      int64            `json:"deleted"`
	Refreshed    int64            `json:"refreshed"`
	Queries      int64            `json:"queries"`
	QueryErrors  int64            `json:"query_errors"`
	SlowQueries  int64            `json:"slow_queries"`
	P50ms        float64          `json:"p50_ms"`
	P99ms        float64          `json:"p99_ms"`
	Oracle       oracle.Stats     `json:"oracle"`
	StaleHits    int64            `json:"stale_hits"`
	Footprint    metrics.Verdict  `json:"footprint"`
	Profiles     map[string]int64 `json:"profiles"`
	Samples      []metrics.Sample `json:"-"`
}

// Finalize computes the summary and writes artifacts into outDir.
func (r *Reporter) Finalize(cfg *config.Config) (Summary, error) {
	s := Summary{
		DurationSec:  r.met.Elapsed().Seconds(),
		Ingested:     r.met.C.Ingested.Load(),
		IngestErrors: r.met.C.IngestErrors.Load(),
		Edited:       r.met.C.Edited.Load(),
		Deleted:      r.met.C.Deleted.Load(),
		Refreshed:    r.met.C.Refreshed.Load(),
		Queries:      r.met.C.Queries.Load(),
		QueryErrors:  r.met.C.QueryErrors.Load(),
		SlowQueries:  r.met.C.SlowQueries.Load(),
		P50ms:        r.met.Lat.Percentile(0.50),
		P99ms:        r.met.Lat.Percentile(0.99),
		Oracle:       r.orc.Stats(),
		StaleHits:    r.orc.StaleHits(),
		Footprint:    r.met.Plateau(),
		Profiles:     r.met.ProfileCounts(),
		Samples:      r.met.Series(),
	}
	if err := os.MkdirAll(cfg.OutDir, 0o755); err != nil {
		return s, err
	}
	if err := writeJSON(filepath.Join(cfg.OutDir, "summary.json"), s); err != nil {
		return s, err
	}
	if err := writeCSV(filepath.Join(cfg.OutDir, "metrics.csv"), s.Samples); err != nil {
		return s, err
	}
	return s, nil
}

// Text renders a human-readable summary block.
func (s Summary) Text() string {
	var b strings.Builder
	verdict := "PASS"
	if s.StaleHits > 0 {
		verdict = "FAIL"
	}
	fmt.Fprintf(&b, "\n===== chatstress summary =====\n")
	fmt.Fprintf(&b, "duration:        %.0fs\n", s.DurationSec)
	fmt.Fprintf(&b, "ingested:        %d (errors %d)\n", s.Ingested, s.IngestErrors)
	fmt.Fprintf(&b, "mutations:       edited %d, deleted %d, refreshed %d\n", s.Edited, s.Deleted, s.Refreshed)
	fmt.Fprintf(&b, "queries:         %d (errors %d, slow %d)  p50 %.1fms  p99 %.1fms\n", s.Queries, s.QueryErrors, s.SlowQueries, s.P50ms, s.P99ms)
	if len(s.Profiles) > 0 {
		fmt.Fprintf(&b, "query mix:       %s\n", formatProfiles(s.Profiles))
	}
	fmt.Fprintf(&b, "recall:          ok %d, miss %d\n", s.Oracle.RecallOK, s.Oracle.RecallMiss)
	fmt.Fprintf(&b, "probes(expired): %d, stale-on-probe %d\n", s.Oracle.Probes, s.Oracle.ProbeStale)
	fmt.Fprintf(&b, "STALE HITS:      %d (expired %d, deleted %d, probe %d)  -> correctness %s\n",
		s.StaleHits, s.Oracle.StaleExpired, s.Oracle.StaleDeleted, s.Oracle.ProbeStale, verdict)
	fmt.Fprintf(&b, "footprint:       metric=%s  mean=%s  slope=%s/s  -> %s\n",
		s.Footprint.Metric, humanBytes(int64(s.Footprint.MeanBytes)),
		humanBytes(int64(s.Footprint.SlopeBytesSec)),
		plateauLabel(s.Footprint.Plateau))
	fmt.Fprintf(&b, "                 %s\n", s.Footprint.Note)
	fmt.Fprintf(&b, "==============================\n")
	return b.String()
}

func plateauLabel(p bool) string {
	if p {
		return "PLATEAU"
	}
	return "GROWING"
}

// formatProfiles renders per-profile query counts sorted by count, descending.
func formatProfiles(p map[string]int64) string {
	type kv struct {
		k string
		v int64
	}
	kvs := make([]kv, 0, len(p))
	for k, v := range p {
		kvs = append(kvs, kv{k, v})
	}
	sort.Slice(kvs, func(i, j int) bool { return kvs[i].v > kvs[j].v })
	parts := make([]string, 0, len(kvs))
	for _, e := range kvs {
		parts = append(parts, fmt.Sprintf("%s=%d", e.k, e.v))
	}
	return strings.Join(parts, ", ")
}

func writeJSON(path string, v any) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func writeCSV(path string, samples []metrics.Sample) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	fmt.Fprintln(f, "t_sec,disk_usage,used_mem,num_docs,num_records,inverted_mb,async_expired,compaction_cycles,pending_compaction")
	for _, s := range samples {
		fmt.Fprintf(f, "%.1f,%d,%d,%d,%d,%.3f,%d,%d,%d\n",
			s.TSec, s.DiskUsage, s.UsedMem, s.NumDocs, s.NumRecords,
			s.InvertedMB, s.AsyncExpired, s.CompactionCycles, s.PendingCompaction)
	}
	return nil
}

func humanBytes(n int64) string {
	f := float64(n)
	units := []string{"B", "KB", "MB", "GB", "TB"}
	i := 0
	for f >= 1024 && i < len(units)-1 {
		f /= 1024
		i++
	}
	return fmt.Sprintf("%.1f%s", f, units[i])
}
