// Package oracle is the client-side source of truth for the "no stale hits"
// correctness check. It tracks a (bounded, optionally sampled) set of messages
// and their authoritative whole-key expiry, so the harness can decide whether a
// document returned by a query should still be visible.
//
// The core rule: capture t0 (ms) *before* issuing a query. A returned document
// is a stale-hit violation if the oracle knows it expired at (or before) t0, or
// was deleted at (or before) t0 — because the server evaluates expiry against a
// clock >= t0, so anything already dead before we asked must not appear. This
// avoids false positives for documents that expire *during* query execution.
package oracle

import (
	"sync"
	"sync/atomic"
	"time"
)

// Violation classifies a stale hit.
type Violation int

const (
	None Violation = iota
	Expired
	Deleted
)

type entry struct {
	channel   string
	token     string
	expireAt  int64 // unix ms — authoritative expected expiry
	deletedAt int64 // unix ms — 0 if not deleted
	seq       int64
}

type shard struct {
	mu sync.RWMutex
	m  map[string]*entry
}

// Oracle tracks live messages for correctness checks.
type Oracle struct {
	shards          []shard
	cap             int
	sampleThreshold uint64 // key-hash % 1e6 < threshold => tracked
	graceMs         int64  // grace before a deleted doc counts as stale (async de-index)
	skewMs          int64  // clock-skew tolerance for the expiry check (client vs server clock)
	retainMs        int64
	rr              atomic.Uint64 // round-robin shard selector for sampling

	// atomic counters
	tracked      atomic.Int64
	dropped      atomic.Int64
	staleExpired atomic.Int64
	staleDeleted atomic.Int64
	probes       atomic.Int64
	probeStale   atomic.Int64
	recallMiss   atomic.Int64
	recallOK     atomic.Int64
}

// New builds an Oracle with the given shard count, tracking cap, sample rate
// (0..1), post-expiry/deletion grace (ms), and clock-skew tolerance (ms) for the
// expiry check.
func New(shards, cap int, sampleRate float64, graceMs, skewMs int) *Oracle {
	if shards < 1 {
		shards = 1
	}
	o := &Oracle{
		shards:          make([]shard, shards),
		cap:             cap,
		sampleThreshold: uint64(sampleRate * 1_000_000),
		graceMs:         int64(graceMs),
		skewMs:          int64(skewMs),
		retainMs:        30_000,
	}
	for i := range o.shards {
		o.shards[i].m = make(map[string]*entry)
	}
	return o
}

func fnv64a(s string) uint64 {
	const prime = 1099511628211
	h := uint64(14695981039346656037)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime
	}
	return h
}

func (o *Oracle) shardFor(key string) *shard {
	return &o.shards[fnv64a(key)%uint64(len(o.shards))]
}

// tracks reports whether a key is in the (deterministic) tracking sample.
func (o *Oracle) tracks(key string) bool {
	return fnv64a("s:"+key)%1_000_000 < o.sampleThreshold
}

// Add records a newly-ingested message if it falls in the tracking sample and
// we're under the cap.
func (o *Oracle) Add(key, channel, token string, expireAt, seq int64) {
	if !o.tracks(key) {
		o.dropped.Add(1)
		return
	}
	if int(o.tracked.Load()) >= o.cap {
		o.dropped.Add(1)
		return
	}
	sh := o.shardFor(key)
	sh.mu.Lock()
	if _, exists := sh.m[key]; !exists {
		sh.m[key] = &entry{channel: channel, token: token, expireAt: expireAt, seq: seq}
		o.tracked.Add(1)
	} else {
		sh.m[key] = &entry{channel: channel, token: token, expireAt: expireAt, seq: seq}
	}
	sh.mu.Unlock()
}

// Refresh updates a tracked message's expiry (sliding TTL).
func (o *Oracle) Refresh(key string, expireAt int64) {
	sh := o.shardFor(key)
	sh.mu.Lock()
	if e := sh.m[key]; e != nil && e.deletedAt == 0 {
		e.expireAt = expireAt
	}
	sh.mu.Unlock()
}

// Edit updates a tracked message's searchable token after a body rewrite.
func (o *Oracle) Edit(key, token string) {
	sh := o.shardFor(key)
	sh.mu.Lock()
	if e := sh.m[key]; e != nil {
		e.token = token
	}
	sh.mu.Unlock()
}

// MarkDeleted records that a message was deleted at atMs.
func (o *Oracle) MarkDeleted(key string, atMs int64) {
	sh := o.shardFor(key)
	sh.mu.Lock()
	if e := sh.m[key]; e != nil && e.deletedAt == 0 {
		e.deletedAt = atMs
	}
	sh.mu.Unlock()
}

// Check classifies a returned document against t0 and records any violation.
func (o *Oracle) Check(key string, t0Ms int64) Violation {
	sh := o.shardFor(key)
	sh.mu.RLock()
	e := sh.m[key]
	var exp, del int64
	if e != nil {
		exp, del = e.expireAt, e.deletedAt
	}
	sh.mu.RUnlock()
	if e == nil {
		return None // untracked (sampled out) — not classifiable
	}
	// Expiry has a synchronous read-time filter (the server compares its stored
	// expiration against the query clock). The oracle's expireAt is derived from
	// the CLIENT clock, so against a remote server we require the doc to be
	// expired by more than skewMs before t0 — this tolerates NTP-level client/
	// server clock skew and avoids false stale-hits at the expiry boundary, while
	// still catching the real (multi-second) GC/reap-lag stale hits.
	if exp <= t0Ms-o.skewMs {
		o.staleExpired.Add(1)
		return Expired
	}
	// Deletion (UNLINK) de-indexes ASYNCHRONOUSLY (keyspace-notification driven),
	// so a doc may legitimately appear for a brief window after deletion. Only
	// flag it as stale if it is still visible comfortably (grace) after deletion.
	if del != 0 && del <= t0Ms-o.graceMs {
		o.staleDeleted.Add(1)
		return Deleted
	}
	return None
}

// Sample is a lightweight snapshot handed to query workers.
type Sample struct {
	Key     string
	Channel string
	Token   string
}

// LiveSample returns a currently-live tracked message (for recall/positive
// queries), scanning up to a few shards. ok=false if none found.
func (o *Oracle) LiveSample() (Sample, bool) {
	now := time.Now().UnixMilli()
	for try := 0; try < 4; try++ {
		sh := &o.shards[o.rr.Add(1)%uint64(len(o.shards))]
		sh.mu.RLock()
		for k, e := range sh.m {
			if e.deletedAt == 0 && e.expireAt > now {
				s := Sample{Key: k, Channel: e.channel, Token: e.token}
				sh.mu.RUnlock()
				return s, true
			}
		}
		sh.mu.RUnlock()
	}
	return Sample{}, false
}

// ExpiredSample returns a message that expired at least `grace` ago and was not
// deleted (so the *reason* it must be absent is TTL expiry). ok=false if none.
func (o *Oracle) ExpiredSample() (Sample, bool) {
	cutoff := time.Now().UnixMilli() - o.graceMs
	for try := 0; try < 4; try++ {
		sh := &o.shards[o.rr.Add(1)%uint64(len(o.shards))]
		sh.mu.RLock()
		for k, e := range sh.m {
			if e.deletedAt == 0 && e.expireAt <= cutoff {
				s := Sample{Key: k, Channel: e.channel, Token: e.token}
				sh.mu.RUnlock()
				return s, true
			}
		}
		sh.mu.RUnlock()
	}
	return Sample{}, false
}

// RecordRecall notes whether a positive query returned the expected live key.
func (o *Oracle) RecordRecall(found bool) {
	if found {
		o.recallOK.Add(1)
	} else {
		o.recallMiss.Add(1)
	}
}

// RecordProbe notes a probe of a known-expired doc; found=true is a stale hit.
func (o *Oracle) RecordProbe(found bool) {
	o.probes.Add(1)
	if found {
		o.probeStale.Add(1)
	}
}

// Prune drops entries that expired/were deleted comfortably in the past. Returns
// how many were removed. Call periodically to bound client memory.
func (o *Oracle) Prune() int {
	now := time.Now().UnixMilli()
	removed := 0
	for i := range o.shards {
		sh := &o.shards[i]
		sh.mu.Lock()
		for k, e := range sh.m {
			gone := (e.expireAt > 0 && e.expireAt <= now-o.retainMs) ||
				(e.deletedAt != 0 && e.deletedAt <= now-o.retainMs)
			if gone {
				delete(sh.m, k)
				removed++
			}
		}
		sh.mu.Unlock()
	}
	if removed > 0 {
		o.tracked.Add(int64(-removed))
	}
	return removed
}

// TopChannels tallies channels across a bounded scan of tracked live messages.
func (o *Oracle) TopChannels(maxScan int) map[string]int {
	now := time.Now().UnixMilli()
	counts := make(map[string]int)
	scanned := 0
	for i := range o.shards {
		sh := &o.shards[i]
		sh.mu.RLock()
		for _, e := range sh.m {
			if e.deletedAt == 0 && e.expireAt > now {
				counts[e.channel]++
			}
			scanned++
			if scanned >= maxScan {
				sh.mu.RUnlock()
				return counts
			}
		}
		sh.mu.RUnlock()
	}
	return counts
}

// Stats is a snapshot of oracle counters.
type Stats struct {
	Tracked      int64 `json:"tracked"`
	Dropped      int64 `json:"dropped"`
	StaleExpired int64 `json:"stale_expired"`
	StaleDeleted int64 `json:"stale_deleted"`
	Probes       int64 `json:"probes"`
	ProbeStale   int64 `json:"probe_stale"`
	RecallOK     int64 `json:"recall_ok"`
	RecallMiss   int64 `json:"recall_miss"`
}

// Stats returns the current counters.
func (o *Oracle) Stats() Stats {
	return Stats{
		Tracked:      o.tracked.Load(),
		Dropped:      o.dropped.Load(),
		StaleExpired: o.staleExpired.Load(),
		StaleDeleted: o.staleDeleted.Load(),
		Probes:       o.probes.Load(),
		ProbeStale:   o.probeStale.Load(),
		RecallOK:     o.recallOK.Load(),
		RecallMiss:   o.recallMiss.Load(),
	}
}

// StaleHits is the total number of correctness violations observed.
func (o *Oracle) StaleHits() int64 {
	return o.staleExpired.Load() + o.staleDeleted.Load() + o.probeStale.Load()
}
