// Package control holds the query settings that can be tuned live from the web
// UI while the workload runs: query rate, per-query timeout, result limit, and
// the query-profile mix. The workload reads these on every query; the web
// handlers mutate them. All access is concurrency-safe.
package control

import (
	"sync"
	"sync/atomic"

	"chatstress/internal/config"
)

// QueryItem is one concrete, ready-to-run query in the shared pool that the
// background query workers execute. Args are the arguments AFTER the index name.
type QueryItem struct {
	Profile   string
	Cmd       string // "FT.SEARCH" or "FT.AGGREGATE"
	Args      []any
	Display   string
	NoContent bool // FT.SEARCH NOCONTENT => reply keys are checkable
}

// Control is the shared, live-updatable query configuration.
type Control struct {
	queryRate   atomic.Int64 // queries/sec target (0 = unlimited)
	timeoutMs   atomic.Int64 // per-query TIMEOUT ms (0 = server default)
	limit       atomic.Int64 // FT.SEARCH LIMIT count
	concurrency atomic.Int64 // number of query worker threads/connections
	paused      atomic.Bool  // when true, all workers idle
	maxWorkers  int          // hard cap on concurrency (immutable; sizes the conn pool)

	mu          sync.RWMutex
	profiles    []config.QueryProfile
	totalWeight int

	poolMu sync.RWMutex
	pool   []QueryItem // the concrete queries the workers currently run
}

// New initializes control from the config.
func New(cfg *config.Config) *Control {
	c := &Control{maxWorkers: cfg.Query.MaxWorkers}
	c.queryRate.Store(int64(cfg.Query.Rate))
	c.timeoutMs.Store(0)
	c.limit.Store(int64(cfg.Query.Limit))
	c.concurrency.Store(int64(cfg.Query.Workers))
	c.SetProfiles(cfg.Query.Profiles)
	return c
}

// Concurrency / SetConcurrency — number of query worker threads (clamped to
// [1, maxWorkers]). Increasing it drives more load across more connections.
func (c *Control) Concurrency() int { return int(c.concurrency.Load()) }
func (c *Control) SetConcurrency(v int) {
	if v < 1 {
		v = 1
	}
	if v > c.maxWorkers {
		v = c.maxWorkers
	}
	c.concurrency.Store(int64(v))
}

// MaxWorkers is the hard concurrency cap.
func (c *Control) MaxWorkers() int { return c.maxWorkers }

// Paused / SetPaused — when paused, all workers idle (no ingest/query/mutation).
func (c *Control) Paused() bool     { return c.paused.Load() }
func (c *Control) SetPaused(v bool) { c.paused.Store(v) }

// SetPool replaces the query pool the workers run.
func (c *Control) SetPool(items []QueryItem) {
	c.poolMu.Lock()
	c.pool = items
	c.poolMu.Unlock()
}

// PoolSize returns the number of queries currently in the pool.
func (c *Control) PoolSize() int {
	c.poolMu.RLock()
	defer c.poolMu.RUnlock()
	return len(c.pool)
}

// PickPooled returns a query from the pool chosen by the given random int.
// ok is false when the pool is empty.
func (c *Control) PickPooled(rn int) (QueryItem, bool) {
	c.poolMu.RLock()
	defer c.poolMu.RUnlock()
	if len(c.pool) == 0 {
		return QueryItem{}, false
	}
	if rn < 0 {
		rn = -rn
	}
	return c.pool[rn%len(c.pool)], true
}

// QueryRate / SetQueryRate — target queries per second (0 = unlimited).
func (c *Control) QueryRate() int { return int(c.queryRate.Load()) }
func (c *Control) SetQueryRate(v int) {
	if v < 0 {
		v = 0
	}
	c.queryRate.Store(int64(v))
}

// TimeoutMs / SetTimeoutMs — per-query TIMEOUT in ms (0 = server default).
func (c *Control) TimeoutMs() int { return int(c.timeoutMs.Load()) }
func (c *Control) SetTimeoutMs(v int) {
	if v < 0 {
		v = 0
	}
	c.timeoutMs.Store(int64(v))
}

// Limit / SetLimit — FT.SEARCH result limit.
func (c *Control) Limit() int { return int(c.limit.Load()) }
func (c *Control) SetLimit(v int) {
	if v < 1 {
		v = 1
	}
	c.limit.Store(int64(v))
}

// SetProfiles replaces the mix, keeping only known profiles with positive weight.
func (c *Control) SetProfiles(ps []config.QueryProfile) {
	var kept []config.QueryProfile
	total := 0
	for _, p := range ps {
		if _, ok := config.KnownProfiles[p.Name]; ok && p.Weight > 0 {
			kept = append(kept, p)
			total += p.Weight
		}
	}
	c.mu.Lock()
	c.profiles = kept
	c.totalWeight = total
	c.mu.Unlock()
}

// Profiles returns a copy of the current mix.
func (c *Control) Profiles() []config.QueryProfile {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]config.QueryProfile, len(c.profiles))
	copy(out, c.profiles)
	return out
}

// PickProfile chooses a profile by weight given a non-negative random int.
// Returns "" if no profiles are configured.
func (c *Control) PickProfile(rn int) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.totalWeight <= 0 {
		return ""
	}
	if rn < 0 {
		rn = -rn
	}
	x := rn % c.totalWeight
	for _, p := range c.profiles {
		if x < p.Weight {
			return p.Name
		}
		x -= p.Weight
	}
	return c.profiles[len(c.profiles)-1].Name
}
