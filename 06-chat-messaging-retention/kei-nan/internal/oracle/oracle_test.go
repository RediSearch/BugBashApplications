package oracle

import (
	"math"
	"testing"
	"time"
)

const (
	testGrace = 1500 // ms: deletion de-index grace
	testSkew  = 1000 // ms: expiry clock-skew tolerance
	t0        = int64(1_700_000_000_000)
)

func newTestOracle() *Oracle {
	// sampleRate 1.0 => every key is tracked, so Check is deterministic.
	return New(8, 1_000_000, 1.0, testGrace, testSkew)
}

// TestOracleCheckExpiry covers the t0/skew rule for the expiry classification.
func TestOracleCheckExpiry(t *testing.T) {
	cases := []struct {
		name     string
		expireAt int64
		want     Violation
	}{
		{"expired well before t0 (beyond skew)", t0 - 5000, Expired},
		{"expired just past the skew boundary", t0 - testSkew - 1, Expired},
		{"expired within skew tolerance", t0 - testSkew + 1, None},
		{"expires exactly at t0 (not yet, within skew)", t0, None},
		{"not expired", t0 + 5000, None},
		{"persistent (never expires)", math.MaxInt64, None},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			o := newTestOracle()
			o.Add("k", "c1", "tok", c.expireAt, 1)
			if got := o.Check("k", t0); got != c.want {
				t.Fatalf("Check=%v, want %v (expireAt=%d, t0=%d, skew=%d)", got, c.want, c.expireAt, t0, testSkew)
			}
		})
	}
}

// TestOracleCheckDeletion covers the grace window for the deletion classification.
func TestOracleCheckDeletion(t *testing.T) {
	cases := []struct {
		name      string
		deletedAt int64
		want      Violation
	}{
		{"deleted well before t0 (beyond grace)", t0 - 5000, Deleted},
		{"deleted just past the grace boundary", t0 - testGrace - 1, Deleted},
		{"deleted within grace (async de-index tolerated)", t0 - testGrace + 1, None},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			o := newTestOracle()
			o.Add("k", "c1", "tok", math.MaxInt64, 1) // persistent so expiry can't fire
			o.MarkDeleted("k", c.deletedAt)
			if got := o.Check("k", t0); got != c.want {
				t.Fatalf("Check=%v, want %v (deletedAt=%d, t0=%d, grace=%d)", got, c.want, c.deletedAt, t0, testGrace)
			}
		})
	}
}

// TestOracleCheckUntracked: a key the oracle never saw is not classifiable.
func TestOracleCheckUntracked(t *testing.T) {
	o := newTestOracle()
	if got := o.Check("never-added", t0); got != None {
		t.Fatalf("Check on untracked key = %v, want None", got)
	}
}

// TestOracleCheckCounters: a genuine expired stale hit is recorded exactly once.
func TestOracleCheckCounters(t *testing.T) {
	o := newTestOracle()
	o.Add("k", "c1", "tok", t0-5000, 1)
	o.Check("k", t0)
	o.Check("k", t0)
	s := o.Stats()
	if s.StaleExpired != 2 || s.StaleDeleted != 0 {
		t.Fatalf("staleExpired=%d staleDeleted=%d, want 2/0", s.StaleExpired, s.StaleDeleted)
	}
	if o.StaleHits() != 2 {
		t.Fatalf("StaleHits=%d, want 2", o.StaleHits())
	}
}

// TestOraclePrune removes only entries that expired/were deleted well in the past.
func TestOraclePrune(t *testing.T) {
	o := newTestOracle()
	o.Add("live", "c1", "tok", math.MaxInt64, 1)   // persistent -> keep
	o.Add("recent", "c1", "tok", math.MaxInt64, 2) // will delete recently -> keep
	o.MarkDeleted("recent", time.Now().UnixMilli())
	before := o.Stats().Tracked
	if before != 2 {
		t.Fatalf("tracked=%d, want 2", before)
	}
	if removed := o.Prune(); removed != 0 {
		t.Fatalf("Prune removed %d, want 0 (nothing is old enough)", removed)
	}
}
