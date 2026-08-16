package workload

import (
	"context"
	"sync/atomic"
	"time"
)

// seqCounter hands out globally-unique, monotonically increasing sequence ids.
type seqCounter struct{ n atomic.Int64 }

func (s *seqCounter) next() int64 { return s.n.Add(1) }

// Limiter is a simple token-bucket rate limiter. A rate of <= 0 means unlimited
// (Wait returns immediately). To keep the internal ticker coarse (>= 1ms) at high
// rates, it emits tokens in bursts.
type Limiter struct {
	ch   chan struct{}
	stop chan struct{}
}

// NewLimiter builds a limiter targeting ratePerSec permits/second.
func NewLimiter(ratePerSec int) *Limiter {
	if ratePerSec <= 0 {
		return &Limiter{} // unlimited
	}
	perTick := 1
	interval := time.Second / time.Duration(ratePerSec)
	for interval < time.Millisecond && perTick < 100_000 {
		perTick *= 2
		interval = time.Duration(perTick) * time.Second / time.Duration(ratePerSec)
	}
	l := &Limiter{ch: make(chan struct{}, perTick*2), stop: make(chan struct{})}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-l.stop:
				return
			case <-t.C:
				for i := 0; i < perTick; i++ {
					select {
					case l.ch <- struct{}{}:
					default:
					}
				}
			}
		}
	}()
	return l
}

// Wait blocks until a permit is available or ctx is done.
func (l *Limiter) Wait(ctx context.Context) {
	if l.ch == nil {
		return
	}
	select {
	case <-l.ch:
	case <-ctx.Done():
	}
}

// Close stops the limiter's ticker goroutine.
func (l *Limiter) Close() {
	if l.stop != nil {
		close(l.stop)
	}
}

// RateLimiter is a shared token-bucket whose target rate is read LIVE on every
// tick (0 = unlimited). Unlike a per-worker pre-sleep, tokens are produced
// independently of query latency, so N workers blocking on Wait() deliver the
// configured aggregate rate (capped only by what the workers can actually
// sustain). Used by the query workers so the "rate" control is honored.
type RateLimiter struct {
	ch   chan struct{}
	rate func() int
	stop chan struct{}
}

// NewRateLimiter starts a token producer reading rate() live.
func NewRateLimiter(rate func() int) *RateLimiter {
	l := &RateLimiter{ch: make(chan struct{}, 4096), rate: rate, stop: make(chan struct{})}
	go l.run()
	return l
}

func (l *RateLimiter) run() {
	const tick = 25 * time.Millisecond
	t := time.NewTicker(tick)
	defer t.Stop()
	var acc float64 // fractional token accumulator (handles low rates)
	prev := -1      // last observed rate, to detect changes
	for {
		select {
		case <-l.stop:
			return
		case <-t.C:
			r := l.rate()
			// On any rate DROP (including ->0), flush buffered permits. Otherwise a
			// backlog of stale tokens (e.g. accumulated while unlimited, or when
			// workers are latency-bound and can't keep up) lets workers burst at
			// full capacity for seconds before the new, lower rate takes effect.
			if r < prev {
				drain(l.ch)
			}
			prev = r
			if r <= 0 {
				acc = 0 // unlimited: Wait() returns without needing a token
				continue
			}
			acc += float64(r) * tick.Seconds()
			n := int(acc)
			acc -= float64(n)
			for i := 0; i < n; i++ {
				select {
				case l.ch <- struct{}{}:
				default: // bucket full: cap the burst, drop the surplus
					acc = 0
					i = n
				}
			}
		}
	}
}

// drain empties a token channel without blocking.
func drain(ch chan struct{}) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// Wait blocks for a token. An unlimited rate (<=0) returns immediately, and a
// rate change is picked up within the poll interval even while blocked — so
// switching TO unlimited never strands a worker waiting on the token channel,
// and no bucket "top-up" hack is needed.
func (l *RateLimiter) Wait(ctx context.Context) {
	const poll = 50 * time.Millisecond
	for {
		if l.rate() <= 0 {
			return
		}
		select {
		case <-l.ch:
			return
		case <-ctx.Done():
			return
		case <-time.After(poll):
			// re-check the rate (handles the ->0 transition without a token)
		}
	}
}

// Close stops the token producer.
func (l *RateLimiter) Close() { close(l.stop) }
