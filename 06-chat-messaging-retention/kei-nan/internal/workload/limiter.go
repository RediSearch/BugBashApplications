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
	for {
		select {
		case <-l.stop:
			return
		case <-t.C:
			r := l.rate()
			if r <= 0 {
				// Unlimited: new Wait() callers short-circuit, but a worker already
				// blocked on <-l.ch (from when a rate WAS set) won't re-check the
				// rate until it receives a token — so keep the bucket topped up to
				// release them promptly.
				acc = 0
				for i := 0; i < cap(l.ch); i++ {
					select {
					case l.ch <- struct{}{}:
					default:
						i = cap(l.ch)
					}
				}
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

// Wait blocks for a token, unless the rate is unlimited (returns immediately) or
// ctx is done.
func (l *RateLimiter) Wait(ctx context.Context) {
	if l.rate() <= 0 {
		return
	}
	select {
	case <-l.ch:
	case <-ctx.Done():
	}
}

// Close stops the token producer.
func (l *RateLimiter) Close() { close(l.stop) }
