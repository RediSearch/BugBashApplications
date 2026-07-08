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
