// Package workload drives the use-case-6 stress workload: continuous ingest with
// per-message whole-key TTLs, sliding-TTL refreshes, edits, deletes, and scoped
// full-text queries, plus a metrics sampler. Expiry runs continuously as a
// side-effect of Redis key expiration; the harness never deletes-on-expiry.
package workload

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"chatstress/internal/config"
	"chatstress/internal/gendata"
	"chatstress/internal/metrics"
	"chatstress/internal/model"
	"chatstress/internal/oracle"
	"chatstress/internal/redisx"
)

// Runner owns the worker pool and shared state.
type Runner struct {
	cfg   *config.Config
	cli   *redisx.Client
	orc   *oracle.Oracle
	met   *metrics.Metrics
	space model.Space
	seq   seqCounter
}

// New builds a Runner over shared metrics/oracle/client.
func New(cfg *config.Config, cli *redisx.Client, orc *oracle.Oracle, met *metrics.Metrics) *Runner {
	return &Runner{
		cfg: cfg, cli: cli, orc: orc, met: met,
		space: model.Space{
			Tenants:           cfg.Tenants,
			ChannelsPerTenant: cfg.ChannelsPerTenant,
			UsersPerTenant:    cfg.UsersPerTenant,
			ThreadsPerChannel: cfg.ThreadsPerChannel,
		},
	}
}

// Run launches all workers and blocks until ctx is cancelled and they drain.
func (r *Runner) Run(ctx context.Context) {
	var wg sync.WaitGroup

	batchRate := 0
	if r.cfg.Ingest.Rate > 0 {
		batchRate = ceilDiv(r.cfg.Ingest.Rate, r.cfg.Ingest.Pipeline)
	}
	ingestLim := NewLimiter(batchRate)
	queryLim := NewLimiter(r.cfg.Query.Rate)
	editLim := NewLimiter(r.cfg.Edit.Rate)
	delLim := NewLimiter(r.cfg.Delete.Rate)
	slideLim := NewLimiter(r.cfg.Sliding.Rate)
	defer func() {
		ingestLim.Close()
		queryLim.Close()
		editLim.Close()
		delLim.Close()
		slideLim.Close()
	}()

	for i := 0; i < r.cfg.Ingest.Workers; i++ {
		wg.Add(1)
		go func(id int) { defer wg.Done(); r.ingester(ctx, id, ingestLim) }(i)
	}
	for i := 0; i < r.cfg.Query.Workers; i++ {
		wg.Add(1)
		go func(id int) { defer wg.Done(); r.queryer(ctx, id, queryLim) }(i)
	}
	wg.Add(3)
	go func() { defer wg.Done(); r.editor(ctx, editLim) }()
	go func() { defer wg.Done(); r.deleter(ctx, delLim) }()
	go func() { defer wg.Done(); r.slider(ctx, slideLim) }()

	wg.Add(1)
	go func() { defer wg.Done(); r.metricLoop(ctx) }()
	if r.cfg.Sample.ForceGC && r.cfg.Sample.GCInterval.D() > 0 {
		wg.Add(1)
		go func() { defer wg.Done(); r.gcLoop(ctx) }()
	}

	wg.Wait()
}

// --- workers ---

func (r *Runner) ingester(ctx context.Context, id int, lim *Limiter) {
	pick := model.NewPicker(r.space, r.cfg.Seed, int64(id)*101+1)
	gen := gendata.New(r.cfg.Seed, int64(id)*307+5, r.cfg.Body.MinWords, r.cfg.Body.MaxWords)
	rnd := rand.New(rand.NewSource(r.cfg.Seed*911 + int64(id)))
	pipeline := r.cfg.Ingest.Pipeline

	type pending struct {
		msg   *model.Message
		ttlMs int64
	}
	for {
		if ctx.Err() != nil {
			return
		}
		lim.Wait(ctx)

		batch := make([]*model.Message, 0, pipeline)
		pend := make([]pending, 0, pipeline)
		nowMs := time.Now().UnixMilli()
		for j := 0; j < pipeline; j++ {
			tier := r.pickTier(rnd)
			tid, tenant := pick.Tenant()
			cid, channel := pick.ChannelID(tid)
			body, token := gen.Body()
			seq := r.seq.next()
			m := &model.Message{
				Key:      model.Key(r.cfg.Prefix, seq),
				Seq:      seq,
				Tenant:   tenant,
				Channel:  channel,
				User:     pick.User(tid),
				Thread:   pick.Thread(cid),
				Plan:     tier.Name,
				Body:     body,
				Token:    token,
				TTL:      tier.TTL.D(),
				CreateMs: nowMs,
			}
			batch = append(batch, m)
			pend = append(pend, pending{msg: m, ttlMs: int64(tier.TTL.D() / time.Millisecond)})
		}

		if err := r.cli.IngestBatch(ctx, batch); err != nil {
			if ctx.Err() == nil { // don't count batches cancelled at shutdown
				r.met.C.IngestErrors.Add(int64(len(batch)))
			}
			continue
		}
		// Compute expiry AFTER the write so the oracle's expireAt is an upper
		// bound of the server's stored expiry (server set-time <= now), which
		// makes the "expired before t0" check free of false positives.
		afterMs := time.Now().UnixMilli()
		for _, p := range pend {
			r.orc.Add(p.msg.Key, p.msg.Channel, p.msg.Token, afterMs+p.ttlMs, p.msg.Seq)
		}
		r.met.C.Ingested.Add(int64(len(batch)))
	}
}

func (r *Runner) queryer(ctx context.Context, id int, lim *Limiter) {
	rnd := rand.New(rand.NewSource(r.cfg.Seed*53 + int64(id)*17 + 3))
	n := r.cfg.Query.Limit
	iter := 0
	for {
		if ctx.Err() != nil {
			return
		}
		lim.Wait(ctx)
		iter++

		sample, ok := r.orc.LiveSample()
		if !ok {
			continue
		}

		scoped := rnd.Float64() < 0.85
		var query string
		if scoped {
			query = "@" + model.FieldChannel + ":{" + sample.Channel + "} " + sample.Token
		} else {
			query = sample.Token // unscoped: stresses high-cardinality posting lists
		}

		t0 := time.Now().UnixMilli()
		start := time.Now()
		_, keys, err := r.cli.Search(ctx, query, n)
		r.met.Lat.Record(time.Since(start))
		r.met.C.Queries.Add(1)
		if err != nil {
			if ctx.Err() == nil { // don't count queries cancelled at shutdown
				r.met.C.QueryErrors.Add(1)
			}
			continue
		}

		found := false
		for _, k := range keys {
			if k == sample.Key {
				found = true
			}
			r.orc.Check(k, t0) // records any stale-hit violation
		}
		if scoped {
			r.orc.RecordRecall(found)
		}

		// Every few queries, probe a comfortably-expired doc: it must be absent.
		if iter%5 == 0 {
			if es, ok := r.orc.ExpiredSample(); ok {
				pq := "@" + model.FieldChannel + ":{" + es.Channel + "} " + es.Token
				pt0 := time.Now().UnixMilli()
				_, pkeys, perr := r.cli.Search(ctx, pq, n)
				if perr == nil {
					hit := false
					for _, k := range pkeys {
						if k == es.Key {
							hit = true
						}
						r.orc.Check(k, pt0)
					}
					r.orc.RecordProbe(hit)
				}
			}
		}
	}
}

func (r *Runner) editor(ctx context.Context, lim *Limiter) {
	gen := gendata.New(r.cfg.Seed, 999, r.cfg.Body.MinWords, r.cfg.Body.MaxWords)
	for {
		if ctx.Err() != nil {
			return
		}
		lim.Wait(ctx)
		s, ok := r.orc.LiveSample()
		if !ok {
			continue
		}
		body, token := gen.Body()
		if err := r.cli.EditBody(ctx, s.Key, body); err != nil {
			continue
		}
		r.orc.Edit(s.Key, token)
		r.met.C.Edited.Add(1)
	}
}

func (r *Runner) deleter(ctx context.Context, lim *Limiter) {
	for {
		if ctx.Err() != nil {
			return
		}
		lim.Wait(ctx)
		s, ok := r.orc.LiveSample()
		if !ok {
			continue
		}
		if err := r.cli.Delete(ctx, s.Key); err != nil {
			continue
		}
		r.orc.MarkDeleted(s.Key, time.Now().UnixMilli())
		r.met.C.Deleted.Add(1)
	}
}

func (r *Runner) slider(ctx context.Context, lim *Limiter) {
	rnd := rand.New(rand.NewSource(r.cfg.Seed*7 + 13))
	for {
		if ctx.Err() != nil {
			return
		}
		lim.Wait(ctx)
		s, ok := r.orc.LiveSample()
		if !ok {
			continue
		}
		tier := r.pickTier(rnd)
		ttl := tier.TTL.D()
		ok2, err := r.cli.Refresh(ctx, s.Key, ttl)
		if err != nil || !ok2 {
			continue
		}
		afterMs := time.Now().UnixMilli()
		r.orc.Refresh(s.Key, afterMs+int64(ttl/time.Millisecond))
		r.met.C.Refreshed.Add(1)
	}
}

// metricLoop samples INFO/FT.INFO and prunes the oracle on a fixed cadence. It
// runs independently of gcLoop so a slow/blocking forced GC never stalls metric
// collection.
func (r *Runner) metricLoop(ctx context.Context) {
	tick := time.NewTicker(r.cfg.Sample.Interval.D())
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			sctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			si, _ := r.cli.ServerInfo(sctx)
			fi, _ := r.cli.Info(sctx)
			cancel()
			r.met.AddSample(metrics.Sample{
				TSec:              r.met.Elapsed().Seconds(),
				DiskUsage:         si.DiskUsage,
				UsedMem:           si.UsedMemory,
				NumDocs:           fi.NumDocs,
				NumRecords:        fi.NumRecords,
				InvertedMB:        fi.InvertedSzMB,
				AsyncExpired:      si.AsyncReadsExpired,
				CompactionCycles:  si.CompactionCycles,
				PendingCompaction: si.PendingCompactionBytes,
			}, si.DiskMode)
			r.orc.Prune()
		}
	}
}

// gcLoop periodically forces a flush + GC/compaction so on-disk size is realized
// for measurement. The ticker naturally single-flights: if a forced GC outlasts
// the interval, the next tick is dropped rather than queued. Both calls are
// best-effort and no-op / harmless on an in-RAM (non-disk) server.
func (r *Runner) gcLoop(ctx context.Context) {
	tick := time.NewTicker(r.cfg.Sample.GCInterval.D())
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			gctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			_ = r.cli.DiskFlush(gctx)
			_ = r.cli.ForceGC(gctx)
			cancel()
		}
	}
}

// --- helpers ---

func (r *Runner) pickTier(rnd *rand.Rand) config.Tier {
	total := r.cfg.TotalTierWeight()
	x := rnd.Intn(total)
	for _, t := range r.cfg.Tiers {
		if x < t.Weight {
			return t
		}
		x -= t.Weight
	}
	return r.cfg.Tiers[len(r.cfg.Tiers)-1]
}

func ceilDiv(a, b int) int {
	if b <= 0 {
		return a
	}
	return (a + b - 1) / b
}
