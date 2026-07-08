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
	"chatstress/internal/control"
	"chatstress/internal/gendata"
	"chatstress/internal/metrics"
	"chatstress/internal/model"
	"chatstress/internal/oracle"
	"chatstress/internal/querygen"
	"chatstress/internal/redisx"
)

// Runner owns the worker pool and shared state.
type Runner struct {
	cfg       *config.Config
	cli       *redisx.Client
	orc       *oracle.Oracle
	met       *metrics.Metrics
	ctrl      *control.Control
	space     model.Space
	tierNames []string
	seq       seqCounter

	// pool-generation state (used single-threaded at startup)
	genRnd  *rand.Rand
	genPick *model.Picker
	genGen  *gendata.Generator
}

// New builds a Runner over shared metrics/oracle/client/control.
func New(cfg *config.Config, cli *redisx.Client, orc *oracle.Oracle, met *metrics.Metrics, ctrl *control.Control) *Runner {
	tiers := make([]string, len(cfg.Tiers))
	for i, t := range cfg.Tiers {
		tiers[i] = t.Name
	}
	space := model.Space{
		Tenants:           cfg.Tenants,
		ChannelsPerTenant: cfg.ChannelsPerTenant,
		UsersPerTenant:    cfg.UsersPerTenant,
		ThreadsPerChannel: cfg.ThreadsPerChannel,
	}
	return &Runner{
		cfg: cfg, cli: cli, orc: orc, met: met, ctrl: ctrl, tierNames: tiers, space: space,
		genRnd:  rand.New(rand.NewSource(cfg.Seed*131 + 17)),
		genPick: model.NewPicker(space, cfg.Seed, 31337),
		genGen:  gendata.New(cfg.Seed, 4242, cfg.Body.MinWords, cfg.Body.MaxWords, 0),
	}
}

// Run launches all workers and blocks until ctx is cancelled and they drain.
func (r *Runner) Run(ctx context.Context) {
	var wg sync.WaitGroup

	r.regenPool() // populate the initial query pool the workers run

	batchRate := 0
	if r.cfg.Ingest.Rate > 0 {
		batchRate = ceilDiv(r.cfg.Ingest.Rate, r.cfg.Ingest.Pipeline)
	}
	ingestLim := NewLimiter(batchRate)
	editLim := NewLimiter(r.cfg.Edit.Rate)
	delLim := NewLimiter(r.cfg.Delete.Rate)
	slideLim := NewLimiter(r.cfg.Sliding.Rate)
	defer func() {
		ingestLim.Close()
		editLim.Close()
		delLim.Close()
		slideLim.Close()
	}()

	for i := 0; i < r.cfg.Ingest.Workers; i++ {
		wg.Add(1)
		go func(id int) { defer wg.Done(); r.ingester(ctx, id, ingestLim) }(i)
	}
	// A supervisor spawns/stops query workers to match the live concurrency
	// (threads) set from the UI; each self-paces to the live rate.
	wg.Add(1)
	go func() { defer wg.Done(); r.querySupervisor(ctx) }()

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
	gen := gendata.New(r.cfg.Seed, int64(id)*307+5, r.cfg.Body.MinWords, r.cfg.Body.MaxWords, r.cfg.Body.VocabHigh)
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
		if r.ctrl.Paused() {
			sleepCtx(ctx, 200*time.Millisecond)
			continue
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

// querySupervisor keeps the number of live query workers equal to the control's
// concurrency setting, spawning new workers or cancelling extras as it changes.
func (r *Runner) querySupervisor(ctx context.Context) {
	var workers []context.CancelFunc
	var wwg sync.WaitGroup
	nextID := 0

	spawn := func() {
		wctx, cancel := context.WithCancel(ctx)
		workers = append(workers, cancel)
		id := nextID
		nextID++
		wwg.Add(1)
		go func() { defer wwg.Done(); r.queryer(wctx, id) }()
	}
	shrink := func() {
		last := len(workers) - 1
		workers[last]() // cancel
		workers = workers[:last]
	}
	adjust := func() {
		target := r.ctrl.Concurrency()
		for len(workers) < target {
			spawn()
		}
		for len(workers) > target {
			shrink()
		}
	}

	adjust()
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			wwg.Wait() // workers observe ctx cancellation
			return
		case <-tick.C:
			adjust()
		}
	}
}

func (r *Runner) queryer(ctx context.Context, id int) {
	rnd := rand.New(rand.NewSource(r.cfg.Seed*53 + int64(id)*17 + 3))
	slowMs := r.cfg.Query.SlowMs
	iter := 0
	for {
		if ctx.Err() != nil {
			return
		}
		if r.ctrl.Paused() {
			sleepCtx(ctx, 200*time.Millisecond)
			continue
		}
		r.paceQuery(ctx)

		item, ok := r.ctrl.PickPooled(rnd.Int())
		if !ok {
			sleepCtx(ctx, 100*time.Millisecond) // pool not populated yet
			continue
		}
		iter++
		to := r.ctrl.TimeoutMs()
		start := time.Now()
		total, err := r.runItem(ctx, item, to)
		lat := time.Since(start)
		r.met.RecordQuery(item.Profile, lat, slowMs)
		if err != nil && ctx.Err() == nil {
			r.met.C.QueryErrors.Add(1)
		} else if err == nil && item.NoContent {
			r.orc.RecordRecall(total > 0) // hit-rate: did the search return anything
		}
		// Sample ~1/8 of executed queries into the live "recent queries" feed.
		if iter%8 == 0 && ctx.Err() == nil {
			errStr := ""
			if err != nil {
				errStr = err.Error()
			}
			r.met.PushQuery(metrics.QSample{
				TSec: r.met.Elapsed().Seconds(), Profile: item.Profile, Query: item.Display,
				Ms: float64(lat.Microseconds()) / 1000.0, Total: total, Err: errStr,
			})
		}
		// Every few queries, probe a comfortably-expired doc: it must be absent.
		if iter%5 == 0 {
			if es, ok := r.orc.ExpiredSample(); ok {
				pq := "@" + model.FieldChannel + ":{" + es.Channel + "} " + es.Token
				pt0 := time.Now().UnixMilli()
				_, pkeys, perr := r.cli.Search(ctx, pq, r.ctrl.Limit(), to)
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

// runItem executes one pooled query and returns the match/row count. When the
// query is a NOCONTENT search, the returned keys feed the no-stale-hits oracle.
func (r *Runner) runItem(ctx context.Context, item control.QueryItem, to int) (int64, error) {
	full := make([]any, 0, len(item.Args)+4)
	full = append(full, item.Cmd, r.cfg.Index)
	full = append(full, item.Args...)
	if to > 0 {
		full = append(full, "TIMEOUT", to)
	}
	t0 := time.Now().UnixMilli()
	total, keys, err := r.cli.RawCount(ctx, full...)
	if err == nil && item.NoContent {
		for _, k := range keys {
			r.orc.Check(k, t0)
		}
	}
	return total, err
}

// regenPool rebuilds the shared query pool from the current mix. Called at
// startup; the web handlers also regenerate it on demand (Randomize button).
func (r *Runner) regenPool() {
	d := querygen.Deps{
		Pick: r.genPick, Gen: r.genGen, Tiers: r.tierNames,
		PageDepth: r.cfg.Query.PageDepth, Limit: r.ctrl.Limit(), Live: r.orc.LiveSample,
	}
	r.ctrl.SetPool(querygen.Pool(r.genRnd, r.cfg.Query.PoolSize, r.ctrl.Profiles(), d))
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

// paceQuery self-throttles a query worker to (liveRate / liveConcurrency) qps,
// reading both live so the UI can change rate and thread count at runtime.
// rate <= 0 means unlimited (load is then bounded only by the thread count).
func (r *Runner) paceQuery(ctx context.Context) {
	rate := r.ctrl.QueryRate()
	workers := r.ctrl.Concurrency()
	if rate <= 0 || workers <= 0 {
		return
	}
	interval := time.Duration(int64(time.Second) * int64(workers) / int64(rate))
	if interval <= 0 {
		return
	}
	t := time.NewTimer(interval)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

func (r *Runner) editor(ctx context.Context, lim *Limiter) {
	gen := gendata.New(r.cfg.Seed, 999, r.cfg.Body.MinWords, r.cfg.Body.MaxWords, r.cfg.Body.VocabHigh)
	for {
		if ctx.Err() != nil {
			return
		}
		if r.ctrl.Paused() {
			sleepCtx(ctx, 200*time.Millisecond)
			continue
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
		if r.ctrl.Paused() {
			sleepCtx(ctx, 200*time.Millisecond)
			continue
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
		if r.ctrl.Paused() {
			sleepCtx(ctx, 200*time.Millisecond)
			continue
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
			r.met.SetStatus(metrics.Status{
				NumDocs:              fi.NumDocs,
				NumRecords:           fi.NumRecords,
				MaxDocID:             fi.MaxDocID,
				InvertedMB:           fi.InvertedSzMB,
				DocTableMB:           fi.DocTableSzMB,
				TotalIndexMemMB:      fi.TotalIndexMemMB,
				HashIndexingFailures: fi.HashIndexingFailures,
				Indexing:             fi.Indexing,
				PercentIndexed:       fi.PercentIndexed,
				Cleaning:             fi.Cleaning,
				DiskMode:             si.DiskMode,
				DiskUsage:            si.DiskUsage,
				UsedMem:              si.UsedMemory,
				AsyncReadsExpired:    si.AsyncReadsExpired,
				CompactionCycles:     si.CompactionCycles,
				PendingCompaction:    si.PendingCompactionBytes,
			})
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
