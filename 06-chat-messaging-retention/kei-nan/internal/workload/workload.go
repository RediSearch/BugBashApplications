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
	"chatstress/internal/redisx"
)

// Runner owns the worker pool and shared state.
type Runner struct {
	cfg   *config.Config
	cli   *redisx.Client
	orc   *oracle.Oracle
	met   *metrics.Metrics
	ctrl  *control.Control
	space model.Space
	seq   seqCounter
}

// New builds a Runner over shared metrics/oracle/client/control.
func New(cfg *config.Config, cli *redisx.Client, orc *oracle.Oracle, met *metrics.Metrics, ctrl *control.Control) *Runner {
	return &Runner{
		cfg: cfg, cli: cli, orc: orc, met: met, ctrl: ctrl,
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
	pick := model.NewPicker(r.space, r.cfg.Seed, int64(id)*29+7)
	gen := gendata.New(r.cfg.Seed, int64(id)*71+11, r.cfg.Body.MinWords, r.cfg.Body.MaxWords, 0)
	slowMs := r.cfg.Query.SlowMs
	iter := 0
	for {
		if ctx.Err() != nil {
			return
		}
		r.paceQuery(ctx)
		iter++

		prof := r.ctrl.PickProfile(rnd.Int())
		if prof == "" {
			continue
		}
		n := r.ctrl.Limit()
		to := r.ctrl.TimeoutMs()
		start := time.Now()
		err := r.runProfile(ctx, prof, rnd, pick, gen, n, to)
		r.met.RecordQuery(prof, time.Since(start), slowMs)
		if err != nil && ctx.Err() == nil {
			r.met.C.QueryErrors.Add(1)
		}

		// Every few queries, probe a comfortably-expired doc: it must be absent.
		if iter%5 == 0 {
			if es, ok := r.orc.ExpiredSample(); ok {
				pq := "@" + model.FieldChannel + ":{" + es.Channel + "} " + es.Token
				pt0 := time.Now().UnixMilli()
				_, pkeys, perr := r.cli.Search(ctx, pq, n, to)
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

// runProfile executes one query of the given profile. Profiles that return
// document keys feed the no-stale-hits oracle; aggregate profiles are executed
// for their latency/timeout/OOM behavior. Each targets a disk pattern/weak-point.
func (r *Runner) runProfile(ctx context.Context, prof string, rnd *rand.Rand, pick *model.Picker, gen *gendata.Generator, n, to int) error {
	checkKeys := func(keys []string, t0 int64) {
		for _, k := range keys {
			r.orc.Check(k, t0)
		}
	}

	switch prof {
	case "channel_search": // scoped text in a channel; recall-checked
		s, ok := r.orc.LiveSample()
		if !ok {
			return nil
		}
		t0 := time.Now().UnixMilli()
		_, keys, err := r.cli.Search(ctx, "@"+model.FieldChannel+":{"+s.Channel+"} "+s.Token, n, to)
		if err != nil {
			return err
		}
		found := false
		for _, k := range keys {
			if k == s.Key {
				found = true
			}
			r.orc.Check(k, t0)
		}
		r.orc.RecordRecall(found)
		return nil

	case "thread_search": // high-cardinality thread posting list
		tid, _ := pick.Tenant()
		cid, _ := pick.ChannelID(tid)
		thread := pick.Thread(cid)
		t0 := time.Now().UnixMilli()
		_, keys, err := r.cli.Search(ctx, "@"+model.FieldThread+":{"+thread+"} "+gen.Word(), n, to)
		if err != nil {
			return err
		}
		checkKeys(keys, t0)
		return nil

	case "tag_filter": // retention-tier / tenant TAG filter + term
		var scope string
		if rnd.Intn(2) == 0 {
			scope = "@" + model.FieldPlan + ":{" + r.pickTier(rnd).Name + "}"
		} else {
			_, tenant := pick.Tenant()
			scope = "@" + model.FieldTenant + ":{" + tenant + "}"
		}
		t0 := time.Now().UnixMilli()
		_, keys, err := r.cli.Search(ctx, scope+" "+gen.Word(), n, to)
		if err != nil {
			return err
		}
		checkKeys(keys, t0)
		return nil

	case "recent_timeline": // FT.AGGREGATE: LOAD unindexed @ts + APPLY to_number + SORTBY
		ch := r.channelFor(pick)
		_, err := r.cli.Aggregate(ctx, to, "@"+model.FieldChannel+":{"+ch+"}",
			"LOAD", 2, "@"+model.FieldTs, "@"+model.FieldUser,
			"APPLY", "to_number(@"+model.FieldTs+")", "AS", "ts_num",
			"SORTBY", 2, "@ts_num", "DESC",
			"LIMIT", 0, n)
		return err

	case "plan_analytics": // wide GROUPBY on a high-cardinality TAG (OOM weak-point)
		_, err := r.cli.Aggregate(ctx, to, "*",
			"GROUPBY", 1, "@"+model.FieldChannel,
			"REDUCE", "COUNT", 0, "AS", "n",
			"SORTBY", 2, "@n", "DESC",
			"LIMIT", 0, 10)
		return err

	case "deep_pagination": // large LIMIT offset (large-offset / OOM weak-point)
		s, ok := r.orc.LiveSample()
		if !ok {
			return nil
		}
		offset := 0
		if r.cfg.Query.PageDepth > 0 {
			offset = rnd.Intn(r.cfg.Query.PageDepth)
		}
		t0 := time.Now().UnixMilli()
		_, keys, err := r.cli.SearchLimit(ctx, "@"+model.FieldChannel+":{"+s.Channel+"} "+s.Token, offset, n, to)
		if err != nil {
			return err
		}
		checkKeys(keys, t0)
		return nil

	case "text_prefix": // TEXT prefix expansion (foo*)
		s, ok := r.orc.LiveSample()
		if !ok {
			return nil
		}
		t0 := time.Now().UnixMilli()
		_, keys, err := r.cli.Search(ctx, "@"+model.FieldChannel+":{"+s.Channel+"} "+prefixOf(s.Token), n, to)
		if err != nil {
			return err
		}
		checkKeys(keys, t0)
		return nil
	}
	return nil
}

// channelFor returns a channel that likely has live data (from the oracle),
// falling back to a random channel from the id-space.
func (r *Runner) channelFor(pick *model.Picker) string {
	if s, ok := r.orc.LiveSample(); ok {
		return s.Channel
	}
	tid, _ := pick.Tenant()
	_, ch := pick.ChannelID(tid)
	return ch
}

// prefixOf turns a term into a prefix query (e.g. "deploy" -> "deplo*"). It is
// rune-aware so unicode vocabulary words don't produce invalid UTF-8 prefixes.
func prefixOf(w string) string {
	rs := []rune(w)
	if len(rs) <= 3 {
		return w + "*"
	}
	return string(rs[:len(rs)-1]) + "*"
}

func (r *Runner) editor(ctx context.Context, lim *Limiter) {
	gen := gendata.New(r.cfg.Seed, 999, r.cfg.Body.MinWords, r.cfg.Body.MaxWords, r.cfg.Body.VocabHigh)
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
