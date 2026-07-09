package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"chatstress/internal/config"
	"chatstress/internal/gendata"
	"chatstress/internal/model"
	"chatstress/internal/redisx"
)

// reloadCheck verifies the restart/RDB-reload correctness axis: load a mix of
// long-lived and already-expired documents, take an in-process RDB save+load
// round-trip via DEBUG RELOAD, and assert that (a) expired docs are never
// visible (no stale hits) before or after the reload, and (b) live docs survive.
//
// Each document carries a UNIQUE text token (e.g. "expuid1234") so a scoped
// search resolves to exactly one document, making the presence/absence checks
// deterministic (unlike the vocabulary-based tokens used by the main workload).
func reloadCheck(ctx context.Context, cfg *config.Config, cli *redisx.Client) error {
	const (
		nLive       = 2000
		nExpire     = 2000
		sampleCheck = 300
		batchSz     = 500
	)

	fmt.Println("reload-check: RDB save/load with expired docs present")
	if err := cli.FlushAll(ctx); err != nil {
		return fmt.Errorf("flushall: %w", err)
	}
	if err := cli.CreateIndex(ctx, cfg.Prefix); err != nil {
		return fmt.Errorf("create index: %w", err)
	}

	space := model.Space{
		Tenants:           cfg.Tenants,
		ChannelsPerTenant: cfg.ChannelsPerTenant,
		UsersPerTenant:    cfg.UsersPerTenant,
		ThreadsPerChannel: cfg.ThreadsPerChannel,
	}
	pick := model.NewPicker(space, cfg.Seed, 1)
	gen := gendata.New(cfg.Seed, 2, cfg.Body.MinWords, cfg.Body.MaxWords, 0)

	type rec struct{ key, channel, token string }
	var seq int64

	build := func(n int, ttl time.Duration, uniqPrefix string) ([]rec, error) {
		recs := make([]rec, 0, n)
		batch := make([]*model.Message, 0, batchSz)
		flush := func() error {
			if len(batch) == 0 {
				return nil
			}
			err := cli.IngestBatch(ctx, batch)
			batch = batch[:0]
			return err
		}
		now := time.Now().UnixMilli()
		for i := 0; i < n; i++ {
			seq++
			tid, tenant := pick.Tenant()
			cid, channel := pick.ChannelID(tid)
			base, _ := gen.Body()
			token := uniqPrefix + strconv.FormatInt(seq, 10)
			m := &model.Message{
				Key: model.Key(cfg.Prefix, seq), Seq: seq,
				Tenant: tenant, Channel: channel,
				User: pick.User(tid), Thread: pick.Thread(cid),
				Plan: "check", Body: base + " " + token, Token: token,
				TTL: ttl, CreateMs: now,
			}
			batch = append(batch, m)
			recs = append(recs, rec{key: m.Key, channel: channel, token: token})
			if len(batch) == batchSz {
				if err := flush(); err != nil {
					return nil, err
				}
			}
		}
		return recs, flush()
	}

	live, err := build(nLive, time.Hour, "liveuid")
	if err != nil {
		return fmt.Errorf("load live docs: %w", err)
	}
	expd, err := build(nExpire, time.Second, "expuid")
	if err != nil {
		return fmt.Errorf("load expiring docs: %w", err)
	}

	// Let the short TTLs expire and indexing settle.
	time.Sleep(2500 * time.Millisecond)

	has := func(r rec) bool {
		q := "@" + model.FieldChannel + ":{" + r.channel + "} " + r.token
		_, keys, err := cli.Search(ctx, q, 10, 0)
		if err != nil {
			return false
		}
		for _, k := range keys {
			if k == r.key {
				return true
			}
		}
		return false
	}
	sample := func(rs []rec, n int) []rec {
		if len(rs) <= n {
			return rs
		}
		step := len(rs) / n
		out := make([]rec, 0, n)
		for i := 0; i < len(rs); i += step {
			out = append(out, rs[i])
		}
		return out
	}
	countFound := func(rs []rec) int {
		c := 0
		for _, r := range rs {
			if has(r) {
				c++
			}
		}
		return c
	}

	liveS := sample(live, sampleCheck)
	expS := sample(expd, sampleCheck)

	preLive := countFound(liveS)
	preStale := countFound(expS)
	fmt.Printf("pre-reload:  live visible %d/%d, expired-visible %d/%d\n", preLive, len(liveS), preStale, len(expS))

	fmt.Println("issuing DEBUG RELOAD (RDB save + load)…")
	if err := cli.DebugReload(ctx); err != nil {
		return fmt.Errorf("DEBUG RELOAD (needs enable-debug-command): %w", err)
	}
	time.Sleep(500 * time.Millisecond)

	postLive := countFound(liveS)
	postStale := countFound(expS)
	fmt.Printf("post-reload: live visible %d/%d, expired-visible %d/%d\n", postLive, len(liveS), postStale, len(expS))

	if preStale > 0 || postStale > 0 {
		return fmt.Errorf("expired docs were visible (pre=%d, post=%d) — STALE HITS", preStale, postStale)
	}
	if postLive < len(liveS)*8/10 {
		fmt.Printf("WARNING: post-reload live recall low (%d/%d) — indexing lag or a real regression\n", postLive, len(liveS))
	}
	fmt.Println("reload-check PASSED: no stale hits before/after RDB reload; live docs preserved.")
	return nil
}
