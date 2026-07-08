// Package web serves a live dashboard for the running stress test: high-level
// status, a browsable view of the generated conversations (backed by real
// FT.SEARCH + HGETALL against the disk index), index-status metrics, a live
// query-control panel (rate / timeout / profile mix), and an ad-hoc query editor
// that generates, edits and sends queries to the cluster. Assets are embedded and
// charts are drawn in vanilla JS — no external dependencies.
package web

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"io/fs"
	"math/rand"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
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

//go:embed static
var staticFS embed.FS

// Server is the dashboard HTTP server.
type Server struct {
	cfg  *config.Config
	cli  *redisx.Client
	met  *metrics.Metrics
	orc  *oracle.Oracle
	ctrl *control.Control

	rmu     sync.Mutex
	rIngest float64
	rQuery  float64
	rDelete float64
	prevIng int64
	prevQ   int64
	prevDel int64
	prevT   time.Time

	qmu  sync.Mutex // guards the (non-thread-safe) query generators below
	rnd  *rand.Rand
	pick *model.Picker
	gen  *gendata.Generator
}

// New builds a Server over shared state.
func New(cfg *config.Config, cli *redisx.Client, met *metrics.Metrics, orc *oracle.Oracle, ctrl *control.Control) *Server {
	space := model.Space{
		Tenants:           cfg.Tenants,
		ChannelsPerTenant: cfg.ChannelsPerTenant,
		UsersPerTenant:    cfg.UsersPerTenant,
		ThreadsPerChannel: cfg.ThreadsPerChannel,
	}
	return &Server{
		cfg: cfg, cli: cli, met: met, orc: orc, ctrl: ctrl, prevT: time.Now(),
		rnd:  rand.New(rand.NewSource(cfg.Seed*7919 + 1)),
		pick: model.NewPicker(space, cfg.Seed, 424242),
		gen:  gendata.New(cfg.Seed, 999999, cfg.Body.MinWords, cfg.Body.MaxWords, 0),
	}
}

var tagValRe = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// searchProfiles are the query profiles the ad-hoc generator emits as editable
// FT.SEARCH query bodies (aggregate profiles aren't single editable strings).
var searchProfiles = []string{"channel_search", "thread_search", "tag_filter", "text_prefix", "deep_pagination"}

// Start runs the HTTP server until ctx is cancelled.
func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/stats", s.handleStats)
	mux.HandleFunc("/api/channels", s.handleChannels)
	mux.HandleFunc("/api/messages", s.handleMessages)
	mux.HandleFunc("/api/search", s.handleSearch)
	mux.HandleFunc("/api/config", s.handleConfig)
	mux.HandleFunc("/api/control", s.handleControl)
	mux.HandleFunc("/api/gen-queries", s.handleGenQueries)
	mux.HandleFunc("/api/run-queries", s.handleRunQueries)
	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		return err
	}
	mux.Handle("/", http.FileServer(http.FS(sub)))

	srv := &http.Server{Addr: s.cfg.Web.Addr, Handler: mux}
	go s.rateLoop(ctx)
	go func() {
		<-ctx.Done()
		sh, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(sh)
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) rateLoop(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			now := time.Now()
			dt := now.Sub(s.prevT).Seconds()
			if dt <= 0 {
				dt = 1
			}
			ing := s.met.C.Ingested.Load()
			q := s.met.C.Queries.Load()
			del := s.met.C.Deleted.Load()
			s.rmu.Lock()
			s.rIngest = float64(ing-s.prevIng) / dt
			s.rQuery = float64(q-s.prevQ) / dt
			s.rDelete = float64(del-s.prevDel) / dt
			s.rmu.Unlock()
			s.prevIng, s.prevQ, s.prevDel, s.prevT = ing, q, del, now
		}
	}
}

// --- /api/stats ---

type statsResp struct {
	ElapsedSec  float64          `json:"elapsed_sec"`
	DiskMode    bool             `json:"disk_mode"`
	Rates       ratesResp        `json:"rates"`
	Latency     latencyResp      `json:"latency"`
	Correctness correctnessResp  `json:"correctness"`
	Index       metrics.Status   `json:"index"`
	Footprint   footprintResp    `json:"footprint"`
	Series      []metrics.Sample `json:"series"`
	Errors      errsResp         `json:"errors"`
}

type ratesResp struct {
	Ingest float64 `json:"ingest"`
	Query  float64 `json:"query"`
	Expire float64 `json:"expire"` // estimated messages expiring/sec
	Delete float64 `json:"delete"`
}

type latencyResp struct {
	P50  float64 `json:"p50"`
	P99  float64 `json:"p99"`
	Slow int64   `json:"slow"` // queries at/over the slow (timeout-risk) threshold
}

type correctnessResp struct {
	StaleHits    int64        `json:"stale_hits"`
	Oracle       oracle.Stats `json:"oracle"`
	RecallPct    float64      `json:"recall_pct"`
	SlowThreshMs int          `json:"slow_thresh_ms"`
}

type footprintResp struct {
	Metric  string  `json:"metric"`
	Bytes   int64   `json:"bytes"`
	Plateau bool    `json:"plateau"`
	Note    string  `json:"note"`
	Samples int     `json:"samples"`
	Slope   float64 `json:"slope_bytes_sec"`
}

type errsResp struct {
	Query  int64 `json:"query"`
	Ingest int64 `json:"ingest"`
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	series := s.met.Series()
	status := s.met.Status()
	s.rmu.Lock()
	ri, rq, rd := s.rIngest, s.rQuery, s.rDelete
	s.rmu.Unlock()

	// Estimate expiry rate: at steady state, expire ≈ ingest − net-doc-growth − deletes.
	docGrowth := 0.0
	if n := len(series); n >= 2 {
		a, b := series[n-2], series[n-1]
		if dt := b.TSec - a.TSec; dt > 0 {
			docGrowth = float64(b.NumDocs-a.NumDocs) / dt
		}
	}
	expire := ri - docGrowth - rd
	if expire < 0 {
		expire = 0
	}

	v := s.met.Plateau()
	footBytes := status.UsedMem
	if status.DiskMode {
		footBytes = status.DiskUsage
	}
	ost := s.orc.Stats()
	recall := 0.0
	if tot := ost.RecallOK + ost.RecallMiss; tot > 0 {
		recall = 100 * float64(ost.RecallOK) / float64(tot)
	}

	resp := statsResp{
		ElapsedSec: s.met.Elapsed().Seconds(),
		DiskMode:   status.DiskMode,
		Rates:      ratesResp{Ingest: ri, Query: rq, Expire: expire, Delete: rd},
		Latency: latencyResp{
			P50:  s.met.Lat.Percentile(0.50),
			P99:  s.met.Lat.Percentile(0.99),
			Slow: s.met.C.SlowQueries.Load(),
		},
		Correctness: correctnessResp{
			StaleHits:    s.orc.StaleHits(),
			Oracle:       ost,
			RecallPct:    recall,
			SlowThreshMs: s.cfg.Query.SlowMs,
		},
		Index: status,
		Footprint: footprintResp{
			Metric: v.Metric, Bytes: footBytes, Plateau: v.Plateau,
			Note: v.Note, Samples: v.Samples, Slope: v.SlopeBytesSec,
		},
		Series: series,
		Errors: errsResp{Query: s.met.C.QueryErrors.Load(), Ingest: s.met.C.IngestErrors.Load()},
	}
	writeJSON(w, resp)
}

// --- /api/channels ---

type channelCount struct {
	Channel string `json:"channel"`
	Count   int    `json:"count"`
}

func (s *Server) handleChannels(w http.ResponseWriter, r *http.Request) {
	counts := s.orc.TopChannels(50_000)
	out := make([]channelCount, 0, len(counts))
	for c, n := range counts {
		out = append(out, channelCount{Channel: c, Count: n})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Count > out[j].Count })
	if len(out) > 40 {
		out = out[:40]
	}
	writeJSON(w, out)
}

// --- /api/messages + /api/search ---

type msgView struct {
	Key     string `json:"key"`
	Channel string `json:"channel"`
	User    string `json:"user"`
	Thread  string `json:"thread"`
	Plan    string `json:"plan"`
	Body    string `json:"body"`
	Seq     int64  `json:"seq"`
	TTLms   int64  `json:"ttl_ms"`
}

type searchResp struct {
	Query    string    `json:"query"`
	Total    int64     `json:"total"`
	Returned int       `json:"returned"`
	Messages []msgView `json:"messages"`
	Error    string    `json:"error,omitempty"`
}

func (s *Server) handleMessages(w http.ResponseWriter, r *http.Request) {
	channel := r.URL.Query().Get("channel")
	q := r.URL.Query().Get("q")
	limit := clampLimit(r.URL.Query().Get("limit"), 30)
	s.runContentSearch(w, r, channel, "", "", q, limit)
}

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	qp := r.URL.Query()
	limit := clampLimit(qp.Get("limit"), 30)
	s.runContentSearch(w, r, qp.Get("channel"), qp.Get("user"), qp.Get("thread"), qp.Get("q"), limit)
}

func (s *Server) runContentSearch(w http.ResponseWriter, r *http.Request, channel, user, thread, q string, limit int) {
	query, err := buildQuery(channel, user, thread, q)
	resp := searchResp{Query: query}
	if err != nil {
		resp.Error = err.Error()
		writeJSON(w, resp)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	total, keys, err := s.cli.Search(ctx, query, limit, s.ctrl.TimeoutMs())
	if err != nil {
		resp.Error = err.Error()
		writeJSON(w, resp)
		return
	}
	resp.Total = total
	for _, k := range keys {
		fields, ttl, derr := s.cli.Doc(ctx, k)
		if derr != nil || len(fields) == 0 {
			continue // raced with expiry/deletion — skip
		}
		mv := msgView{
			Key: k, Channel: fields[model.FieldChannel], User: fields[model.FieldUser],
			Thread: fields[model.FieldThread], Plan: fields[model.FieldPlan],
			Body: fields[model.FieldBody], TTLms: int64(ttl / time.Millisecond),
		}
		if vv, e := strconv.ParseInt(fields[model.FieldSeq], 10, 64); e == nil {
			mv.Seq = vv
		}
		resp.Messages = append(resp.Messages, mv)
	}
	sort.Slice(resp.Messages, func(i, j int) bool { return resp.Messages[i].Seq > resp.Messages[j].Seq })
	resp.Returned = len(resp.Messages)
	writeJSON(w, resp)
}

// --- /api/config ---

type tierResp struct {
	Name   string `json:"name"`
	TTL    string `json:"ttl"`
	Weight int    `json:"weight"`
}

type configResp struct {
	Index     string     `json:"index"`
	Addr      string     `json:"addr"`
	Tenants   int        `json:"tenants"`
	Channels  int        `json:"channels"`
	Users     int        `json:"users"`
	Threads   int        `json:"threads"`
	Tiers     []tierResp `json:"tiers"`
	VocabHigh int        `json:"vocab_high"`
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	c := s.cfg
	resp := configResp{
		Index: c.Index, Addr: c.Addr, Tenants: c.Tenants,
		Channels:  c.Tenants * c.ChannelsPerTenant,
		Users:     c.Tenants * c.UsersPerTenant,
		Threads:   c.Tenants * c.ChannelsPerTenant * c.ThreadsPerChannel,
		VocabHigh: c.Body.VocabHigh,
	}
	for _, t := range c.Tiers {
		resp.Tiers = append(resp.Tiers, tierResp{Name: t.Name, TTL: t.TTL.D().String(), Weight: t.Weight})
	}
	writeJSON(w, resp)
}

// --- /api/control (GET current settings + profile mix, POST to update) ---

type profileView struct {
	Name   string `json:"name"`
	Weight int    `json:"weight"`
	Desc   string `json:"desc"`
	Count  int64  `json:"count"`
}

type controlResp struct {
	Rate      int           `json:"rate"`
	TimeoutMs int           `json:"timeout_ms"`
	Limit     int           `json:"limit"`
	PageDepth int           `json:"page_depth"`
	Profiles  []profileView `json:"profiles"`
}

type controlReq struct {
	Rate      *int `json:"rate"`
	TimeoutMs *int `json:"timeout_ms"`
	Limit     *int `json:"limit"`
	Profiles  []struct {
		Name   string `json:"name"`
		Weight int    `json:"weight"`
	} `json:"profiles"`
}

func (s *Server) handleControl(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		var req controlReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.Rate != nil {
			s.ctrl.SetQueryRate(*req.Rate)
		}
		if req.TimeoutMs != nil {
			s.ctrl.SetTimeoutMs(*req.TimeoutMs)
		}
		if req.Limit != nil {
			s.ctrl.SetLimit(*req.Limit)
		}
		if len(req.Profiles) > 0 {
			ps := make([]config.QueryProfile, 0, len(req.Profiles))
			for _, p := range req.Profiles {
				ps = append(ps, config.QueryProfile{Name: p.Name, Weight: p.Weight})
			}
			s.ctrl.SetProfiles(ps)
		}
	}
	// GET or after POST: return current state.
	counts := s.met.ProfileCounts()
	profs := s.ctrl.Profiles()
	// include known profiles even if weight 0, so the UI can show/enable them
	seen := map[string]bool{}
	out := make([]profileView, 0, len(config.KnownProfiles))
	for _, p := range profs {
		out = append(out, profileView{Name: p.Name, Weight: p.Weight, Desc: config.KnownProfiles[p.Name], Count: counts[p.Name]})
		seen[p.Name] = true
	}
	for name, desc := range config.KnownProfiles {
		if !seen[name] {
			out = append(out, profileView{Name: name, Weight: 0, Desc: desc, Count: counts[name]})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Weight != out[j].Weight {
			return out[i].Weight > out[j].Weight
		}
		return out[i].Name < out[j].Name
	})
	writeJSON(w, controlResp{
		Rate: s.ctrl.QueryRate(), TimeoutMs: s.ctrl.TimeoutMs(), Limit: s.ctrl.Limit(),
		PageDepth: s.cfg.Query.PageDepth, Profiles: out,
	})
}

// --- /api/gen-queries (build N editable FT.SEARCH query bodies) ---

type genReq struct {
	Count    int      `json:"count"`
	Profiles []string `json:"profiles"`
}

type genQuery struct {
	Profile string `json:"profile"`
	Query   string `json:"query"`
}

func (s *Server) handleGenQueries(w http.ResponseWriter, r *http.Request) {
	var req genReq
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.Count <= 0 {
		req.Count = 8
	}
	if req.Count > 100 {
		req.Count = 100
	}
	pool := filterSearchProfiles(req.Profiles)
	if len(pool) == 0 {
		pool = searchProfiles
	}

	s.qmu.Lock()
	defer s.qmu.Unlock()
	out := make([]genQuery, 0, req.Count)
	for i := 0; i < req.Count; i++ {
		prof := pool[s.rnd.Intn(len(pool))]
		out = append(out, genQuery{Profile: prof, Query: s.buildProfileBody(prof)})
	}
	writeJSON(w, out)
}

// buildProfileBody produces one FT.SEARCH query body for a profile. Caller holds qmu.
func (s *Server) buildProfileBody(prof string) string {
	live, hasLive := s.orc.LiveSample()
	channel := func() string {
		if hasLive {
			return live.Channel
		}
		tid, _ := s.pick.Tenant()
		_, ch := s.pick.ChannelID(tid)
		return ch
	}
	token := func() string {
		if hasLive {
			return live.Token
		}
		return s.gen.Word()
	}
	switch prof {
	case "thread_search":
		tid, _ := s.pick.Tenant()
		cid, _ := s.pick.ChannelID(tid)
		return "@" + model.FieldThread + ":{" + s.pick.Thread(cid) + "} " + s.gen.Word()
	case "tag_filter":
		if len(s.cfg.Tiers) > 0 && s.rnd.Intn(2) == 0 {
			tier := s.cfg.Tiers[s.rnd.Intn(len(s.cfg.Tiers))]
			return "@" + model.FieldPlan + ":{" + tier.Name + "} " + s.gen.Word()
		}
		_, tenant := s.pick.Tenant()
		return "@" + model.FieldTenant + ":{" + tenant + "} " + s.gen.Word()
	case "text_prefix":
		return "@" + model.FieldChannel + ":{" + channel() + "} " + prefixOf(token())
	default: // channel_search, deep_pagination
		return "@" + model.FieldChannel + ":{" + channel() + "} " + token()
	}
}

// --- /api/run-queries (execute a batch of edited FT.SEARCH query bodies) ---

type runReq struct {
	Queries   []string `json:"queries"`
	TimeoutMs *int     `json:"timeout_ms"`
	Limit     *int     `json:"limit"`
}

type runResult struct {
	Query    string   `json:"query"`
	Total    int64    `json:"total"`
	Returned int      `json:"returned"`
	Ms       float64  `json:"ms"`
	Keys     []string `json:"keys"`
	Error    string   `json:"error,omitempty"`
}

func (s *Server) handleRunQueries(w http.ResponseWriter, r *http.Request) {
	var req runReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(req.Queries) > 100 {
		req.Queries = req.Queries[:100]
	}
	limit := s.ctrl.Limit()
	if req.Limit != nil && *req.Limit > 0 {
		limit = *req.Limit
	}
	timeout := s.ctrl.TimeoutMs()
	if req.TimeoutMs != nil {
		timeout = *req.TimeoutMs
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	out := make([]runResult, 0, len(req.Queries))
	for _, q := range req.Queries {
		q = strings.TrimSpace(q)
		if q == "" {
			continue
		}
		start := time.Now()
		total, keys, err := s.cli.Search(ctx, q, limit, timeout)
		res := runResult{Query: q, Ms: float64(time.Since(start).Microseconds()) / 1000.0}
		if err != nil {
			res.Error = err.Error()
		} else {
			res.Total = total
			res.Returned = len(keys)
			if len(keys) > 3 {
				keys = keys[:3]
			}
			res.Keys = keys
		}
		out = append(out, res)
	}
	writeJSON(w, out)
}

// --- helpers ---

func filterSearchProfiles(names []string) []string {
	if len(names) == 0 {
		return nil
	}
	ok := map[string]bool{}
	for _, p := range searchProfiles {
		ok[p] = true
	}
	var out []string
	for _, n := range names {
		if ok[n] {
			out = append(out, n)
		}
	}
	return out
}

func prefixOf(w string) string {
	rs := []rune(w)
	if len(rs) <= 3 {
		return w + "*"
	}
	return string(rs[:len(rs)-1]) + "*"
}

// buildQuery assembles a disk-legal query for the browse/search UI: TAG scopes are
// ANDed as @f:{v}; the free-text term is appended as a TEXT match. TAG values must
// be alphanumeric.
func buildQuery(channel, user, thread, q string) (string, error) {
	var parts []string
	add := func(field, val string) error {
		if val == "" {
			return nil
		}
		if !tagValRe.MatchString(val) {
			return errors.New("tag value must be alphanumeric: " + val)
		}
		parts = append(parts, "@"+field+":{"+val+"}")
		return nil
	}
	if err := add(model.FieldChannel, channel); err != nil {
		return "", err
	}
	if err := add(model.FieldUser, user); err != nil {
		return "", err
	}
	if err := add(model.FieldThread, thread); err != nil {
		return "", err
	}
	if q = strings.TrimSpace(q); q != "" {
		parts = append(parts, sanitizeText(q))
	}
	if len(parts) == 0 {
		return "*", nil
	}
	return strings.Join(parts, " "), nil
}

func sanitizeText(q string) string {
	repl := strings.NewReplacer(
		"@", " ", "{", " ", "}", " ", "|", " ", "(", " ", ")", " ",
		"\"", " ", "'", " ", "~", " ", ":", " ", "=", " ",
		"[", " ", "]", " ", ">", " ", "<", " ",
	)
	return strings.TrimSpace(repl.Replace(q))
}

func clampLimit(s string, def int) int {
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return def
	}
	if n > 100 {
		return 100
	}
	return n
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
