// Package web serves a live dashboard for the running stress test: a browsable
// view of the generated conversations (backed by real FT.SEARCH + HGETALL against
// the disk index), an interactive scoped-search box, and DB-stats charts fed from
// the in-process metrics/oracle. It has no external dependencies — assets are
// embedded and charts are drawn in vanilla JS.
package web

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"chatstress/internal/config"
	"chatstress/internal/metrics"
	"chatstress/internal/model"
	"chatstress/internal/oracle"
	"chatstress/internal/redisx"
)

//go:embed static
var staticFS embed.FS

// Server is the dashboard HTTP server.
type Server struct {
	cfg *config.Config
	cli *redisx.Client
	met *metrics.Metrics
	orc *oracle.Oracle

	rmu     sync.Mutex
	rIngest float64
	rQuery  float64
	prevIng int64
	prevQ   int64
	prevT   time.Time
}

// New builds a Server over shared state.
func New(cfg *config.Config, cli *redisx.Client, met *metrics.Metrics, orc *oracle.Oracle) *Server {
	return &Server{cfg: cfg, cli: cli, met: met, orc: orc, prevT: time.Now()}
}

var tagValRe = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// Start runs the HTTP server until ctx is cancelled.
func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/stats", s.handleStats)
	mux.HandleFunc("/api/channels", s.handleChannels)
	mux.HandleFunc("/api/messages", s.handleMessages)
	mux.HandleFunc("/api/search", s.handleSearch)
	mux.HandleFunc("/api/config", s.handleConfig)
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
			s.rmu.Lock()
			s.rIngest = float64(ing-s.prevIng) / dt
			s.rQuery = float64(q-s.prevQ) / dt
			s.rmu.Unlock()
			s.prevIng, s.prevQ, s.prevT = ing, q, now
		}
	}
}

// --- API responses ---

type statsResp struct {
	ElapsedSec float64          `json:"elapsed_sec"`
	DiskMode   bool             `json:"disk_mode"`
	Counters   countersResp     `json:"counters"`
	Rates      ratesResp        `json:"rates"`
	Latency    latencyResp      `json:"latency"`
	Oracle     oracle.Stats     `json:"oracle"`
	StaleHits  int64            `json:"stale_hits"`
	Latest     metrics.Sample   `json:"latest"`
	Series     []metrics.Sample `json:"series"`
	Verdict    metrics.Verdict  `json:"verdict"`
}

type countersResp struct {
	Ingested     int64 `json:"ingested"`
	Queries      int64 `json:"queries"`
	Edited       int64 `json:"edited"`
	Deleted      int64 `json:"deleted"`
	Refreshed    int64 `json:"refreshed"`
	IngestErrors int64 `json:"ingest_errors"`
	QueryErrors  int64 `json:"query_errors"`
}

type ratesResp struct {
	Ingest float64 `json:"ingest"`
	Query  float64 `json:"query"`
}

type latencyResp struct {
	P50 float64 `json:"p50"`
	P90 float64 `json:"p90"`
	P99 float64 `json:"p99"`
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	series := s.met.Series()
	var latest metrics.Sample
	var diskMode bool
	if n := len(series); n > 0 {
		latest = series[n-1]
		diskMode = latest.DiskUsage > 0
	}
	s.rmu.Lock()
	ri, rq := s.rIngest, s.rQuery
	s.rmu.Unlock()

	resp := statsResp{
		ElapsedSec: s.met.Elapsed().Seconds(),
		DiskMode:   diskMode,
		Counters: countersResp{
			Ingested:     s.met.C.Ingested.Load(),
			Queries:      s.met.C.Queries.Load(),
			Edited:       s.met.C.Edited.Load(),
			Deleted:      s.met.C.Deleted.Load(),
			Refreshed:    s.met.C.Refreshed.Load(),
			IngestErrors: s.met.C.IngestErrors.Load(),
			QueryErrors:  s.met.C.QueryErrors.Load(),
		},
		Rates:     ratesResp{Ingest: ri, Query: rq},
		Latency:   latencyResp{P50: s.met.Lat.Percentile(0.50), P90: s.met.Lat.Percentile(0.90), P99: s.met.Lat.Percentile(0.99)},
		Oracle:    s.orc.Stats(),
		StaleHits: s.orc.StaleHits(),
		Latest:    latest,
		Series:    series,
		Verdict:   s.met.Plateau(),
	}
	writeJSON(w, resp)
}

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
	s.runSearch(w, r, channel, "", "", q, limit)
}

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	qp := r.URL.Query()
	limit := clampLimit(qp.Get("limit"), 30)
	s.runSearch(w, r, qp.Get("channel"), qp.Get("user"), qp.Get("thread"), qp.Get("q"), limit)
}

func (s *Server) runSearch(w http.ResponseWriter, r *http.Request, channel, user, thread, q string, limit int) {
	query, err := buildQuery(channel, user, thread, q)
	resp := searchResp{Query: query}
	if err != nil {
		resp.Error = err.Error()
		writeJSON(w, resp)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	total, keys, err := s.cli.Search(ctx, query, limit)
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
			Key:     k,
			Channel: fields[model.FieldChannel],
			User:    fields[model.FieldUser],
			Thread:  fields[model.FieldThread],
			Plan:    fields[model.FieldPlan],
			Body:    fields[model.FieldBody],
			TTLms:   int64(ttl / time.Millisecond),
		}
		if v, e := strconv.ParseInt(fields[model.FieldSeq], 10, 64); e == nil {
			mv.Seq = v
		}
		resp.Messages = append(resp.Messages, mv)
	}
	sort.Slice(resp.Messages, func(i, j int) bool { return resp.Messages[i].Seq > resp.Messages[j].Seq })
	resp.Returned = len(resp.Messages)
	writeJSON(w, resp)
}

type configResp struct {
	Index     string     `json:"index"`
	Addr      string     `json:"addr"`
	Tenants   int        `json:"tenants"`
	Channels  int        `json:"channels"`
	Users     int        `json:"users"`
	Threads   int        `json:"threads"`
	Tiers     []tierResp `json:"tiers"`
	IngestCfg [2]int     `json:"ingest_cfg"` // [workers, rate]
	QueryCfg  [2]int     `json:"query_cfg"`
}

type tierResp struct {
	Name   string `json:"name"`
	TTL    string `json:"ttl"`
	Weight int    `json:"weight"`
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	c := s.cfg
	resp := configResp{
		Index:     c.Index,
		Addr:      c.Addr,
		Tenants:   c.Tenants,
		Channels:  c.Tenants * c.ChannelsPerTenant,
		Users:     c.Tenants * c.UsersPerTenant,
		Threads:   c.Tenants * c.ChannelsPerTenant * c.ThreadsPerChannel,
		IngestCfg: [2]int{c.Ingest.Workers, c.Ingest.Rate},
		QueryCfg:  [2]int{c.Query.Workers, c.Query.Rate},
	}
	for _, t := range c.Tiers {
		resp.Tiers = append(resp.Tiers, tierResp{Name: t.Name, TTL: t.TTL.D().String(), Weight: t.Weight})
	}
	writeJSON(w, resp)
}

// --- helpers ---

// buildQuery assembles a disk-legal query: TAG scopes are ANDed as @f:{v}; the
// free-text term is appended as a TEXT match. TAG values must be alphanumeric
// (no prefix/wildcard on TAG is allowed on disk anyway).
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
	q = strings.TrimSpace(q)
	if q != "" {
		parts = append(parts, sanitizeText(q))
	}
	if len(parts) == 0 {
		return "*", nil
	}
	return strings.Join(parts, " "), nil
}

// sanitizeText strips RediSearch query metacharacters from user text so a stray
// character can't break query parsing (this is a trusted local UI, so we keep it
// simple rather than fully escaping).
func sanitizeText(q string) string {
	repl := strings.NewReplacer(
		"@", " ", "{", " ", "}", " ", "|", " ", "(", " ", ")", " ",
		"\"", " ", "'", " ", "~", " ", "*", " ", ":", " ", "=", " ",
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
