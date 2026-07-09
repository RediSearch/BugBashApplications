// Package redisx wraps the go-redis client with the exact command surface the
// harness needs against a disk-backed RediSearch index, honoring the documented
// disk constraints (SKIPINITIALSCAN, create-before-load, TEXT/TAG-only schema,
// whole-key TTL, TAG query form). All INFO/FT.INFO parsing is tolerant of missing
// fields so the same binary runs against an in-RAM OSS server (where the
// search_disk_* metrics simply don't exist) and a Flex/disk server.
package redisx

import (
	"context"
	"crypto/tls"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"chatstress/internal/model"
)

// Client is the harness's Redis handle.
type Client struct {
	rdb   *redis.Client
	index string
}

// Options configures the connection. A full URL (redis://|rediss://, typically
// from a cloud provider) takes precedence; otherwise Addr + Username/Password/TLS
// are used. All three support cloud endpoints (e.g. Redis Cloud / Enterprise Flex
// over TLS with ACL auth).
type Options struct {
	URL           string // full connection URL; overrides Addr/Username/Password/TLS
	Addr          string
	Username      string
	Password      string
	TLS           bool
	TLSSkipVerify bool
	Index         string
	PoolSize      int
}

// New dials the server. RESP2 is forced so FT.INFO / FT.SEARCH replies are plain
// arrays that are simple to parse.
func New(o Options) (*Client, error) {
	var ropts *redis.Options
	if o.URL != "" {
		p, err := redis.ParseURL(o.URL)
		if err != nil {
			// Deliberately do NOT include the URL in the error — it carries the
			// password.
			return nil, fmt.Errorf("parse redis url: %w", err)
		}
		ropts = p
		if o.TLSSkipVerify && ropts.TLSConfig != nil {
			ropts.TLSConfig.InsecureSkipVerify = true
		}
	} else {
		ropts = &redis.Options{Addr: o.Addr, Username: o.Username, Password: o.Password}
		if o.TLS {
			ropts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: o.TLSSkipVerify}
		}
	}
	ropts.Protocol = 2 // force RESP2 regardless of what the URL requested
	ropts.PoolSize = o.PoolSize
	rdb := redis.NewClient(ropts)
	c := &Client{rdb: rdb, index: o.Index}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("ping %s: %w", ropts.Addr, err) // ropts.Addr is host:port only
	}
	return c, nil
}

// AddrFromURL returns just the host:port of a connection URL, with no
// credentials, for display/logging. Returns "" if url is empty or unparseable.
func AddrFromURL(url string) string {
	if url == "" {
		return ""
	}
	p, err := redis.ParseURL(url)
	if err != nil {
		return ""
	}
	return p.Addr
}

// Close releases the connection pool.
func (c *Client) Close() error { return c.rdb.Close() }

// CreateIndex creates the disk-appropriate HASH index. SKIPINITIALSCAN is
// required for disk indexes, so this MUST be called before loading data.
func (c *Client) CreateIndex(ctx context.Context, prefix string) error {
	args := []any{
		"FT.CREATE", c.index, "ON", "HASH",
		"PREFIX", 1, prefix,
		"SKIPINITIALSCAN",
		"SCHEMA",
		model.FieldBody, "TEXT",
		model.FieldTenant, "TAG",
		model.FieldChannel, "TAG",
		model.FieldUser, "TAG",
		model.FieldThread, "TAG",
		model.FieldPlan, "TAG",
	}
	err := c.rdb.Do(ctx, args...).Err()
	if err != nil && strings.Contains(strings.ToLower(err.Error()), "already exists") {
		return nil
	}
	return err
}

// DropIndex drops the index (keeps the doc keys — disk FT.DROPINDEX rejects DD).
func (c *Client) DropIndex(ctx context.Context) error {
	err := c.rdb.Do(ctx, "FT.DROPINDEX", c.index).Err()
	if err != nil && strings.Contains(strings.ToLower(err.Error()), "unknown index") {
		return nil
	}
	return err
}

// IngestBatch pipelines HSET (+ whole-key PEXPIRE) for a batch of messages. A
// message with TTL <= 0 is written persistent (no PEXPIRE).
func (c *Client) IngestBatch(ctx context.Context, msgs []*model.Message) error {
	pipe := c.rdb.Pipeline()
	for _, m := range msgs {
		pipe.HSet(ctx, m.Key, m.HashArgs()...)
		if m.TTL > 0 {
			pipe.PExpire(ctx, m.Key, m.TTL)
		}
	}
	_, err := pipe.Exec(ctx)
	return err
}

// EditBody rewrites a message body (re-indexes the doc, keeping its TTL).
func (c *Client) EditBody(ctx context.Context, key, body string) error {
	return c.rdb.HSet(ctx, key, model.FieldBody, body).Err()
}

// Delete removes a message (de-indexes via UNLINK).
func (c *Client) Delete(ctx context.Context, key string) error {
	return c.rdb.Unlink(ctx, key).Err()
}

// Refresh applies a sliding TTL (whole-key PEXPIRE). Returns false if the key is
// already gone.
func (c *Client) Refresh(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return c.rdb.PExpire(ctx, key, ttl).Result()
}

// Search runs a count-and-keys query: FT.SEARCH <idx> <q> NOCONTENT LIMIT 0 n DIALECT 2.
// Returns the true total match count and up to n matching keys. timeoutMs > 0
// appends a per-query TIMEOUT.
func (c *Client) Search(ctx context.Context, query string, n, timeoutMs int) (total int64, keys []string, err error) {
	return c.SearchLimit(ctx, query, 0, n, timeoutMs)
}

// SearchLimit is Search with an explicit offset (for the deep-pagination profile).
func (c *Client) SearchLimit(ctx context.Context, query string, offset, n, timeoutMs int) (total int64, keys []string, err error) {
	args := []any{"FT.SEARCH", c.index, query, "NOCONTENT", "LIMIT", offset, n}
	if timeoutMs > 0 {
		args = append(args, "TIMEOUT", timeoutMs)
	}
	args = append(args, "DIALECT", 2)
	res, err := c.rdb.Do(ctx, args...).Slice()
	if err != nil {
		return 0, nil, err
	}
	if len(res) == 0 {
		return 0, nil, nil
	}
	total = toInt(res[0])
	keys = make([]string, 0, len(res)-1)
	for _, v := range res[1:] {
		if s, ok := v.(string); ok {
			keys = append(keys, s)
		}
	}
	return total, keys, nil
}

// RawCount executes an arbitrary command (args must start with the command name
// and index, e.g. "FT.SEARCH"/"FT.AGGREGATE") and returns the leading count plus
// any string elements of the reply (the matching keys, for NOCONTENT searches).
// Used by the query fuzzer.
func (c *Client) RawCount(ctx context.Context, args ...any) (total int64, keys []string, err error) {
	res, err := c.rdb.Do(ctx, args...).Slice()
	if err != nil {
		return 0, nil, err
	}
	if len(res) == 0 {
		return 0, nil, nil
	}
	total = toInt(res[0])
	for _, v := range res[1:] {
		if s, ok := v.(string); ok {
			keys = append(keys, s)
		}
	}
	return total, keys, nil
}

// Doc returns a message's fields and its remaining whole-key TTL.
func (c *Client) Doc(ctx context.Context, key string) (fields map[string]string, ttl time.Duration, err error) {
	pipe := c.rdb.Pipeline()
	hg := pipe.HGetAll(ctx, key)
	pt := pipe.PTTL(ctx, key)
	if _, err = pipe.Exec(ctx); err != nil {
		return nil, 0, err
	}
	return hg.Val(), pt.Val(), nil
}

// FtInfo returns the meaningful FT.INFO fields for a disk index. Fields that are
// placeholders on Flex (offset_vectors_sz_mb, key_table_size_mb, etc.) are
// deliberately omitted — see the MS2 Limitations doc.
type FtInfo struct {
	NumDocs                int64
	MaxDocID               int64
	NumRecords             int64
	InvertedSzMB           float64 // Speedb estimate
	DocTableSzMB           float64 // Speedb estimate
	TotalIndexMemMB        float64 // Speedb estimate
	TotalInvertedIdxBlocks int64
	HashIndexingFailures   int64
	Indexing               int64   // 1 while a background index op is running
	PercentIndexed         float64 // 0..1
	Cleaning               int64   // 1 while GC/cleaning is running
}

// Info reads FT.INFO and extracts the index-level fields the harness watches.
func (c *Client) Info(ctx context.Context) (FtInfo, error) {
	var fi FtInfo
	res, err := c.rdb.Do(ctx, "FT.INFO", c.index).Slice()
	if err != nil {
		return fi, err
	}
	for i := 0; i+1 < len(res); i += 2 {
		key, _ := res[i].(string)
		val := res[i+1]
		switch key {
		case "num_docs":
			fi.NumDocs = toInt(val)
		case "max_doc_id":
			fi.MaxDocID = toInt(val)
		case "num_records":
			fi.NumRecords = toInt(val)
		case "inverted_sz_mb":
			fi.InvertedSzMB = toFloat(val)
		case "doc_table_size_mb":
			fi.DocTableSzMB = toFloat(val)
		case "total_index_memory_sz_mb":
			fi.TotalIndexMemMB = toFloat(val)
		case "total_inverted_index_blocks":
			fi.TotalInvertedIdxBlocks = toInt(val)
		case "hash_indexing_failures":
			fi.HashIndexingFailures = toInt(val)
		case "indexing":
			fi.Indexing = toInt(val)
		case "percent_indexed":
			fi.PercentIndexed = toFloat(val)
		case "cleaning":
			fi.Cleaning = toInt(val)
		}
	}
	return fi, nil
}

// ServerInfo holds the process/disk-level metrics scraped from INFO.
type ServerInfo struct {
	UsedMemory             int64 // always present
	DiskUsage              int64 // search_disk_usage; 0 when not disk-backed
	AsyncReadsExpired      int64 // best-effort
	CompactionCycles       int64 // best-effort (text CF)
	PendingCompactionBytes int64 // best-effort
	DiskMode               bool  // true if search_disk_usage was present
}

var numNear = func(name string) *regexp.Regexp {
	return regexp.MustCompile(regexp.QuoteMeta(name) + `[=:]\s*(\d+)`)
}

// ServerInfo reads INFO everything and extracts memory + (if present) disk metrics.
func (c *Client) ServerInfo(ctx context.Context) (ServerInfo, error) {
	var si ServerInfo
	text, err := c.rdb.Info(ctx, "everything").Result()
	if err != nil {
		return si, err
	}
	fields := parseInfo(text)
	si.UsedMemory = toInt(fields["used_memory"])
	if v, ok := fields["search_disk_usage"]; ok {
		si.DiskUsage = toInt(v)
		si.DiskMode = true
	}
	// Nested disk-dict fields are flattened by the module as `name=value`; scan
	// for them best-effort so we don't depend on the exact rendering.
	si.AsyncReadsExpired = scanNum(text, "async_reads_expired")
	si.CompactionCycles = scanNum(text, "compaction_total_cycles")
	si.PendingCompactionBytes = scanNum(text, "estimate_pending_compaction_bytes")
	return si, nil
}

// DiskFlush flushes memtables to SST (disk only). Best-effort.
func (c *Client) DiskFlush(ctx context.Context) error {
	return c.rdb.Do(ctx, "_FT.DEBUG", "DISK_FLUSH", c.index).Err()
}

// ForceGC runs a synchronous GC/compaction cycle. Best-effort.
func (c *Client) ForceGC(ctx context.Context) error {
	return c.rdb.Do(ctx, "_FT.DEBUG", "GC_FORCEINVOKE", c.index).Err()
}

// DebugReload triggers an in-process RDB save+load round-trip.
func (c *Client) DebugReload(ctx context.Context) error {
	return c.rdb.Do(ctx, "DEBUG", "RELOAD").Err()
}

// FlushAll wipes the DB (used to start smoke runs from a clean slate).
func (c *Client) FlushAll(ctx context.Context) error {
	return c.rdb.FlushAll(ctx).Err()
}

// --- helpers ---

func parseInfo(text string) map[string]string {
	m := make(map[string]string, 256)
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimRight(line, "\r")
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if i := strings.IndexByte(line, ':'); i > 0 {
			m[line[:i]] = line[i+1:]
		}
	}
	return m
}

func scanNum(text, name string) int64 {
	m := numNear(name).FindStringSubmatch(text)
	if len(m) == 2 {
		n, _ := strconv.ParseInt(m[1], 10, 64)
		return n
	}
	return 0
}

func toInt(v any) int64 {
	switch t := v.(type) {
	case int64:
		return t
	case int:
		return int64(t)
	case float64:
		return int64(t)
	case string:
		n, _ := strconv.ParseInt(strings.TrimSpace(t), 10, 64)
		return n
	}
	return 0
}

func toFloat(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case int64:
		return float64(t)
	case string:
		f, _ := strconv.ParseFloat(strings.TrimSpace(t), 64)
		return f
	}
	return 0
}
