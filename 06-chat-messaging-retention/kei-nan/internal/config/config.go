// Package config defines the runtime configuration for the chatstress harness
// and loads it from a YAML file (with a few flag overrides applied by main).
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration is a time.Duration that unmarshals from a Go duration string
// (e.g. "90s", "30m", "2h") in YAML.
type Duration time.Duration

// UnmarshalYAML parses a duration string such as "45s" into a Duration.
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

// D returns the value as a time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// Tier is one retention tier: a whole-key TTL applied to a weighted fraction of
// messages. Short- and long-lived tiers coexist in a single index.
type Tier struct {
	Name   string   `yaml:"name"`
	Weight int      `yaml:"weight"`
	TTL    Duration `yaml:"ttl"`
}

// QueryProfile is one high-level query kind and its relative weight in the mix.
type QueryProfile struct {
	Name   string `yaml:"name"`
	Weight int    `yaml:"weight"`
}

// KnownProfiles is the set of query-profile names the workload can execute.
// Each targets a disk query pattern / weak-point (see workload.go).
var KnownProfiles = map[string]string{
	"channel_search":  "full-text scoped to a channel TAG (the common case; recall-checked)",
	"thread_search":   "full-text scoped to a thread TAG (high-cardinality posting lists)",
	"tag_filter":      "retention-tier / tenant TAG filter + term",
	"recent_timeline": "FT.AGGREGATE LOAD @ts + APPLY to_number + SORTBY (message timeline)",
	"plan_analytics":  "FT.AGGREGATE GROUPBY high-cardinality TAG + REDUCE COUNT (wide groupby)",
	"deep_pagination": "FT.SEARCH with a large LIMIT offset (OOM / large-offset weak-point)",
	"text_prefix":     "TEXT prefix query foo* (prefix expansion)",
	"fuzz":            "fully randomized disk-legal FT.SEARCH/FT.AGGREGATE (broad fuzz testing)",
}

// Config is the full harness configuration.
type Config struct {
	Addr     string   `yaml:"addr"`
	Password string   `yaml:"password"`
	Index    string   `yaml:"index"`
	Prefix   string   `yaml:"prefix"`
	Duration Duration `yaml:"duration"`
	Seed     int64    `yaml:"seed"`

	Tenants           int `yaml:"tenants"`
	ChannelsPerTenant int `yaml:"channels_per_tenant"`
	UsersPerTenant    int `yaml:"users_per_tenant"`
	ThreadsPerChannel int `yaml:"threads_per_channel"`

	Ingest struct {
		Workers  int `yaml:"workers"`
		Rate     int `yaml:"rate"`
		Pipeline int `yaml:"pipeline"`
	} `yaml:"ingest"`

	Query struct {
		Workers    int            `yaml:"workers"`     // initial query concurrency (threads/connections)
		MaxWorkers int            `yaml:"max_workers"` // upper bound for live concurrency (sizes the conn pool)
		Rate       int            `yaml:"rate"`        // aggregate queries/sec target (0 = unbounded)
		Limit      int            `yaml:"limit"`
		PageDepth  int            `yaml:"page_depth"` // max offset for the deep-pagination profile
		SlowMs     int            `yaml:"slow_ms"`    // a query at/above this latency counts as "slow" (timeout-risk)
		PoolSize   int            `yaml:"pool_size"`  // number of concrete queries in the pool the workers run
		Profiles   []QueryProfile `yaml:"profiles"`   // the high-level query mix
	} `yaml:"query"`

	Edit    struct{ Rate int } `yaml:"edit"`
	Delete  struct{ Rate int } `yaml:"delete"`
	Sliding struct{ Rate int } `yaml:"sliding"`

	Body struct {
		MinWords  int `yaml:"min_words"`
		MaxWords  int `yaml:"max_words"`
		VocabHigh int `yaml:"vocab_high"` // size of a synthetic high-cardinality term pool sprinkled into bodies (0 = base vocab only); stresses the pinned-RAM term dictionary
	} `yaml:"body"`

	Tiers []Tier `yaml:"tiers"`

	Sample struct {
		Interval   Duration `yaml:"interval"`
		GCInterval Duration `yaml:"gc_interval"`
		ForceGC    bool     `yaml:"force_gc"`
	} `yaml:"sample"`

	Oracle struct {
		SampleRate float64 `yaml:"sample_rate"`
		MaxTracked int     `yaml:"max_tracked"`
		GraceMs    int     `yaml:"grace_ms"`
	} `yaml:"oracle"`

	Web struct {
		Enabled bool   `yaml:"enabled"`
		Addr    string `yaml:"addr"`
	} `yaml:"web"`

	OutDir string `yaml:"out_dir"`
}

// Default returns a Config populated with sensible defaults. Loading a YAML file
// over this leaves unspecified fields at their defaults.
func Default() *Config {
	c := &Config{
		Addr:              "127.0.0.1:6379",
		Index:             "chat",
		Prefix:            "msg:",
		Duration:          Duration(90 * time.Second),
		Seed:              1,
		Tenants:           20,
		ChannelsPerTenant: 10,
		UsersPerTenant:    50,
		ThreadsPerChannel: 8,
		OutDir:            "./out",
	}
	c.Ingest.Workers, c.Ingest.Rate, c.Ingest.Pipeline = 4, 2000, 200
	c.Query.Workers, c.Query.MaxWorkers, c.Query.Rate, c.Query.Limit = 4, 64, 200, 20
	c.Query.PageDepth, c.Query.SlowMs, c.Query.PoolSize = 2000, 500, 64
	c.Query.Profiles = []QueryProfile{
		{Name: "channel_search", Weight: 45},
		{Name: "thread_search", Weight: 15},
		{Name: "tag_filter", Weight: 10},
		{Name: "recent_timeline", Weight: 15},
		{Name: "plan_analytics", Weight: 5},
		{Name: "deep_pagination", Weight: 5},
		{Name: "text_prefix", Weight: 5},
		{Name: "fuzz", Weight: 10},
	}
	c.Edit.Rate, c.Delete.Rate, c.Sliding.Rate = 50, 50, 100
	c.Body.MinWords, c.Body.MaxWords, c.Body.VocabHigh = 6, 24, 0
	c.Tiers = []Tier{
		{Name: "disappearing", Weight: 30, TTL: Duration(15 * time.Second)},
		{Name: "free", Weight: 50, TTL: Duration(45 * time.Second)},
		{Name: "pro", Weight: 20, TTL: Duration(120 * time.Second)},
	}
	c.Sample.Interval = Duration(2 * time.Second)
	c.Sample.GCInterval = Duration(10 * time.Second)
	c.Sample.ForceGC = true
	c.Oracle.SampleRate, c.Oracle.MaxTracked, c.Oracle.GraceMs = 1.0, 500000, 1500
	c.Web.Enabled, c.Web.Addr = true, ":8080"
	return c
}

// Load reads a YAML config file over the defaults and validates it.
func Load(path string) (*Config, error) {
	c := Default()
	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read config: %w", err)
		}
		if err := yaml.Unmarshal(raw, c); err != nil {
			return nil, fmt.Errorf("parse config: %w", err)
		}
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

// Validate checks required invariants.
func (c *Config) Validate() error {
	if c.Tenants < 1 || c.ChannelsPerTenant < 1 || c.UsersPerTenant < 1 || c.ThreadsPerChannel < 1 {
		return fmt.Errorf("id-space dimensions must all be >= 1")
	}
	if len(c.Tiers) == 0 {
		return fmt.Errorf("at least one retention tier is required")
	}
	if c.TotalTierWeight() <= 0 {
		return fmt.Errorf("retention tier weights must sum to > 0")
	}
	if c.Body.MinWords < 1 || c.Body.MaxWords < c.Body.MinWords {
		return fmt.Errorf("body word range invalid")
	}
	if c.Ingest.Pipeline < 1 {
		c.Ingest.Pipeline = 1
	}
	if c.Oracle.SampleRate <= 0 || c.Oracle.SampleRate > 1 {
		return fmt.Errorf("oracle.sample_rate must be in (0,1]")
	}
	if len(c.Query.Profiles) == 0 {
		return fmt.Errorf("at least one query profile is required")
	}
	if c.TotalProfileWeight() <= 0 {
		return fmt.Errorf("query profile weights must sum to > 0")
	}
	for _, p := range c.Query.Profiles {
		if _, ok := KnownProfiles[p.Name]; !ok {
			return fmt.Errorf("unknown query profile %q (known: see config.KnownProfiles)", p.Name)
		}
	}
	if c.Query.SlowMs <= 0 {
		c.Query.SlowMs = 500
	}
	if c.Query.PageDepth < 0 {
		c.Query.PageDepth = 0
	}
	if c.Query.PoolSize < 1 {
		c.Query.PoolSize = 64
	}
	if c.Query.Workers < 1 {
		c.Query.Workers = 1
	}
	if c.Query.MaxWorkers < c.Query.Workers {
		c.Query.MaxWorkers = c.Query.Workers
	}
	return nil
}

// TotalTierWeight sums the tier weights.
func (c *Config) TotalTierWeight() int {
	sum := 0
	for _, t := range c.Tiers {
		sum += t.Weight
	}
	return sum
}

// TotalProfileWeight sums the query-profile weights.
func (c *Config) TotalProfileWeight() int {
	sum := 0
	for _, p := range c.Query.Profiles {
		sum += p.Weight
	}
	return sum
}
