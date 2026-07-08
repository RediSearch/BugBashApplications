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
		Workers int `yaml:"workers"`
		Rate    int `yaml:"rate"`
		Limit   int `yaml:"limit"`
	} `yaml:"query"`

	Edit    struct{ Rate int } `yaml:"edit"`
	Delete  struct{ Rate int } `yaml:"delete"`
	Sliding struct{ Rate int } `yaml:"sliding"`

	Body struct {
		MinWords int `yaml:"min_words"`
		MaxWords int `yaml:"max_words"`
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
	c.Query.Workers, c.Query.Rate, c.Query.Limit = 4, 200, 20
	c.Edit.Rate, c.Delete.Rate, c.Sliding.Rate = 50, 50, 100
	c.Body.MinWords, c.Body.MaxWords = 6, 24
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
