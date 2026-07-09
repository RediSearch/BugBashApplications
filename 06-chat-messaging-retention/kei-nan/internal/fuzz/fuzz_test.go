package fuzz

import (
	"math/rand"
	"regexp"
	"strings"
	"testing"

	"chatstress/internal/gendata"
	"chatstress/internal/model"
	"chatstress/internal/oracle"
)

// hasArg reports whether the arg list contains tok as a standalone string arg
// (case-insensitive) — i.e. a command keyword, not text inside the query body.
func hasArg(args []any, tok string) bool {
	for _, a := range args {
		if s, ok := a.(string); ok && strings.EqualFold(s, tok) {
			return true
		}
	}
	return false
}

// TestFuzzDiskLegal is the fuzzer's core contract: over many generated queries it
// must NEVER emit an argument that Flex/BigRedis rejects.
func TestFuzzDiskLegal(t *testing.T) {
	sp := model.Space{Tenants: 100, ChannelsPerTenant: 10, UsersPerTenant: 50, ThreadsPerChannel: 8}
	pick := model.NewPicker(sp, 1, 7)
	gen := gendata.New(1, 9, 4, 12, 100000)
	rnd := rand.New(rand.NewSource(0xC0FFEE))
	params := Params{Tiers: []string{"free", "pro", "enterprise"}, PageDepth: 2000, Limit: 25, TimeoutMs: 300}
	sample := oracle.Sample{Key: "msg:000000000001", Channel: "c5", Token: "deploy"}

	// TAG clause: @<field>:{ <content> } — content must be alphanumeric / _ / | (OR)
	// only. Any *, ?, %, [, ] etc. would be a prefix/wildcard/lexrange TAG query,
	// which disk rejects.
	tagRe := regexp.MustCompile(`@[a-z_]+:\{([^}]*)\}`)
	tagContentOK := regexp.MustCompile(`^[A-Za-z0-9_|]+$`)

	// Forbidden as a standalone arg on ANY Flex query.
	forbiddenAny := []string{"SUMMARIZE", "HIGHLIGHT", "SLOP", "INORDER", "GEOFILTER", "WITHCURSOR", "WITHSUFFIXTRIE", "PAYLOAD"}

	for i := 0; i < 20000; i++ {
		hasLive := i%2 == 0
		q := Build(rnd, pick, gen, params, sample, hasLive)

		if q.Cmd != "FT.SEARCH" && q.Cmd != "FT.AGGREGATE" {
			t.Fatalf("unexpected command %q: %s", q.Cmd, q.Display)
		}
		if len(q.Args) == 0 {
			t.Fatalf("empty args: %s", q.Display)
		}
		// Args[0] is the free-text query EXPRESSION (arbitrary vocabulary words, e.g.
		// "payload" or "filter" — legitimate search terms). Command keywords can only
		// appear as OPTION args after it, so scan Args[1:] for forbidden keywords.
		opts := q.Args[1:]
		for _, bad := range forbiddenAny {
			if hasArg(opts, bad) {
				t.Fatalf("emitted forbidden arg %q: %s", bad, q.Display)
			}
		}
		if q.Cmd == "FT.SEARCH" {
			if hasArg(opts, "SORTBY") {
				t.Fatalf("FT.SEARCH must not use SORTBY on disk (vector-only): %s", q.Display)
			}
			if hasArg(opts, "FILTER") {
				t.Fatalf("FT.SEARCH must not use a numeric FILTER on disk: %s", q.Display)
			}
			if !hasArg(opts, "DIALECT") {
				t.Fatalf("FT.SEARCH must carry DIALECT (for wildcard/fuzzy): %s", q.Display)
			}
		}
		// The query expression is always the first arg.
		expr, ok := q.Args[0].(string)
		if !ok {
			t.Fatalf("first arg not a string: %s", q.Display)
		}
		for _, m := range tagRe.FindAllStringSubmatch(expr, -1) {
			if !tagContentOK.MatchString(m[1]) {
				t.Fatalf("illegal TAG content %q (prefix/wildcard/lexrange not allowed on disk): %s", m[1], q.Display)
			}
		}
	}
}

// TestFuzzBothCommandsAppear guards that the fuzzer actually exercises both
// FT.SEARCH and FT.AGGREGATE (a regression that only ever emitted one would
// silently shrink coverage).
func TestFuzzBothCommandsAppear(t *testing.T) {
	pick := model.NewPicker(model.Space{Tenants: 10, ChannelsPerTenant: 5, UsersPerTenant: 10, ThreadsPerChannel: 4}, 2, 3)
	gen := gendata.New(2, 4, 4, 10, 0)
	rnd := rand.New(rand.NewSource(1))
	p := Params{Tiers: []string{"free"}, PageDepth: 100, Limit: 10}
	var search, agg int
	for i := 0; i < 2000; i++ {
		switch Build(rnd, pick, gen, p, oracle.Sample{}, false).Cmd {
		case "FT.SEARCH":
			search++
		case "FT.AGGREGATE":
			agg++
		}
	}
	if search == 0 || agg == 0 {
		t.Fatalf("expected both command types; got search=%d agg=%d", search, agg)
	}
}
