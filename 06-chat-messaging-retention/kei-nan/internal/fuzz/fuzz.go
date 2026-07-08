// Package fuzz composes random, disk-legal FT.SEARCH / FT.AGGREGATE queries for
// fuzz-testing the on-disk engine across the supported query surface.
//
// The grammar is derived from RediSearch's command schema (deps/RediSearch/
// commands.json — the argument trees of FT.SEARCH and FT.AGGREGATE) intersected
// with the MS2 Flex limitations. Args that Flex rejects are deliberately never
// emitted: numeric/geo FILTER, GEOFILTER, SUMMARIZE, HIGHLIGHT, SLOP, INORDER,
// WITHSUFFIXTRIE, FT.SEARCH SORTBY (disk allows it only for vector distance),
// FT.AGGREGATE WITHCURSOR, and TAG prefix/suffix/infix/wildcard/lexrange. TEXT
// prefix/fuzzy/wildcard/phrase/negation/optional/union ARE emitted (supported).
package fuzz

import (
	"math/rand"
	"strconv"
	"strings"

	"chatstress/internal/gendata"
	"chatstress/internal/model"
	"chatstress/internal/oracle"
)

// Query is one composed query ready to execute after the index name.
type Query struct {
	Cmd       string // "FT.SEARCH" or "FT.AGGREGATE"
	Args      []any  // args AFTER the index name
	Display   string // human-readable form for the feed
	NoContent bool   // FT.SEARCH with NOCONTENT => reply keys are checkable
}

// Params carries schema knobs the fuzzer needs.
type Params struct {
	Tiers     []string // plan/tier TAG values
	PageDepth int      // upper bound for random LIMIT offsets
	Limit     int      // result LIMIT (from the live control)
	TimeoutMs int      // per-query TIMEOUT (0 = none)
}

var (
	tagFields  = []string{model.FieldChannel, model.FieldUser, model.FieldThread, model.FieldTenant}
	scorers    = []string{"BM25STD", "TFIDF", "TFIDF.DOCNORM", "DISMAX", "DOCSCORE"}
	numFields  = []string{model.FieldTs, model.FieldSeq}
	groupables = []string{model.FieldPlan, model.FieldTenant, model.FieldChannel, model.FieldUser, model.FieldThread}
)

// Build composes a random query. When hasLive, some queries are scoped to a
// live (channel, token) so they hit real data.
func Build(rnd *rand.Rand, pick *model.Picker, gen *gendata.Generator, p Params, sample oracle.Sample, hasLive bool) Query {
	if rnd.Intn(100) < 55 {
		return buildSearch(rnd, pick, gen, p, sample, hasLive)
	}
	return buildAggregate(rnd, pick, gen, p, sample, hasLive)
}

// --- FT.SEARCH ---

func buildSearch(rnd *rand.Rand, pick *model.Picker, gen *gendata.Generator, p Params, sample oracle.Sample, hasLive bool) Query {
	expr := queryExpr(rnd, pick, gen, p, sample, hasLive)
	args := []any{expr}
	disp := []string{"FT.SEARCH", "'" + expr + "'"}
	add := func(parts ...any) {
		for _, x := range parts {
			args = append(args, x)
			disp = append(disp, toStr(x))
		}
	}
	if rnd.Intn(100) < 35 {
		add("VERBATIM")
	}
	if rnd.Intn(100) < 15 {
		add("NOSTOPWORDS")
	}
	if rnd.Intn(100) < 35 {
		add("SCORER", scorers[rnd.Intn(len(scorers))])
	}
	if rnd.Intn(100) < 15 {
		add("LANGUAGE", "english")
	}
	if rnd.Intn(100) < 15 {
		add("INFIELDS", 1, model.FieldBody)
	}
	noContent := rnd.Intn(100) < 75
	if noContent {
		add("NOCONTENT")
	} else if rnd.Intn(2) == 0 {
		add("RETURN", 2, "@"+model.FieldUser, "@"+model.FieldPlan)
	}
	off := 0
	if p.PageDepth > 0 && rnd.Intn(100) < 30 {
		off = rnd.Intn(p.PageDepth)
	}
	num := p.Limit
	if num < 1 {
		num = 20
	}
	add("LIMIT", off, num)
	if p.TimeoutMs > 0 {
		add("TIMEOUT", p.TimeoutMs)
	}
	add("DIALECT", 2)
	return Query{Cmd: "FT.SEARCH", Args: args, Display: strings.Join(disp, " "), NoContent: noContent}
}

// --- FT.AGGREGATE ---

func buildAggregate(rnd *rand.Rand, pick *model.Picker, gen *gendata.Generator, p Params, sample oracle.Sample, hasLive bool) Query {
	// aggregate filter: usually a channel scope (non-empty), sometimes match-all.
	filter := "*"
	if hasLive && rnd.Intn(3) != 0 {
		filter = "@" + model.FieldChannel + ":{" + sample.Channel + "}"
	} else if rnd.Intn(3) == 0 {
		filter = tagClause(rnd, pick, p)
	}
	args := []any{filter}
	disp := []string{"FT.AGGREGATE", "'" + filter + "'"}
	add := func(parts ...any) {
		for _, x := range parts {
			args = append(args, x)
			disp = append(disp, toStr(x))
		}
	}

	if rnd.Intn(2) == 0 {
		// GROUPBY shape
		gcount := 1 + rnd.Intn(2)
		add("GROUPBY", gcount)
		seen := map[string]bool{}
		for i := 0; i < gcount; i++ {
			f := groupables[rnd.Intn(len(groupables))]
			for seen[f] {
				f = groupables[rnd.Intn(len(groupables))]
			}
			seen[f] = true
			add("@" + f)
		}
		switch rnd.Intn(3) {
		case 0:
			add("REDUCE", "COUNT", 0, "AS", "n")
		case 1:
			add("REDUCE", "COUNT_DISTINCT", 1, "@"+tagFields[rnd.Intn(len(tagFields))], "AS", "n")
		default:
			add("REDUCE", "TOLIST", 1, "@"+model.FieldUser, "AS", "n")
		}
		if rnd.Intn(2) == 0 {
			add("SORTBY", 2, "@n", dir(rnd))
		}
		add("LIMIT", 0, 10+rnd.Intn(15))
	} else {
		// LOAD / FILTER exists / APPLY / SORTBY shape
		nf := numFields[rnd.Intn(len(numFields))]
		add("LOAD", 2, "@"+nf, "@"+model.FieldUser)
		// exists() guards the sort so an expired-but-unreclaimed doc (empty hash)
		// doesn't make the APPLY throw once SORTBY forces every row to evaluate.
		add("FILTER", "exists(@"+nf+")")
		if rnd.Intn(2) == 0 {
			add("APPLY", "to_number(@"+nf+")", "AS", "v")
			add("SORTBY", 2, "@v", dir(rnd))
		} else {
			add("APPLY", "upper(@"+model.FieldUser+")", "AS", "u")
			add("SORTBY", 2, "@u", dir(rnd))
		}
		add("LIMIT", 0, 10+rnd.Intn(20))
	}
	if p.TimeoutMs > 0 {
		add("TIMEOUT", p.TimeoutMs)
	}
	return Query{Cmd: "FT.AGGREGATE", Args: args, Display: strings.Join(disp, " ")}
}

// --- query-expression grammar (shared) ---

func queryExpr(rnd *rand.Rand, pick *model.Picker, gen *gendata.Generator, p Params, sample oracle.Sample, hasLive bool) string {
	var clauses []string
	// Scope to a live channel + its token often, so results are non-empty.
	if hasLive && rnd.Intn(100) < 60 {
		clauses = append(clauses, "@"+model.FieldChannel+":{"+sample.Channel+"}")
		if rnd.Intn(2) == 0 {
			clauses = append(clauses, sample.Token)
		}
	}
	extra := rnd.Intn(3) // 0..2 more clauses
	for i := 0; i < extra; i++ {
		if rnd.Intn(2) == 0 {
			clauses = append(clauses, tagClause(rnd, pick, p))
		} else {
			clauses = append(clauses, textClause(rnd, gen))
		}
	}
	if len(clauses) == 0 {
		if rnd.Intn(3) == 0 {
			return "*"
		}
		clauses = append(clauses, textClause(rnd, gen))
	}
	return strings.Join(clauses, " ")
}

func tagClause(rnd *rand.Rand, pick *model.Picker, p Params) string {
	// plan tier (single or OR of two) or another TAG scope
	if len(p.Tiers) > 0 && rnd.Intn(2) == 0 {
		if len(p.Tiers) >= 2 && rnd.Intn(2) == 0 {
			return "@" + model.FieldPlan + ":{" + p.Tiers[rnd.Intn(len(p.Tiers))] + "|" + p.Tiers[rnd.Intn(len(p.Tiers))] + "}"
		}
		return "@" + model.FieldPlan + ":{" + p.Tiers[rnd.Intn(len(p.Tiers))] + "}"
	}
	tid, tenant := pick.Tenant()
	cid, channel := pick.ChannelID(tid)
	switch rnd.Intn(4) {
	case 0:
		return "@" + model.FieldTenant + ":{" + tenant + "}"
	case 1:
		return "@" + model.FieldChannel + ":{" + channel + "}"
	case 2:
		return "@" + model.FieldUser + ":{" + pick.User(tid) + "}"
	default:
		return "@" + model.FieldThread + ":{" + pick.Thread(cid) + "}"
	}
}

func textClause(rnd *rand.Rand, gen *gendata.Generator) string {
	w := gen.Word()
	switch rnd.Intn(8) {
	case 0:
		return "(" + w + "|" + gen.Word() + ")" // union
	case 1:
		return "-" + w // negation
	case 2:
		return "~" + w // optional
	case 3:
		return prefixOf(w) // prefix
	case 4:
		return "%" + w + "%" // fuzzy (LD 1)
	case 5:
		return wildcardOf(w) // wildcard
	case 6:
		return "\"" + w + " " + gen.Word() + "\"" // phrase
	default:
		return w // plain term
	}
}

// --- helpers ---

func dir(rnd *rand.Rand) string {
	if rnd.Intn(2) == 0 {
		return "ASC"
	}
	return "DESC"
}

func prefixOf(w string) string {
	rs := []rune(w)
	if len(rs) <= 3 {
		return w + "*"
	}
	return string(rs[:len(rs)-1]) + "*"
}

func wildcardOf(w string) string {
	rs := []rune(w)
	if len(rs) < 3 {
		return prefixOf(w)
	}
	return "w'" + string(rs[0]) + "?" + string(rs[2:]) + "'"
}

func toStr(x any) string {
	switch v := x.(type) {
	case string:
		return v
	case int:
		return strconv.Itoa(v)
	default:
		return ""
	}
}
