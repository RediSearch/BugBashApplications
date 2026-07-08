// Package querygen builds the concrete, randomized queries that fill the shared
// query pool the background workers run. Building is separated from execution so
// the pool can be (re)generated on demand — e.g. from the dashboard's "Randomize
// queries" button — without the workers stopping. Each profile targets a disk
// pattern/weak-point; the "fuzz" profile delegates to the broad-coverage fuzzer.
package querygen

import (
	"math/rand"
	"strconv"

	"chatstress/internal/config"
	"chatstress/internal/control"
	"chatstress/internal/fuzz"
	"chatstress/internal/gendata"
	"chatstress/internal/model"
	"chatstress/internal/oracle"
)

// Deps are the inputs needed to build queries.
type Deps struct {
	Pick      *model.Picker
	Gen       *gendata.Generator
	Tiers     []string
	PageDepth int
	Limit     int
	TimeoutMs int
	Live      func() (oracle.Sample, bool) // a currently-live (channel, token), if any
}

// Pool builds n query items, drawing a profile per item from the weighted mix.
func Pool(rnd *rand.Rand, n int, mix []config.QueryProfile, d Deps) []control.QueryItem {
	total := 0
	for _, p := range mix {
		if p.Weight > 0 {
			total += p.Weight
		}
	}
	items := make([]control.QueryItem, 0, n)
	for i := 0; i < n; i++ {
		prof := "fuzz"
		if total > 0 {
			x := rnd.Intn(total)
			for _, p := range mix {
				if p.Weight <= 0 {
					continue
				}
				if x < p.Weight {
					prof = p.Name
					break
				}
				x -= p.Weight
			}
		}
		items = append(items, BuildOne(rnd, prof, d))
	}
	return items
}

// BuildOne composes one randomized query of the given profile.
func BuildOne(rnd *rand.Rand, profile string, d Deps) control.QueryItem {
	switch profile {
	case "channel_search":
		ch, tok := d.channelTerm(rnd)
		return d.search(profile, "@"+model.FieldChannel+":{"+ch+"} "+tok, 0, true)

	case "thread_search":
		field, val := d.randScope(rnd)
		return d.search(profile, "@"+field+":{"+val+"} "+d.randTerm(rnd), 0, true)

	case "tag_filter":
		var scope string
		if len(d.Tiers) >= 2 && rnd.Intn(2) == 0 {
			scope = "@" + model.FieldPlan + ":{" + d.tier(rnd) + "|" + d.tier(rnd) + "}"
		} else if len(d.Tiers) > 0 && rnd.Intn(2) == 0 {
			scope = "@" + model.FieldPlan + ":{" + d.tier(rnd) + "}"
		} else {
			_, tenant := d.Pick.Tenant()
			scope = "@" + model.FieldTenant + ":{" + tenant + "}"
		}
		term := d.randTerm(rnd)
		if rnd.Intn(3) == 0 {
			term += " -" + d.Gen.Word()
		}
		return d.search(profile, scope+" "+term, 0, true)

	case "deep_pagination":
		ch, tok := d.channelTerm(rnd)
		off := 0
		if d.PageDepth > 0 {
			off = rnd.Intn(d.PageDepth)
		}
		return d.search(profile, "@"+model.FieldChannel+":{"+ch+"} "+tok, off, true)

	case "text_prefix":
		ch, tok := d.channelTerm(rnd)
		return d.search(profile, "@"+model.FieldChannel+":{"+ch+"} "+fuzzyOf(rnd, tok), 0, true)

	case "recent_timeline":
		ch, _ := d.channelTerm(rnd)
		dir := dirOf(rnd)
		filter := "@" + model.FieldChannel + ":{" + ch + "}"
		tail := []any{
			"LOAD", 2, "@" + model.FieldTs, "@" + model.FieldUser,
			"FILTER", "exists(@" + model.FieldTs + ")",
			"APPLY", "to_number(@" + model.FieldTs + ")", "AS", "ts_num",
			"SORTBY", 2, "@ts_num", dir, "LIMIT", 0, d.Limit}
		disp := "FT.AGGREGATE '" + filter + "' LOAD @ts @user_id FILTER exists(@ts) APPLY to_number(@ts) SORTBY @ts_num " + dir + " LIMIT 0 " + strconv.Itoa(d.Limit)
		return d.agg(profile, filter, tail, disp)

	case "plan_analytics":
		gfield := groupables[rnd.Intn(len(groupables))]
		redArgs, redDisp := randReducer(rnd)
		tail := []any{"GROUPBY", 1, "@" + gfield}
		tail = append(tail, redArgs...)
		tail = append(tail, "SORTBY", 2, "@n", "DESC", "LIMIT", 0, d.Limit)
		disp := "FT.AGGREGATE '*' GROUPBY @" + gfield + " REDUCE " + redDisp + " SORTBY @n DESC LIMIT 0 " + strconv.Itoa(d.Limit)
		return d.agg(profile, "*", tail, disp)

	default: // "fuzz"
		s, ok := d.Live()
		q := fuzz.Build(rnd, d.Pick, d.Gen, fuzz.Params{Tiers: d.Tiers, PageDepth: d.PageDepth, Limit: d.Limit, TimeoutMs: d.TimeoutMs}, s, ok)
		return control.QueryItem{Profile: "fuzz", Cmd: q.Cmd, Args: q.Args, Display: q.Display, NoContent: q.NoContent}
	}
}

// --- builders (bake limit + timeout into the query so they're visible & effective) ---

func (d Deps) search(profile, expr string, offset int, noContent bool) control.QueryItem {
	args := []any{expr}
	if noContent {
		args = append(args, "NOCONTENT")
	}
	args = append(args, "LIMIT", offset, d.Limit)
	disp := "FT.SEARCH " + expr + " LIMIT " + strconv.Itoa(offset) + " " + strconv.Itoa(d.Limit)
	if d.TimeoutMs > 0 {
		args = append(args, "TIMEOUT", d.TimeoutMs)
		disp += " TIMEOUT " + strconv.Itoa(d.TimeoutMs)
	}
	args = append(args, "DIALECT", 2)
	return control.QueryItem{Profile: profile, Cmd: "FT.SEARCH", Args: args, Display: disp, NoContent: noContent}
}

func (d Deps) agg(profile, filter string, tail []any, disp string) control.QueryItem {
	args := append([]any{filter}, tail...)
	if d.TimeoutMs > 0 {
		args = append(args, "TIMEOUT", d.TimeoutMs)
		disp += " TIMEOUT " + strconv.Itoa(d.TimeoutMs)
	}
	return control.QueryItem{Profile: profile, Cmd: "FT.AGGREGATE", Args: args, Display: disp}
}

// --- randomization helpers ---

var groupables = []string{model.FieldPlan, model.FieldTenant, model.FieldChannel, model.FieldUser, model.FieldThread}

func (d Deps) tier(rnd *rand.Rand) string { return d.Tiers[rnd.Intn(len(d.Tiers))] }

// channelTerm returns a live (channel, token) when available, else a random one.
func (d Deps) channelTerm(rnd *rand.Rand) (string, string) {
	if s, ok := d.Live(); ok {
		return s.Channel, s.Token
	}
	tid, _ := d.Pick.Tenant()
	_, ch := d.Pick.ChannelID(tid)
	return ch, d.Gen.Word()
}

func (d Deps) randScope(rnd *rand.Rand) (string, string) {
	tid, _ := d.Pick.Tenant()
	cid, channel := d.Pick.ChannelID(tid)
	switch rnd.Intn(3) {
	case 0:
		return model.FieldThread, d.Pick.Thread(cid)
	case 1:
		return model.FieldUser, d.Pick.User(tid)
	default:
		return model.FieldChannel, channel
	}
}

func (d Deps) randTerm(rnd *rand.Rand) string {
	if rnd.Intn(3) == 0 {
		return "(" + d.Gen.Word() + "|" + d.Gen.Word() + ")"
	}
	return d.Gen.Word()
}

func randReducer(rnd *rand.Rand) ([]any, string) {
	switch rnd.Intn(3) {
	case 0:
		return []any{"REDUCE", "COUNT_DISTINCT", 1, "@" + model.FieldUser, "AS", "n"}, "COUNT_DISTINCT @user_id AS n"
	case 1:
		return []any{"REDUCE", "COUNT_DISTINCT", 1, "@" + model.FieldThread, "AS", "n"}, "COUNT_DISTINCT @thread_id AS n"
	default:
		return []any{"REDUCE", "COUNT", 0, "AS", "n"}, "COUNT AS n"
	}
}

func dirOf(rnd *rand.Rand) string {
	if rnd.Intn(2) == 0 {
		return "ASC"
	}
	return "DESC"
}

func fuzzyOf(rnd *rand.Rand, w string) string {
	rs := []rune(w)
	switch rnd.Intn(3) {
	case 0:
		return "%" + w + "%"
	case 1:
		if len(rs) > 2 {
			return "w'" + string(rs[0]) + "?" + string(rs[2:]) + "'"
		}
		fallthrough
	default:
		if len(rs) <= 3 {
			return w + "*"
		}
		return string(rs[:len(rs)-1]) + "*"
	}
}
