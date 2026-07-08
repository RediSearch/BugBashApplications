// Package model defines the chat data model: the id-space of tenants, channels,
// users and threads, and the on-disk representation of a single message.
//
// Field-type note (disk constraint): only TEXT and TAG are indexable on disk, so
// tenant/channel/user/thread/plan are all TAG fields with alphanumeric values
// (no escaping needed) and the message body is the only TEXT field. `seq` and
// `ts` are stored as plain, UN-indexed hash fields purely for client-side
// ordering/display — there is no NUMERIC field in the schema.
package model

import (
	"math/rand"
	"strconv"
	"time"
)

// Space describes the cardinality of the id dimensions.
type Space struct {
	Tenants           int
	ChannelsPerTenant int
	UsersPerTenant    int
	ThreadsPerChannel int
}

// Message is one chat message and its indexed fields.
type Message struct {
	Key      string        // Redis key, e.g. "msg:000000001234"
	Seq      int64         // global monotonic sequence (unindexed, for ordering)
	Tenant   string        // TAG value, e.g. "t42"
	Channel  string        // TAG value, e.g. "c317"
	User     string        // TAG value, e.g. "u18"
	Thread   string        // TAG value, e.g. "th5"
	Plan     string        // TAG value = retention tier name
	Body     string        // TEXT
	Token    string        // a content word guaranteed present in Body (for queries/oracle)
	TTL      time.Duration // whole-key TTL
	CreateMs int64         // creation time (unix ms), stored as unindexed "ts"
}

// TagField / value names used in the schema and queries.
const (
	FieldBody    = "body"
	FieldTenant  = "tenant_id"
	FieldChannel = "channel_id"
	FieldUser    = "user_id"
	FieldThread  = "thread_id"
	FieldPlan    = "plan"
	FieldSeq     = "seq"
	FieldTs      = "ts"
)

// HashArgs returns the field/value pairs for HSET, in a stable order.
func (m *Message) HashArgs() []any {
	return []any{
		FieldBody, m.Body,
		FieldTenant, m.Tenant,
		FieldChannel, m.Channel,
		FieldUser, m.User,
		FieldThread, m.Thread,
		FieldPlan, m.Plan,
		FieldSeq, strconv.FormatInt(m.Seq, 10),
		FieldTs, strconv.FormatInt(m.CreateMs, 10),
	}
}

// Key builds the padded message key for a sequence number. Zero-padding keeps
// keys lexicographically ordered by age, which lets the UI sort "recent first"
// without a NUMERIC sort field.
func Key(prefix string, seq int64) string {
	s := strconv.FormatInt(seq, 10)
	// pad to 12 digits
	for len(s) < 12 {
		s = "0" + s
	}
	return prefix + s
}

// Picker draws random ids from the space using a per-goroutine RNG.
type Picker struct {
	sp Space
	r  *rand.Rand
}

// NewPicker returns a Picker seeded deterministically from seed+salt.
func NewPicker(sp Space, seed, salt int64) *Picker {
	return &Picker{sp: sp, r: rand.New(rand.NewSource(seed*1_000_003 + salt))}
}

// Tenant returns a random tenant tag value.
func (p *Picker) Tenant() (int, string) {
	id := p.r.Intn(p.sp.Tenants)
	return id, "t" + strconv.Itoa(id)
}

// ChannelID returns a globally-unique channel id (across tenants).
func (p *Picker) ChannelID(tenant int) (int, string) {
	local := p.r.Intn(p.sp.ChannelsPerTenant)
	gid := tenant*p.sp.ChannelsPerTenant + local
	return gid, "c" + strconv.Itoa(gid)
}

// User returns a random user tag value scoped to a tenant.
func (p *Picker) User(tenant int) string {
	uid := tenant*p.sp.UsersPerTenant + p.r.Intn(p.sp.UsersPerTenant)
	return "u" + strconv.Itoa(uid)
}

// Thread returns a random thread tag value scoped to a channel.
func (p *Picker) Thread(channel int) string {
	tid := channel*p.sp.ThreadsPerChannel + p.r.Intn(p.sp.ThreadsPerChannel)
	return "th" + strconv.Itoa(tid)
}

// Rand exposes the underlying RNG for callers that need extra draws.
func (p *Picker) Rand() *rand.Rand { return p.r }
