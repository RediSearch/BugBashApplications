// Package gendata generates synthetic chat message bodies from a fixed vocabulary
// of content words (deliberately no stopwords, so any chosen word is a valid,
// non-dropped search term). The generator is deterministic given a seed, which
// keeps runs reproducible.
package gendata

import (
	"math/rand"
	"strings"
)

// vocab is a bag of realistic content words spanning a few chat-ish topics.
// Kept free of RediSearch default stopwords so a body word is always queryable.
var vocab = []string{
	"deploy", "release", "rollback", "incident", "latency", "throughput", "cache",
	"index", "shard", "replica", "primary", "failover", "cluster", "backup",
	"restore", "snapshot", "migration", "schema", "query", "pipeline", "batch",
	"stream", "consumer", "producer", "topic", "partition", "offset", "commit",
	"branch", "merge", "review", "approve", "revert", "hotfix", "feature",
	"sprint", "backlog", "ticket", "standup", "retro", "roadmap", "milestone",
	"budget", "invoice", "契約", "renewal", "upgrade", "downgrade", "trial",
	"onboarding", "support", "escalation", "priority", "severity", "outage",
	"dashboard", "metric", "alert", "threshold", "anomaly", "regression",
	"benchmark", "profiling", "memory", "compaction", "eviction", "expiry",
	"retention", "encryption", "certificate", "rotation", "credential", "token",
	"session", "cookie", "gateway", "proxy", "router", "firewall", "subnet",
	"payload", "header", "checksum", "compression", "serialization", "protocol",
	"handshake", "timeout", "retry", "circuit", "throttle", "quota", "capacity",
	"scaling", "autoscaler", "container", "namespace", "workload", "scheduler",
	"coffee", "lunch", "weekend", "holiday", "birthday", "congrats", "welcome",
	"thanks", "awesome", "brilliant", "curious", "excited", "worried", "blocked",
	"pizza", "sushi", "coffee", "meeting", "calendar", "reminder", "deadline",
	"proposal", "contract", "customer", "partner", "vendor", "shipment", "warehouse",
	"forecast", "revenue", "pricing", "discount", "campaign", "launch", "beta",
	"prototype", "mockup", "wireframe", "usability", "accessibility", "telemetry",
	"webhook", "integration", "connector", "adapter", "plugin", "extension",
	"kanban", "velocity", "estimate", "capacity", "handoff", "runbook", "playbook",
	"москва", "berlin", "tokyo", "paris", "sydney", "toronto", "mumbai", "lagos",
}

// Generator produces bodies and picks tokens.
type Generator struct {
	r        *rand.Rand
	minWords int
	maxWords int
}

// New returns a Generator seeded from seed+salt.
func New(seed, salt int64, minWords, maxWords int) *Generator {
	if maxWords < minWords {
		maxWords = minWords
	}
	return &Generator{
		r:        rand.New(rand.NewSource(seed*2_654_435_761 + salt)),
		minWords: minWords,
		maxWords: maxWords,
	}
}

// Body returns a random body string and a "token" — a content word guaranteed to
// appear in the body — usable as a search term that should match this message.
func (g *Generator) Body() (body, token string) {
	n := g.minWords
	if g.maxWords > g.minWords {
		n += g.r.Intn(g.maxWords - g.minWords + 1)
	}
	words := make([]string, n)
	tokenIdx := g.r.Intn(n)
	for i := range words {
		words[i] = vocab[g.r.Intn(len(vocab))]
	}
	return strings.Join(words, " "), words[tokenIdx]
}

// Word returns a single random vocabulary word (used for miss-generating queries).
func (g *Generator) Word() string { return vocab[g.r.Intn(len(vocab))] }
