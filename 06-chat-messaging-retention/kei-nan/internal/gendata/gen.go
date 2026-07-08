// Package gendata generates synthetic chat message bodies from a fixed vocabulary
// of content words (deliberately no stopwords, so any chosen word is a valid,
// non-dropped search term). The generator is deterministic given a seed, which
// keeps runs reproducible.
package gendata

import (
	"math/rand"
	"strconv"
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
	r         *rand.Rand
	minWords  int
	maxWords  int
	vocabHigh int // size of a synthetic high-cardinality term pool (0 = disabled)
}

// New returns a Generator seeded from seed+salt. vocabHigh, when > 0, sprinkles
// synthetic unique-ish terms (drawn from a pool of that size) into each body to
// grow the on-disk TEXT term dictionary — which is pinned in RAM and scales with
// the number of *distinct* terms (a Flex weak-point). The returned search token
// is always a base-vocabulary word, so channel-scoped recall queries stay
// reliable regardless of vocabHigh.
func New(seed, salt int64, minWords, maxWords, vocabHigh int) *Generator {
	if maxWords < minWords {
		maxWords = minWords
	}
	return &Generator{
		r:         rand.New(rand.NewSource(seed*2_654_435_761 + salt)),
		minWords:  minWords,
		maxWords:  maxWords,
		vocabHigh: vocabHigh,
	}
}

// Body returns a random body string and a "token" — a base-vocabulary word
// guaranteed to appear in the body — usable as a search term that matches it.
func (g *Generator) Body() (body, token string) {
	n := g.minWords
	if g.maxWords > g.minWords {
		n += g.r.Intn(g.maxWords - g.minWords + 1)
	}
	words := make([]string, n)
	tokenIdx := g.r.Intn(n) // this position stays a base-vocab word (the token)
	for i := range words {
		if i != tokenIdx && g.vocabHigh > 0 && g.r.Intn(3) == 0 {
			// ~1/3 of non-token words are high-cardinality synthetic terms
			words[i] = "z" + strconv.Itoa(g.r.Intn(g.vocabHigh))
		} else {
			words[i] = vocab[g.r.Intn(len(vocab))]
		}
	}
	return strings.Join(words, " "), words[tokenIdx]
}

// Word returns a single random base-vocabulary word (used for query terms).
func (g *Generator) Word() string { return vocab[g.r.Intn(len(vocab))] }
