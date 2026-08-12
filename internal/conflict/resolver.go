package conflict

import (
	"time"

	"github.com/Akkeshri/distributed-kv-store/internal/store"
)

type Strategy string

const (
	StrategyLWW    Strategy = "lww"
	StrategyVector Strategy = "vector"
)

type Result struct {
	Record   store.Record
	Conflict bool
	Resolved bool
	Strategy Strategy
}

type Resolver struct {
	strategy Strategy
}

func NewResolver(strategy string) *Resolver {
	s := StrategyLWW
	if strategy == string(StrategyVector) {
		s = StrategyVector
	}
	return &Resolver{strategy: s}
}

func (r *Resolver) Resolve(records []store.Record) Result {
	if len(records) == 0 {
		return Result{Resolved: false, Strategy: r.strategy}
	}
	if len(records) == 1 {
		return Result{Record: records[0], Resolved: true, Strategy: r.strategy}
	}

	switch r.strategy {
	case StrategyVector:
		return r.resolveVector(records)
	default:
		return r.resolveLWW(records)
	}
}

func (r *Resolver) resolveLWW(records []store.Record) Result {
	best := records[0]
	conflict := false
	for _, rec := range records[1:] {
		if rec.Tombstone != best.Tombstone || rec.Value != best.Value {
			conflict = true
		}
		if rec.Timestamp.After(best.Timestamp) {
			best = rec
		} else if rec.Timestamp.Equal(best.Timestamp) && rec.Key > best.Key {
			best = rec
		}
	}
	return Result{
		Record:   best,
		Conflict: conflict,
		Resolved: true,
		Strategy: StrategyLWW,
	}
}

func (r *Resolver) resolveVector(records []store.Record) Result {
	best := records[0]
	conflict := false
	for _, rec := range records[1:] {
		cmp := store.CompareVersions(rec.Version, best.Version)
		if cmp == 0 && (rec.Value != best.Value || rec.Tombstone != best.Tombstone) {
			conflict = true
		}
		if cmp > 0 {
			best = rec
		} else if cmp == 0 {
			if rec.Timestamp.After(best.Timestamp) {
				best = rec
			}
			if rec.Value != best.Value || rec.Tombstone != best.Tombstone {
				conflict = true
			}
		} else {
			conflict = true
		}
	}
	return Result{
		Record:   best,
		Conflict: conflict,
		Resolved: true,
		Strategy: StrategyVector,
	}
}

func Now() time.Time {
	return time.Now().UTC()
}
