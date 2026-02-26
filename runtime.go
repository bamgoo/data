package data

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/bamgoo/base"
)

type moduleStats struct {
	Queries  atomic.Int64
	Writes   atomic.Int64
	Errors   atomic.Int64
	Slow     atomic.Int64
	CacheHit atomic.Int64
}

type cacheValue struct {
	expireAt int64
	items    []Map
	total    int64
}

type Stats struct {
	Queries  int64 `json:"queries"`
	Writes   int64 `json:"writes"`
	Errors   int64 `json:"errors"`
	Slow     int64 `json:"slow"`
	CacheHit int64 `json:"cacheHit"`
}

var (
	statsRegistry sync.Map
	cacheRegistry sync.Map
	cacheVersion  sync.Map
)

func statsFor(name string) *moduleStats {
	if name == "" {
		name = "default"
	}
	if v, ok := statsRegistry.Load(name); ok {
		return v.(*moduleStats)
	}
	s := &moduleStats{}
	actual, _ := statsRegistry.LoadOrStore(name, s)
	return actual.(*moduleStats)
}

func (m *Module) Stats(names ...string) Stats {
	name := "default"
	if len(names) > 0 && strings.TrimSpace(names[0]) != "" {
		name = names[0]
	}
	s := statsFor(name)
	return Stats{
		Queries:  s.Queries.Load(),
		Writes:   s.Writes.Load(),
		Errors:   s.Errors.Load(),
		Slow:     s.Slow.Load(),
		CacheHit: s.CacheHit.Load(),
	}
}

func cacheMap(name string) *sync.Map {
	if name == "" {
		name = "default"
	}
	if v, ok := cacheRegistry.Load(name); ok {
		return v.(*sync.Map)
	}
	m := &sync.Map{}
	actual, _ := cacheRegistry.LoadOrStore(name, m)
	return actual.(*sync.Map)
}

func cacheVersionPtr(name string) *atomic.Uint64 {
	if name == "" {
		name = "default"
	}
	if v, ok := cacheVersion.Load(name); ok {
		return v.(*atomic.Uint64)
	}
	p := &atomic.Uint64{}
	actual, _ := cacheVersion.LoadOrStore(name, p)
	return actual.(*atomic.Uint64)
}

func cacheVersionGet(name string) uint64 {
	return cacheVersionPtr(name).Load()
}

func cacheVersionBump(name string) {
	cacheVersionPtr(name).Add(1)
}

func cloneMaps(items []Map) []Map {
	out := make([]Map, 0, len(items))
	for _, item := range items {
		one := Map{}
		for k, v := range item {
			one[k] = v
		}
		out = append(out, one)
	}
	return out
}

func makeCacheKey(base *sqlBase, scope string, q Query) string {
	payload := Map{
		"scope":  scope,
		"name":   scope,
		"select": q.Select,
		"sort":   q.Sort,
		"group":  q.Group,
		"aggs":   q.Aggs,
		"joins":  q.Joins,
		"filter": fmt.Sprintf("%v", q.Filter),
		"limit":  q.Limit,
		"offset": q.Offset,
		"after":  q.After,
	}
	b, _ := json.Marshal(payload)
	return string(b)
}

func (b *sqlBase) cacheEnabled() bool {
	if b == nil || b.inst == nil || b.inst.Config.Setting == nil {
		return false
	}
	raw, ok := b.inst.Config.Setting["cache"]
	if !ok {
		return false
	}
	switch vv := raw.(type) {
	case bool:
		return vv
	case int:
		return vv > 0
	case int64:
		return vv > 0
	case string:
		on, err := strconv.ParseBool(strings.TrimSpace(vv))
		return err == nil && on
	case Map:
		if e, ok := vv["enable"]; ok {
			on, yes := parseBool(e)
			return yes && on
		}
		return true
	default:
		return false
	}
}

func (b *sqlBase) cacheTTL() time.Duration {
	if b == nil || b.inst == nil || b.inst.Config.Setting == nil {
		return 0
	}
	raw, ok := b.inst.Config.Setting["cache"]
	if !ok {
		return 0
	}
	switch vv := raw.(type) {
	case Map:
		if d, ok := vv["ttl"]; ok {
			switch dt := d.(type) {
			case string:
				if parsed, err := time.ParseDuration(dt); err == nil && parsed > 0 {
					return parsed
				}
			case int:
				if dt > 0 {
					return time.Second * time.Duration(dt)
				}
			case int64:
				if dt > 0 {
					return time.Second * time.Duration(dt)
				}
			}
		}
	}
	return 3 * time.Second
}
