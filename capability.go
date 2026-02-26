package data

import "strings"

func detectCapabilities(d Dialect) Capabilities {
	name := strings.ToLower(strings.TrimSpace(d.Name()))
	caps := Capabilities{
		Dialect:     name,
		ILike:       d.SupportsILike(),
		Returning:   d.SupportsReturning(),
		Join:        true,
		Group:       true,
		Having:      true,
		Aggregate:   true,
		KeysetAfter: true,
	}

	switch name {
	case "pgsql", "postgres":
		caps.JsonContains = true
		caps.ArrayOverlap = true
		caps.JsonElemMatch = true
	case "mysql":
		caps.JsonContains = true
		caps.ArrayOverlap = true
		caps.JsonElemMatch = true
	case "sqlite", "sqlite3":
		caps.JsonContains = true // fallback behavior exists
		caps.ArrayOverlap = false
		caps.JsonElemMatch = false
	default:
		caps.JsonContains = false
		caps.ArrayOverlap = false
		caps.JsonElemMatch = false
	}

	return caps
}
