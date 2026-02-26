package data

import "database/sql"

type (
	Driver interface {
		Connect(*Instance) (Connection, error)
	}

	Connection interface {
		Open() error
		Close() error
		Health() Health
		DB() *sql.DB
		Dialect() Dialect
	}

	Health struct {
		Workload int64
	}

	Capabilities struct {
		Dialect       string `json:"dialect"`
		ILike         bool   `json:"ilike"`
		Returning     bool   `json:"returning"`
		Join          bool   `json:"join"`
		Group         bool   `json:"group"`
		Having        bool   `json:"having"`
		Aggregate     bool   `json:"aggregate"`
		KeysetAfter   bool   `json:"keyset_after"`
		JsonContains  bool   `json:"json_contains"`
		ArrayOverlap  bool   `json:"array_overlap"`
		JsonElemMatch bool   `json:"json_elem_match"`
	}

	Dialect interface {
		Name() string
		Quote(string) string
		Placeholder(int) string
		SupportsILike() bool
		SupportsReturning() bool
	}
)
