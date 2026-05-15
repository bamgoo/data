package data

import (
	"database/sql"
	"testing"

	. "github.com/infrago/base"
)

type writeNormalizeTestConn struct{}

type writeNormalizeTestDialect struct{}

func (c *writeNormalizeTestConn) Open() error    { return nil }
func (c *writeNormalizeTestConn) Close() error   { return nil }
func (c *writeNormalizeTestConn) Health() Health { return Health{} }
func (c *writeNormalizeTestConn) DB() *sql.DB    { return nil }
func (c *writeNormalizeTestConn) Dialect() Dialect {
	return writeNormalizeTestDialect{}
}

func (writeNormalizeTestDialect) Name() string            { return "pgsql" }
func (writeNormalizeTestDialect) Quote(s string) string   { return `"` + s + `"` }
func (writeNormalizeTestDialect) Placeholder(int) string  { return "$1" }
func (writeNormalizeTestDialect) SupportsILike() bool     { return true }
func (writeNormalizeTestDialect) SupportsReturning() bool { return true }

func TestNormalizeWriteValueForPostgresArrayField(t *testing.T) {
	table := &sqlTable{
		sqlView: sqlView{
			base: &sqlBase{
				inst: &Instance{Name: "normalize-array"},
				conn: &writeNormalizeTestConn{},
			},
			fields: Vars{
				"roleIds": Var{Type: "[int]"},
			},
		},
	}
	if got := table.normalizeWriteValue("roleIds", []int64{}); got != "{}" {
		t.Fatalf("expected empty postgres array literal, got %#v", got)
	}
	if got := table.normalizeWriteValue("roleIds", []int64{3, 5}); got != "{3,5}" {
		t.Fatalf("expected postgres array literal, got %#v", got)
	}
}

func TestNormalizeWriteValueForPostgresArrayFlag(t *testing.T) {
	table := &sqlTable{
		sqlView: sqlView{
			base: &sqlBase{
				inst: &Instance{Name: "normalize-array-flag"},
				conn: &writeNormalizeTestConn{},
			},
			fields: Vars{
				"tags": Var{
					Type:    "string",
					Setting: Map{"array": true},
				},
			},
		},
	}

	if got := table.normalizeWriteValue("tags", []string{}); got != "{}" {
		t.Fatalf("expected empty postgres array literal by flag, got %#v", got)
	}
}
