package data

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	xbase "github.com/infrago/base"
	"github.com/infrago/infra"
)

type txContextTestDriver struct{}

type txContextTestConn struct{}

type txContextTestTx struct{}

type txContextTestConnection struct {
	db *sql.DB
}

type txContextTestDialect struct{}

var (
	txContextBeginCount    atomic.Int32
	txContextCommitCount   atomic.Int32
	txContextRollbackCount atomic.Int32
)

func (d *txContextTestDriver) Open(string) (driver.Conn, error) {
	return &txContextTestConn{}, nil
}

func (c *txContextTestConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (c *txContextTestConn) Close() error                        { return nil }
func (c *txContextTestConn) Begin() (driver.Tx, error) {
	txContextBeginCount.Add(1)
	return &txContextTestTx{}, nil
}

func (c *txContextTestConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	txContextBeginCount.Add(1)
	return &txContextTestTx{}, nil
}

func (tx *txContextTestTx) Commit() error {
	txContextCommitCount.Add(1)
	return nil
}
func (tx *txContextTestTx) Rollback() error {
	txContextRollbackCount.Add(1)
	return nil
}

func (c *txContextTestConnection) Open() error    { return nil }
func (c *txContextTestConnection) Close() error   { return c.db.Close() }
func (c *txContextTestConnection) Health() Health { return Health{} }
func (c *txContextTestConnection) DB() *sql.DB    { return c.db }
func (c *txContextTestConnection) Dialect() Dialect {
	return txContextTestDialect{}
}

func (txContextTestDialect) Name() string            { return "test" }
func (txContextTestDialect) Quote(s string) string   { return s }
func (txContextTestDialect) Placeholder(int) string  { return "?" }
func (txContextTestDialect) SupportsILike() bool     { return false }
func (txContextTestDialect) SupportsReturning() bool { return false }

var registerTxContextTestDriver sync.Once

func resetTxContextCounts() {
	txContextBeginCount.Store(0)
	txContextCommitCount.Store(0)
	txContextRollbackCount.Store(0)
}

func openTxContextTestDB(t *testing.T) *sql.DB {
	t.Helper()
	registerTxContextTestDriver.Do(func() {
		sql.Register("tx-context-test", &txContextTestDriver{})
	})
	db, err := sql.Open("tx-context-test", "")
	if err != nil {
		t.Fatalf("open test db failed: %v", err)
	}
	return db
}

func TestBeginCommitKeepsTransactionContextAlive(t *testing.T) {
	resetTxContextCounts()
	db := openTxContextTestDB(t)
	defer db.Close()

	base := &sqlBase{
		inst: &Instance{Name: "tx-context"},
		conn: &txContextTestConnection{db: db},
	}

	if err := base.Begin(); err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	if base.tx == nil {
		t.Fatalf("expected tx to be created")
	}
	if base.txDone == nil {
		t.Fatalf("expected tx cancel func to be retained until commit")
	}

	if err := base.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	if base.tx != nil {
		t.Fatalf("expected tx to be cleared after commit")
	}
	if base.txDone != nil {
		t.Fatalf("expected tx cancel func to be cleared after commit")
	}
}

func TestNestedTxReusesOuterTransaction(t *testing.T) {
	resetTxContextCounts()
	db := openTxContextTestDB(t)
	defer db.Close()

	base := &sqlBase{
		inst: &Instance{Name: "tx-nested"},
		conn: &txContextTestConnection{db: db},
	}

	res := base.Tx(func(db DataBase) xbase.Res {
		if got := txContextBeginCount.Load(); got != 1 {
			return infra.Fail.With(fmt.Sprintf("expected one tx begin, got %d", got))
		}
		if base.tx == nil {
			return infra.Fail.With("expected outer tx to be active")
		}

		if res := db.Tx(func(inner DataBase) xbase.Res {
			if got := txContextBeginCount.Load(); got != 1 {
				return infra.Fail.With(fmt.Sprintf("nested tx should not begin a new transaction, got %d begins", got))
			}
			if got := txContextCommitCount.Load(); got != 0 {
				return infra.Fail.With(fmt.Sprintf("nested tx should not commit outer transaction, got %d commits", got))
			}
			if base.tx == nil {
				return infra.Fail.With("expected outer tx to remain active inside nested tx")
			}
			return infra.OK
		}); res.Fail() {
			return res
		}

		if got := txContextCommitCount.Load(); got != 0 {
			return infra.Fail.With(fmt.Sprintf("outer tx should not be committed before callback returns, got %d commits", got))
		}
		if base.tx == nil {
			return infra.Fail.With("expected outer tx to remain active after nested tx")
		}
		return infra.OK
	})
	if res.Fail() {
		t.Fatalf("nested tx failed: %v", res)
	}
	if got := txContextBeginCount.Load(); got != 1 {
		t.Fatalf("expected one begin, got %d", got)
	}
	if got := txContextCommitCount.Load(); got != 1 {
		t.Fatalf("expected one final commit, got %d", got)
	}
	if got := txContextRollbackCount.Load(); got != 0 {
		t.Fatalf("expected no rollback, got %d", got)
	}
}

func TestTxRollsBackWhenInnerSetsBaseError(t *testing.T) {
	resetTxContextCounts()
	db := openTxContextTestDB(t)
	defer db.Close()

	base := &sqlBase{
		inst: &Instance{Name: "tx-error"},
		conn: &txContextTestConnection{db: db},
	}

	res := base.Tx(func(db DataBase) xbase.Res {
		return db.Tx(func(inner DataBase) xbase.Res {
			base.setError(errors.New("boom"))
			return infra.OK
		})
	})
	if !res.Fail() {
		t.Fatalf("expected tx error when inner tx leaves base error")
	}
	if got := txContextCommitCount.Load(); got != 0 {
		t.Fatalf("expected no commit on tx error, got %d", got)
	}
	if got := txContextRollbackCount.Load(); got != 1 {
		t.Fatalf("expected one rollback on tx error, got %d", got)
	}
}
