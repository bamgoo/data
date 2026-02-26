package data

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bamgoo/bamgoo"
	. "github.com/bamgoo/base"
)

type (
	RangeFunc func(Map) Res
	TxFunc    func(DataBase) error

	PageResult struct {
		Offset int64 `json:"offset"`
		Limit  int64 `json:"limit"`
		Total  int64 `json:"total"`
		Items  []Map `json:"items"`
	}

	DataBase interface {
		Close() error
		Begin() error
		Commit() error
		Rollback() error
		Tx(TxFunc) error
		Migrate(...string) error
		Capabilities() Capabilities

		Table(string) DataTable
		View(string) DataView
		Model(string) DataModel

		Raw(string, ...Any) ([]Map, error)
		Exec(string, ...Any) (int64, error)
	}

	DataTable interface {
		Create(Map) (Map, error)
		CreateMany([]Map) ([]Map, error)
		Upsert(Map, ...Any) (Map, error)
		UpsertMany([]Map, ...Any) ([]Map, error)
		Change(Map, Map) (Map, error)
		Remove(...Any) (Map, error)
		Update(Map, ...Any) (int64, error)
		Delete(...Any) (int64, error)

		Entity(Any) (Map, error)
		Count(...Any) (int64, error)
		Aggregate(...Any) ([]Map, error)
		First(...Any) (Map, error)
		Query(...Any) ([]Map, error)
		Range(RangeFunc, ...Any) Res
		LimitRange(int64, RangeFunc, ...Any) Res
		Limit(offset, limit int64, args ...Any) (int64, []Map, error)
		Page(offset, limit int64, args ...Any) (PageResult, error)
		Group(field string, args ...Any) ([]Map, error)
	}

	DataView interface {
		Count(...Any) (int64, error)
		Aggregate(...Any) ([]Map, error)
		First(...Any) (Map, error)
		Query(...Any) ([]Map, error)
		Range(RangeFunc, ...Any) Res
		LimitRange(int64, RangeFunc, ...Any) Res
		Limit(offset, limit int64, args ...Any) (int64, []Map, error)
		Page(offset, limit int64, args ...Any) (PageResult, error)
		Group(field string, args ...Any) ([]Map, error)
	}

	DataModel interface {
		First(...Any) (Map, error)
		Query(...Any) ([]Map, error)
		Range(RangeFunc, ...Any) Res
		LimitRange(int64, RangeFunc, ...Any) Res
		Limit(offset, limit int64, args ...Any) (int64, []Map, error)
		Page(offset, limit int64, args ...Any) (PageResult, error)
	}
)

type sqlBase struct {
	inst   *Instance
	conn   Connection
	tx     *sql.Tx
	closed bool
}

type invalidDataBase struct {
	err error
}

type invalidTable struct {
	err error
}

func (m *Module) Base(names ...string) DataBase {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	name := bamgoo.DEFAULT
	if len(names) > 0 && strings.TrimSpace(names[0]) != "" {
		name = names[0]
	}
	inst := m.instances[name]
	if inst == nil {
		return &invalidDataBase{err: errInvalidConnection}
	}
	return &sqlBase{inst: inst, conn: inst.conn}
}

func (b *sqlBase) Close() error {
	if b.closed {
		return nil
	}
	if b.tx != nil {
		_ = b.tx.Rollback()
		b.tx = nil
	}
	b.closed = true
	return nil
}

func (b *sqlBase) Begin() error {
	if b.tx != nil {
		return nil
	}
	tx, err := b.conn.DB().BeginTx(context.Background(), nil)
	if err != nil {
		statsFor(b.inst.Name).Errors.Add(1)
		return wrapErr("tx.begin", ErrTxFailed, classifySQLError(err))
	}
	b.tx = tx
	return nil
}

func (b *sqlBase) Commit() error {
	if b.tx == nil {
		return nil
	}
	err := b.tx.Commit()
	b.tx = nil
	if err != nil {
		statsFor(b.inst.Name).Errors.Add(1)
		return wrapErr("tx.commit", ErrTxFailed, classifySQLError(err))
	}
	return nil
}

func (b *sqlBase) Rollback() error {
	if b.tx == nil {
		return nil
	}
	err := b.tx.Rollback()
	b.tx = nil
	if err != nil {
		statsFor(b.inst.Name).Errors.Add(1)
		return wrapErr("tx.rollback", ErrTxFailed, classifySQLError(err))
	}
	return nil
}

func (b *sqlBase) Tx(fn TxFunc) error {
	if fn == nil {
		return nil
	}
	if err := b.Begin(); err != nil {
		return wrapErr("tx.begin", ErrTxFailed, err)
	}
	if err := fn(b); err != nil {
		_ = b.Rollback()
		return wrapErr("tx.run", ErrTxFailed, err)
	}
	if err := b.Commit(); err != nil {
		_ = b.Rollback()
		return wrapErr("tx.commit", ErrTxFailed, err)
	}
	return nil
}

func (b *sqlBase) Capabilities() Capabilities {
	return detectCapabilities(b.conn.Dialect())
}

func (b *sqlBase) Migrate(names ...string) error {
	targets := names
	if len(targets) == 0 {
		targets = make([]string, 0, len(module.tables))
		for name := range module.Tables() {
			targets = append(targets, name)
		}
	}
	for _, name := range targets {
		cfg, err := module.tableConfig(b.inst.Name, name)
		if err != nil {
			continue
		}
		schema := pickSchema(b.inst, cfg.Schema)
		source := pickName(name, cfg.Table)
		if err := b.migrateTable(schema, source, pickKey(cfg.Key), cfg.Fields, cfg.Setting); err != nil {
			return wrapErr("migrate."+name, ErrInvalidQuery, err)
		}
	}
	return nil
}

func (b *sqlBase) Table(name string) DataTable {
	cfg, err := module.tableConfig(b.inst.Name, name)
	if err != nil {
		panic(err)
	}
	return &sqlTable{sqlView: sqlView{base: b, name: name, schema: pickSchema(b.inst, cfg.Schema), source: pickName(name, cfg.Table), key: pickKey(cfg.Key), fields: cfg.Fields}}
}

func (b *sqlBase) View(name string) DataView {
	cfg, err := module.viewConfig(b.inst.Name, name)
	if err != nil {
		panic(err)
	}
	return &sqlView{base: b, name: name, schema: pickSchema(b.inst, cfg.Schema), source: pickName(name, cfg.View), key: pickKey(cfg.Key), fields: cfg.Fields}
}

func (b *sqlBase) Model(name string) DataModel {
	cfg, err := module.modelConfig(b.inst.Name, name)
	if err != nil {
		panic(err)
	}
	return &sqlModel{sqlView: sqlView{base: b, name: name, schema: pickSchema(b.inst, cfg.Schema), source: pickName(name, cfg.Model), key: pickKey(cfg.Key), fields: cfg.Fields}}
}

func pickSchema(inst *Instance, schema string) string {
	if schema != "" {
		return schema
	}
	if inst.Config.Schema != "" {
		return inst.Config.Schema
	}
	return ""
}

func pickName(name, own string) string {
	if own != "" {
		return own
	}
	return strings.ReplaceAll(name, ".", "_")
}

func pickKey(key string) string {
	if key != "" {
		return key
	}
	return "id"
}

func (b *sqlBase) sourceExpr(schema, source string) string {
	d := b.conn.Dialect()
	if schema != "" {
		return d.Quote(schema) + "." + d.Quote(source)
	}
	return d.Quote(source)
}

func (b *sqlBase) currentExec() execer {
	if b.tx != nil {
		return b.tx
	}
	return b.conn.DB()
}

type execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (b *sqlBase) Raw(query string, args ...Any) ([]Map, error) {
	start := time.Now()
	rows, err := b.currentExec().QueryContext(context.Background(), query, toInterfaces(args)...)
	if err != nil {
		statsFor(b.inst.Name).Errors.Add(1)
		return nil, wrapErr("raw.query", ErrInvalidQuery, classifySQLError(err))
	}
	defer rows.Close()
	items, err := scanMaps(rows)
	if err != nil {
		statsFor(b.inst.Name).Errors.Add(1)
		return nil, wrapErr("raw.scan", ErrInvalidQuery, classifySQLError(err))
	}
	statsFor(b.inst.Name).Queries.Add(1)
	b.logSlow(query, args, start)
	return items, nil
}

func (b *sqlBase) Exec(query string, args ...Any) (int64, error) {
	start := time.Now()
	res, err := b.currentExec().ExecContext(context.Background(), query, toInterfaces(args)...)
	if err != nil {
		statsFor(b.inst.Name).Errors.Add(1)
		return 0, wrapErr("exec", ErrInvalidQuery, classifySQLError(err))
	}
	b.logSlow(query, args, start)
	statsFor(b.inst.Name).Writes.Add(1)
	cacheVersionBump(b.inst.Name)
	affected, err := res.RowsAffected()
	if err != nil {
		statsFor(b.inst.Name).Errors.Add(1)
		return 0, wrapErr("exec.rows", ErrInvalidQuery, classifySQLError(err))
	}
	return affected, nil
}

func toInterfaces(args []Any) []any {
	out := make([]any, 0, len(args))
	for _, v := range args {
		out = append(out, v)
	}
	return out
}

func scanMaps(rows *sql.Rows) ([]Map, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	items := make([]Map, 0)
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		item := Map{}
		for i, col := range cols {
			switch v := vals[i].(type) {
			case []byte:
				item[col] = string(v)
			default:
				item[col] = v
			}
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func mapCreate(fields Vars, val Map) (Map, error) {
	if len(fields) == 0 {
		out := Map{}
		for k, v := range val {
			out[k] = v
		}
		return out, nil
	}
	out := Map{}
	res := bamgoo.Mapping(fields, val, out, false, false)
	if res != nil && res.Fail() {
		return nil, errors.New(res.Error())
	}
	return out, nil
}

func mapChange(fields Vars, val Map) (Map, error) {
	if len(fields) == 0 {
		out := Map{}
		for k, v := range val {
			out[k] = v
		}
		return out, nil
	}
	out := Map{}
	res := bamgoo.Mapping(fields, val, out, true, false)
	if res != nil && res.Fail() {
		return nil, wrapErr("map.change", ErrInvalidUpdate, fmt.Errorf("%s", res.Error()))
	}
	return out, nil
}

func (b *sqlBase) migrateTable(schema, table, key string, fields Vars, setting Map) error {
	d := b.conn.Dialect()
	source := b.sourceExpr(schema, table)
	cols := make([]string, 0, len(fields)+1)
	if key == "" {
		key = "id"
	}
	seenKey := false
	for name, field := range fields {
		sqlType := migrateType(d.Name(), field.Type)
		def := d.Quote(name) + " " + sqlType
		if strings.EqualFold(name, key) {
			seenKey = true
			if strings.Contains(strings.ToLower(d.Name()), "sqlite") {
				def = d.Quote(name) + " INTEGER PRIMARY KEY"
			}
		}
		if field.Required && !field.Nullable {
			def += " NOT NULL"
		}
		cols = append(cols, def)
	}
	if !seenKey {
		if strings.Contains(strings.ToLower(d.Name()), "sqlite") {
			cols = append([]string{d.Quote(key) + " INTEGER PRIMARY KEY"}, cols...)
		} else {
			cols = append([]string{d.Quote(key) + " BIGINT"}, cols...)
		}
	}
	if !strings.Contains(strings.ToLower(d.Name()), "sqlite") {
		cols = append(cols, "PRIMARY KEY ("+d.Quote(key)+")")
	}
	sqlText := "CREATE TABLE IF NOT EXISTS " + source + " (" + strings.Join(cols, ",") + ")"
	if _, err := b.currentExec().ExecContext(context.Background(), sqlText); err != nil {
		return err
	}
	return b.migrateIndexes(schema, table, setting)
}

func (b *sqlBase) migrateIndexes(schema, table string, setting Map) error {
	if setting == nil {
		return nil
	}
	raw, ok := setting["indexes"]
	if !ok {
		return nil
	}
	indexes := make([]Map, 0)
	switch vv := raw.(type) {
	case []Map:
		indexes = append(indexes, vv...)
	case []Any:
		for _, one := range vv {
			if m, ok := one.(Map); ok {
				indexes = append(indexes, m)
			}
		}
	case Map:
		indexes = append(indexes, vv)
	}
	if len(indexes) == 0 {
		return nil
	}
	d := b.conn.Dialect()
	for i, idx := range indexes {
		fields := parseStringList(idx["fields"])
		if len(fields) == 0 {
			continue
		}
		name := ""
		if v, ok := idx["name"].(string); ok {
			name = strings.TrimSpace(v)
		}
		if name == "" {
			name = fmt.Sprintf("idx_%s_%d", table, i+1)
		}
		unique, _ := parseBool(idx["unique"])
		parts := make([]string, 0, len(fields))
		for _, f := range fields {
			parts = append(parts, d.Quote(f))
		}
		target := b.sourceExpr(schema, table)
		sqlText := "CREATE "
		if unique {
			sqlText += "UNIQUE "
		}
		sqlText += "INDEX "
		if !strings.Contains(strings.ToLower(d.Name()), "mysql") {
			sqlText += "IF NOT EXISTS "
		}
		sqlText += d.Quote(name) + " ON " + target + " (" + strings.Join(parts, ",") + ")"
		if _, err := b.currentExec().ExecContext(context.Background(), sqlText); err != nil {
			msg := strings.ToLower(err.Error())
			if strings.Contains(strings.ToLower(d.Name()), "mysql") && strings.Contains(msg, "duplicate key name") {
				continue
			}
			if strings.Contains(msg, "already exists") {
				continue
			}
			return err
		}
	}
	return nil
}

func (b *sqlBase) logSlow(query string, args []Any, start time.Time) {
	threshold := time.Duration(0)
	if b.inst != nil && b.inst.Config.Setting != nil {
		if v, ok := b.inst.Config.Setting["slow"]; ok {
			switch vv := v.(type) {
			case int:
				threshold = time.Millisecond * time.Duration(vv)
			case int64:
				threshold = time.Millisecond * time.Duration(vv)
			case float64:
				threshold = time.Millisecond * time.Duration(vv)
			case string:
				if d, err := time.ParseDuration(vv); err == nil {
					threshold = d
				}
			}
		}
	}
	if threshold <= 0 {
		return
	}
	cost := time.Since(start)
	if cost < threshold {
		return
	}
	statsFor(b.inst.Name).Slow.Add(1)
	fmt.Printf("[data][slow] conn=%s dialect=%s cost=%s sql=%s args=%v\n", b.inst.Name, b.conn.Dialect().Name(), cost.String(), query, args)
}

func migrateType(dialect, typ string) string {
	t := strings.ToLower(strings.TrimSpace(typ))
	switch {
	case t == "int" || t == "integer" || t == "int64" || t == "uint" || t == "uint64":
		if strings.Contains(dialect, "sqlite") {
			return "INTEGER"
		}
		return "BIGINT"
	case t == "float" || t == "number" || t == "double" || t == "decimal" || t == "decimal128":
		return "DOUBLE PRECISION"
	case t == "bool":
		return "BOOLEAN"
	case t == "timestamp" || t == "datetime" || t == "date":
		if strings.Contains(dialect, "sqlite") {
			return "DATETIME"
		}
		return "TIMESTAMP"
	case t == "json" || t == "jsonb":
		if strings.Contains(dialect, "mysql") || strings.Contains(dialect, "pgsql") || strings.Contains(dialect, "postgres") {
			return "JSON"
		}
		return "TEXT"
	default:
		return "TEXT"
	}
}

func (b *invalidDataBase) Close() error                       { return nil }
func (b *invalidDataBase) Begin() error                       { return b.err }
func (b *invalidDataBase) Commit() error                      { return b.err }
func (b *invalidDataBase) Rollback() error                    { return b.err }
func (b *invalidDataBase) Tx(TxFunc) error                    { return b.err }
func (b *invalidDataBase) Migrate(...string) error            { return b.err }
func (b *invalidDataBase) Capabilities() Capabilities         { return Capabilities{} }
func (b *invalidDataBase) Table(string) DataTable             { return &invalidTable{err: b.err} }
func (b *invalidDataBase) View(string) DataView               { return &invalidTable{err: b.err} }
func (b *invalidDataBase) Model(string) DataModel             { return &invalidTable{err: b.err} }
func (b *invalidDataBase) Raw(string, ...Any) ([]Map, error)  { return nil, b.err }
func (b *invalidDataBase) Exec(string, ...Any) (int64, error) { return 0, b.err }

func (t *invalidTable) Create(Map) (Map, error)                 { return nil, t.err }
func (t *invalidTable) CreateMany([]Map) ([]Map, error)         { return nil, t.err }
func (t *invalidTable) Upsert(Map, ...Any) (Map, error)         { return nil, t.err }
func (t *invalidTable) UpsertMany([]Map, ...Any) ([]Map, error) { return nil, t.err }
func (t *invalidTable) Change(Map, Map) (Map, error)            { return nil, t.err }
func (t *invalidTable) Remove(...Any) (Map, error)              { return nil, t.err }
func (t *invalidTable) Update(Map, ...Any) (int64, error)       { return 0, t.err }
func (t *invalidTable) Delete(...Any) (int64, error)            { return 0, t.err }
func (t *invalidTable) Entity(Any) (Map, error)                 { return nil, t.err }
func (t *invalidTable) Count(...Any) (int64, error)             { return 0, t.err }
func (t *invalidTable) Aggregate(...Any) ([]Map, error)         { return nil, t.err }
func (t *invalidTable) First(...Any) (Map, error)               { return nil, t.err }
func (t *invalidTable) Query(...Any) ([]Map, error)             { return nil, t.err }
func (t *invalidTable) Range(RangeFunc, ...Any) Res             { return bamgoo.Fail.With(t.err.Error()) }
func (t *invalidTable) LimitRange(int64, RangeFunc, ...Any) Res {
	return bamgoo.Fail.With(t.err.Error())
}
func (t *invalidTable) Limit(int64, int64, ...Any) (int64, []Map, error) { return 0, nil, t.err }
func (t *invalidTable) Page(int64, int64, ...Any) (PageResult, error)    { return PageResult{}, t.err }
func (t *invalidTable) Group(string, ...Any) ([]Map, error)              { return nil, t.err }
