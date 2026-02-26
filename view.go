package data

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bamgoo/bamgoo"
	. "github.com/bamgoo/base"
)

type sqlView struct {
	base   *sqlBase
	name   string
	schema string
	source string
	key    string
	fields Vars
}

var sqlPlanCache sync.Map

func (v *sqlView) Count(args ...Any) int64 {
	q, err := ParseQuery(args...)
	if err != nil {
		v.base.setError(wrapErr(v.name+".count.parse", ErrInvalidQuery, err))
		return 0
	}
	if total, ok := v.loadCountCache(q); ok {
		statsFor(v.base.inst.Name).CacheHit.Add(1)
		v.base.setError(nil)
		return total
	}
	builder := NewSQLBuilder(v.base.conn.Dialect())
	from, joins, err := v.buildFrom(q, builder)
	if err != nil {
		statsFor(v.base.inst.Name).Errors.Add(1)
		v.base.setError(wrapErr(v.name+".count.from", ErrInvalidQuery, err))
		return 0
	}
	where, params, err := builder.CompileWhere(q)
	if err != nil {
		statsFor(v.base.inst.Name).Errors.Add(1)
		v.base.setError(wrapErr(v.name+".count.where", ErrInvalidQuery, err))
		return 0
	}
	sql := "SELECT COUNT(1) FROM " + from + joins + " WHERE " + where
	if len(q.Group) > 0 {
		sql = "SELECT COUNT(1) FROM (SELECT 1 FROM " + from + joins + " WHERE " + where + BuildGroupBy(q.Group, v.base.conn.Dialect()) + ") _g"
	}
	start := time.Now()
	var total int64
	err = v.base.currentExec().QueryRowContext(context.Background(), sql, toInterfaces(params)...).Scan(&total)
	if err != nil {
		statsFor(v.base.inst.Name).Errors.Add(1)
		v.base.setError(wrapErr(v.name+".count.query", ErrInvalidQuery, classifySQLError(err)))
		return 0
	}
	v.base.logSlow(sql, params, start)
	statsFor(v.base.inst.Name).Queries.Add(1)
	v.storeCountCache(q, total)
	v.base.setError(nil)
	return total
}

func (v *sqlView) First(args ...Any) Map {
	q, err := ParseQuery(args...)
	if err != nil {
		v.base.setError(wrapErr(v.name+".first.parse", ErrInvalidQuery, err))
		return nil
	}
	q.Limit = 1
	items, err := v.queryWithQuery(q)
	if err != nil {
		v.base.setError(wrapErr(v.name+".first.query", ErrInvalidQuery, err))
		return nil
	}
	if len(items) == 0 {
		v.base.setError(nil)
		return nil
	}
	if len(q.Aggs) > 0 || len(q.Group) > 0 {
		v.base.setError(nil)
		return items[0]
	}
	out, err := v.decode(items[0])
	v.base.setError(err)
	return out
}

func (v *sqlView) Query(args ...Any) []Map {
	q, err := ParseQuery(args...)
	if err != nil {
		v.base.setError(wrapErr(v.name+".query.parse", ErrInvalidQuery, err))
		return nil
	}
	items, err := v.queryWithQuery(q)
	if err != nil {
		v.base.setError(wrapErr(v.name+".query.run", ErrInvalidQuery, err))
		return nil
	}
	if len(q.Aggs) > 0 || len(q.Group) > 0 {
		v.base.setError(nil)
		return items
	}
	out := make([]Map, 0, len(items))
	for _, item := range items {
		dec, err := v.decode(item)
		if err != nil {
			statsFor(v.base.inst.Name).Errors.Add(1)
			v.base.setError(wrapErr(v.name+".query.decode", ErrInvalidQuery, err))
			return nil
		}
		out = append(out, dec)
	}
	v.base.setError(nil)
	return out
}

func (v *sqlView) Aggregate(args ...Any) []Map {
	q, err := ParseQuery(args...)
	if err != nil {
		v.base.setError(wrapErr(v.name+".agg.parse", ErrInvalidQuery, err))
		return nil
	}
	if len(q.Aggs) == 0 {
		// default aggregate: count
		q.Aggs = []Agg{{Alias: "$count", Op: "count", Field: "*"}}
	}
	items, err := v.queryWithQuery(q)
	v.base.setError(err)
	return items
}

func (v *sqlView) Range(next RangeFunc, args ...Any) Res {
	return v.LimitRange(0, next, args...)
}

func (v *sqlView) LimitRange(limit int64, next RangeFunc, args ...Any) Res {
	if next == nil || limit < 0 {
		return bamgoo.Fail
	}

	q, err := ParseQuery(args...)
	if err != nil {
		return bamgoo.Fail.With(err.Error())
	}
	if limit > 0 {
		q.Limit = limit
	}

	res := v.streamWithQuery(q, next)
	if res == nil {
		return bamgoo.OK
	}
	return res
}

func (v *sqlView) Limit(offset, limit int64, args ...Any) (int64, []Map) {
	q, err := ParseQuery(args...)
	if err != nil {
		v.base.setError(wrapErr(v.name+".limit.parse", ErrInvalidQuery, err))
		return 0, nil
	}
	q.Offset = offset
	q.Limit = limit
	total := int64(-1)
	if q.WithCount {
		total = v.Count(args...)
		if v.base.Error() != nil {
			v.base.setError(wrapErr(v.name+".limit.count", ErrInvalidQuery, v.base.Error()))
			return 0, nil
		}
	}
	items := v.Query(withQuery(args, q)...)
	if v.base.Error() != nil {
		v.base.setError(wrapErr(v.name+".limit.query", ErrInvalidQuery, v.base.Error()))
		return 0, nil
	}
	return total, items
}

func (v *sqlView) Page(offset, limit int64, args ...Any) PageResult {
	total, items := v.Limit(offset, limit, args...)
	return PageResult{Offset: offset, Limit: limit, Total: total, Items: items}
}

func withQuery(args []Any, q Query) []Any {
	filtered := make([]Any, 0, len(args)+1)
	merged := Map{}
	for _, a := range args {
		if m, ok := a.(Map); ok {
			for k, v := range m {
				merged[k] = v
			}
		}
	}
	if q.Offset > 0 {
		merged[OptOffset] = q.Offset
	}
	if q.Limit > 0 {
		merged[OptLimit] = q.Limit
	}
	filtered = append(filtered, merged)
	return filtered
}

func (v *sqlView) Group(field string, args ...Any) []Map {
	q, err := ParseQuery(args...)
	if err != nil {
		v.base.setError(wrapErr(v.name+".group.parse", ErrInvalidQuery, err))
		return nil
	}
	q.Group = []string{field}
	builder := NewSQLBuilder(v.base.conn.Dialect())
	from, joins, err := v.buildFrom(q, builder)
	if err != nil {
		v.base.setError(wrapErr(v.name+".group.from", ErrInvalidQuery, err))
		return nil
	}
	where, params, err := builder.CompileWhere(q)
	if err != nil {
		v.base.setError(wrapErr(v.name+".group.where", ErrInvalidQuery, err))
		return nil
	}
	groupField := quoteField(v.base.conn.Dialect(), field)
	sql := "SELECT " + groupField + " AS " + v.base.conn.Dialect().Quote(field) + ", COUNT(1) AS " + v.base.conn.Dialect().Quote("$count") +
		" FROM " + from + joins + " WHERE " + where + BuildGroupBy(q.Group, v.base.conn.Dialect()) + BuildOrderBy(q, v.base.conn.Dialect())
	start := time.Now()
	rows, err := v.base.currentExec().QueryContext(context.Background(), sql, toInterfaces(params)...)
	if err != nil {
		statsFor(v.base.inst.Name).Errors.Add(1)
		v.base.setError(wrapErr(v.name+".group.query", ErrInvalidQuery, classifySQLError(err)))
		return nil
	}
	defer rows.Close()
	items, err := scanMaps(rows)
	if err != nil {
		statsFor(v.base.inst.Name).Errors.Add(1)
		v.base.setError(wrapErr(v.name+".group.scan", ErrInvalidQuery, classifySQLError(err)))
		return nil
	}
	statsFor(v.base.inst.Name).Queries.Add(1)
	v.base.logSlow(sql, params, start)
	v.base.setError(nil)
	return items
}

func (v *sqlView) queryWithQuery(q Query) ([]Map, error) {
	if err := v.applyAfter(&q); err != nil {
		return nil, err
	}
	if items, ok := v.loadQueryCache(q); ok {
		statsFor(v.base.inst.Name).CacheHit.Add(1)
		return items, nil
	}

	builder := NewSQLBuilder(v.base.conn.Dialect())
	from, joins, err := v.buildFrom(q, builder)
	if err != nil {
		return nil, err
	}
	where, params, err := builder.CompileWhere(q)
	if err != nil {
		return nil, err
	}

	selectExpr := "*"
	if len(q.Aggs) > 0 {
		parts := make([]string, 0, len(q.Aggs)+len(q.Select))
		for _, agg := range q.Aggs {
			expr, err := compileAgg(v.base.conn.Dialect(), agg)
			if err != nil {
				return nil, err
			}
			parts = append(parts, expr+" AS "+v.base.conn.Dialect().Quote(agg.Alias))
		}
		for _, field := range q.Select {
			parts = append(parts, quoteField(v.base.conn.Dialect(), field))
		}
		selectExpr = strings.Join(parts, ",")
	} else if len(q.Select) > 0 {
		parts := make([]string, 0, len(q.Select))
		for _, field := range q.Select {
			parts = append(parts, quoteField(v.base.conn.Dialect(), field))
		}
		selectExpr = strings.Join(parts, ",")
	}

	sql := "SELECT " + selectExpr + " FROM " + from + joins + " WHERE " + where
	if len(q.Group) > 0 {
		sql += BuildGroupBy(q.Group, v.base.conn.Dialect())
		if q.Having != nil {
			havingSQL, err := builder.CompileExpr(q.Having)
			if err != nil {
				return nil, err
			}
			params = builder.Args()
			sql += " HAVING " + havingSQL
		}
	}
	sql += BuildOrderBy(q, v.base.conn.Dialect())
	sql += BuildLimitOffset(q, len(params)+1, v.base.conn.Dialect(), &params)

	sql = v.cacheSQL(sql, q)
	start := time.Now()
	rows, err := v.base.currentExec().QueryContext(context.Background(), sql, toInterfaces(params)...)
	if err != nil {
		statsFor(v.base.inst.Name).Errors.Add(1)
		return nil, wrapErr(v.name+".query.db", ErrInvalidQuery, classifySQLError(err))
	}
	defer rows.Close()
	v.base.logSlow(sql, params, start)
	items, err := scanMaps(rows)
	if err != nil {
		statsFor(v.base.inst.Name).Errors.Add(1)
		return nil, wrapErr(v.name+".query.scan", ErrInvalidQuery, classifySQLError(err))
	}
	statsFor(v.base.inst.Name).Queries.Add(1)
	v.storeQueryCache(q, items)
	return items, nil
}

func (v *sqlView) streamWithQuery(q Query, next RangeFunc) Res {
	if err := v.applyAfter(&q); err != nil {
		return bamgoo.Fail.With(err.Error())
	}

	builder := NewSQLBuilder(v.base.conn.Dialect())
	from, joins, err := v.buildFrom(q, builder)
	if err != nil {
		return bamgoo.Fail.With(err.Error())
	}
	where, params, err := builder.CompileWhere(q)
	if err != nil {
		return bamgoo.Fail.With(err.Error())
	}

	selectExpr := "*"
	if len(q.Aggs) > 0 {
		parts := make([]string, 0, len(q.Aggs)+len(q.Select))
		for _, agg := range q.Aggs {
			expr, err := compileAgg(v.base.conn.Dialect(), agg)
			if err != nil {
				return bamgoo.Fail.With(err.Error())
			}
			parts = append(parts, expr+" AS "+v.base.conn.Dialect().Quote(agg.Alias))
		}
		for _, field := range q.Select {
			parts = append(parts, quoteField(v.base.conn.Dialect(), field))
		}
		selectExpr = strings.Join(parts, ",")
	} else if len(q.Select) > 0 {
		parts := make([]string, 0, len(q.Select))
		for _, field := range q.Select {
			parts = append(parts, quoteField(v.base.conn.Dialect(), field))
		}
		selectExpr = strings.Join(parts, ",")
	}

	sqlText := "SELECT " + selectExpr + " FROM " + from + joins + " WHERE " + where
	if len(q.Group) > 0 {
		sqlText += BuildGroupBy(q.Group, v.base.conn.Dialect())
		if q.Having != nil {
			havingSQL, err := builder.CompileExpr(q.Having)
			if err != nil {
				return bamgoo.Fail.With(err.Error())
			}
			params = builder.Args()
			sqlText += " HAVING " + havingSQL
		}
	}
	sqlText += BuildOrderBy(q, v.base.conn.Dialect())
	sqlText += BuildLimitOffset(q, len(params)+1, v.base.conn.Dialect(), &params)

	sqlText = v.cacheSQL(sqlText, q)
	start := time.Now()
	rows, err := v.base.currentExec().QueryContext(context.Background(), sqlText, toInterfaces(params)...)
	if err != nil {
		statsFor(v.base.inst.Name).Errors.Add(1)
		return bamgoo.Fail.With(wrapErr(v.name+".range.query", ErrInvalidQuery, classifySQLError(err)).Error())
	}
	defer rows.Close()
	v.base.logSlow(sqlText, params, start)

	cols, err := rows.Columns()
	if err != nil {
		return bamgoo.Fail.With(err.Error())
	}

	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return bamgoo.Fail.With(err.Error())
		}
		item := Map{}
		for i, col := range cols {
			switch vv := values[i].(type) {
			case []byte:
				item[col] = string(vv)
			default:
				item[col] = vv
			}
		}

		if len(q.Aggs) == 0 && len(q.Group) == 0 {
			decoded, err := v.decode(item)
			if err != nil {
				return bamgoo.Fail.With(err.Error())
			}
			item = decoded
		}

		if res := next(item); res != nil && res.Fail() {
			return res
		}
	}

	if err := rows.Err(); err != nil {
		statsFor(v.base.inst.Name).Errors.Add(1)
		return bamgoo.Fail.With(wrapErr(v.name+".range.rows", ErrInvalidQuery, classifySQLError(err)).Error())
	}
	statsFor(v.base.inst.Name).Queries.Add(1)
	return bamgoo.OK
}

func (v *sqlView) cacheSQL(sqlText string, q Query) string {
	key := q.cacheKey(v.base.conn.Dialect().Name(), v.name)
	if key == "" {
		return sqlText
	}
	sqlPlanCache.Store(key, sqlText)
	if cached, ok := sqlPlanCache.Load(key); ok {
		if s, ok := cached.(string); ok {
			return s
		}
	}
	return sqlText
}

func (q Query) cacheKey(dialect, name string) string {
	keys := make([]string, 0, len(q.Select))
	keys = append(keys, q.Select...)
	sort.Strings(keys)
	return fmt.Sprintf("%s|%s|%v|%v|%v|%v|%d|%d|%t", dialect, name, keys, q.Sort, q.Group, q.Aggs, q.Limit, q.Offset, q.WithCount)
}

func (v *sqlView) buildFrom(q Query, builder *SQLBuilder) (string, string, error) {
	from := v.base.sourceExpr(v.schema, v.source)
	if len(q.Joins) == 0 {
		return from, "", nil
	}
	parts := make([]string, 0, len(q.Joins))
	aliasSet := map[string]struct{}{}
	aliasSet[strings.ToLower(strings.TrimSpace(v.source))] = struct{}{}
	aliasSet[strings.ToLower(strings.TrimSpace(v.name))] = struct{}{}
	for _, j := range q.Joins {
		alias := j.Alias
		if alias == "" {
			alias = j.From
		}
		alias = strings.TrimSpace(alias)
		if alias == "" {
			return "", "", fmt.Errorf("join %s invalid alias", j.From)
		}
		if _, exists := aliasSet[strings.ToLower(alias)]; exists {
			return "", "", fmt.Errorf("join alias duplicated: %s", alias)
		}
		aliasSet[strings.ToLower(alias)] = struct{}{}
		joinType := strings.ToUpper(strings.TrimSpace(j.Type))
		if joinType == "" {
			joinType = "LEFT"
		}
		switch joinType {
		case "LEFT", "RIGHT", "INNER", "FULL":
		default:
			return "", "", fmt.Errorf("join %s invalid type: %s", j.From, joinType)
		}
		on := ""
		if j.LocalField != "" && j.ForeignField != "" {
			on = fmt.Sprintf("%s = %s", quoteField(v.base.conn.Dialect(), j.LocalField), quoteField(v.base.conn.Dialect(), j.ForeignField))
		} else if j.On != nil {
			onSQL, err := builder.CompileExpr(j.On)
			if err != nil {
				return "", "", err
			}
			on = onSQL
		}
		if on == "" {
			return "", "", fmt.Errorf("join %s missing ON condition", j.From)
		}
		part := fmt.Sprintf(" %s JOIN %s", joinType, v.base.sourceExpr("", j.From))
		if alias != j.From {
			part += " AS " + v.base.conn.Dialect().Quote(alias)
		}
		part += " ON " + on
		parts = append(parts, part)
	}
	return from, strings.Join(parts, ""), nil
}

func (v *sqlView) decode(item Map) (Map, error) {
	if len(v.fields) == 0 {
		return item, nil
	}
	out := Map{}
	res := bamgoo.Mapping(v.fields, item, out, false, true)
	if res != nil && res.Fail() {
		return nil, fmt.Errorf("decode %s failed: %s", v.name, res.Error())
	}
	return out, nil
}

func (v *sqlView) loadQueryCache(q Query) ([]Map, bool) {
	if !v.base.cacheEnabled() {
		return nil, false
	}
	token := cacheToken(v.base.inst.Name, v.cacheTables(q))
	key := fmt.Sprintf("q:%s:%s", token, makeCacheKey(v.base, v.name, q))
	raw, ok := cacheMap(v.base.inst.Name).Load(key)
	if !ok {
		return nil, false
	}
	cv, ok := raw.(cacheValue)
	if !ok {
		return nil, false
	}
	if cv.expireAt > 0 && time.Now().UnixNano() > cv.expireAt {
		cacheMap(v.base.inst.Name).Delete(key)
		return nil, false
	}
	return cloneMaps(cv.items), true
}

func (v *sqlView) storeQueryCache(q Query, items []Map) {
	if !v.base.cacheEnabled() {
		return
	}
	ttl := v.base.cacheTTL()
	if ttl <= 0 {
		return
	}
	token := cacheToken(v.base.inst.Name, v.cacheTables(q))
	key := fmt.Sprintf("q:%s:%s", token, makeCacheKey(v.base, v.name, q))
	cacheMap(v.base.inst.Name).Store(key, cacheValue{
		expireAt: time.Now().Add(ttl).UnixNano(),
		items:    cloneMaps(items),
		total:    -1,
	})
}

func (v *sqlView) loadCountCache(q Query) (int64, bool) {
	if !v.base.cacheEnabled() {
		return 0, false
	}
	token := cacheToken(v.base.inst.Name, v.cacheTables(q))
	key := fmt.Sprintf("c:%s:%s", token, makeCacheKey(v.base, v.name, q))
	raw, ok := cacheMap(v.base.inst.Name).Load(key)
	if !ok {
		return 0, false
	}
	cv, ok := raw.(cacheValue)
	if !ok {
		return 0, false
	}
	if cv.expireAt > 0 && time.Now().UnixNano() > cv.expireAt {
		cacheMap(v.base.inst.Name).Delete(key)
		return 0, false
	}
	return cv.total, true
}

func (v *sqlView) storeCountCache(q Query, total int64) {
	if !v.base.cacheEnabled() {
		return
	}
	ttl := v.base.cacheTTL()
	if ttl <= 0 {
		return
	}
	token := cacheToken(v.base.inst.Name, v.cacheTables(q))
	key := fmt.Sprintf("c:%s:%s", token, makeCacheKey(v.base, v.name, q))
	cacheMap(v.base.inst.Name).Store(key, cacheValue{
		expireAt: time.Now().Add(ttl).UnixNano(),
		total:    total,
	})
}

func (v *sqlView) cacheTables(q Query) []string {
	out := make([]string, 0, len(q.Joins)+1)
	out = append(out, v.source)
	for _, join := range q.Joins {
		if strings.TrimSpace(join.From) != "" {
			out = append(out, join.From)
		}
	}
	return out
}

func (v *sqlView) applyAfter(q *Query) error {
	if len(q.After) == 0 {
		return nil
	}
	if len(q.Sort) == 0 {
		return fmt.Errorf("$after requires $sort")
	}
	sf := q.Sort[0]
	value, ok := q.After[sf.Field]
	if !ok {
		value, ok = q.After["$value"]
	}
	if !ok {
		return fmt.Errorf("$after missing value for sort field %s", sf.Field)
	}
	op := OpGt
	if sf.Desc {
		op = OpLt
	}
	afterExpr := CmpExpr{Field: sf.Field, Op: op, Value: value}
	if q.Filter == nil {
		q.Filter = afterExpr
		return nil
	}
	if _, ok := q.Filter.(TrueExpr); ok {
		q.Filter = afterExpr
		return nil
	}
	q.Filter = AndExpr{Items: []Expr{q.Filter, afterExpr}}
	return nil
}

func compileAgg(d Dialect, agg Agg) (string, error) {
	op := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(agg.Op)), "$")
	field := strings.TrimSpace(agg.Field)
	if field == "" {
		field = "*"
	}
	var fn string
	switch op {
	case "count":
		fn = "COUNT"
		if field == "*" {
			return fn + "(1)", nil
		}
	case "sum":
		fn = "SUM"
	case "avg":
		fn = "AVG"
	case "min":
		fn = "MIN"
	case "max":
		fn = "MAX"
	default:
		return "", fmt.Errorf("unsupported agg op %s", agg.Op)
	}
	return fn + "(" + quoteField(d, field) + ")", nil
}
