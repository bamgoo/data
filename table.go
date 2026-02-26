package data

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	. "github.com/bamgoo/base"
)

type sqlTable struct {
	sqlView
}

func (t *sqlTable) Create(data Map) (Map, error) {
	val, err := mapCreate(t.fields, data)
	if err != nil {
		return nil, wrapErr(t.name+".create.map", ErrInvalidUpdate, err)
	}
	val = t.normalizeWriteMap(val)
	if len(val) == 0 {
		return nil, wrapErr(t.name+".create.empty", ErrInvalidUpdate, fmt.Errorf("empty create data"))
	}

	d := t.base.conn.Dialect()
	keys := make([]string, 0, len(val))
	vals := make([]Any, 0, len(val))
	placeholders := make([]string, 0, len(val))
	for k, v := range val {
		if k == t.key && v == nil {
			continue
		}
		keys = append(keys, d.Quote(k))
		vals = append(vals, v)
		placeholders = append(placeholders, d.Placeholder(len(vals)))
	}

	source := t.base.sourceExpr(t.schema, t.source)
	sqlText := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", source, strings.Join(keys, ","), strings.Join(placeholders, ","))

	if d.SupportsReturning() {
		sqlText += " RETURNING " + d.Quote(t.key)
		var id any
		if err := t.base.currentExec().QueryRowContext(context.Background(), sqlText, toInterfaces(vals)...).Scan(&id); err != nil {
			statsFor(t.base.inst.Name).Errors.Add(1)
			return nil, wrapErr(t.name+".create.return", ErrInvalidUpdate, classifySQLError(err))
		}
		val[t.key] = id
	} else {
		res, err := t.base.currentExec().ExecContext(context.Background(), sqlText, toInterfaces(vals)...)
		if err != nil {
			statsFor(t.base.inst.Name).Errors.Add(1)
			return nil, wrapErr(t.name+".create.exec", ErrInvalidUpdate, classifySQLError(err))
		}
		if id, err := res.LastInsertId(); err == nil {
			val[t.key] = id
		}
	}
	statsFor(t.base.inst.Name).Writes.Add(1)
	cacheVersionBump(t.base.inst.Name)

	return val, nil
}

func (t *sqlTable) CreateMany(items []Map) ([]Map, error) {
	if len(items) == 0 {
		return []Map{}, nil
	}
	out := make([]Map, 0, len(items))
	err := t.base.Tx(func(db DataBase) error {
		tb := db.Table(t.name)
		for _, item := range items {
			one, err := tb.Create(item)
			if err != nil {
				return wrapErr(t.name+".createMany.item", ErrInvalidUpdate, err)
			}
			out = append(out, one)
		}
		return nil
	})
	return out, wrapErr(t.name+".createMany", ErrInvalidUpdate, err)
}

func (t *sqlTable) Upsert(data Map, args ...Any) (Map, error) {
	condition := Map{}
	if len(args) > 0 {
		if m, ok := args[0].(Map); ok {
			for k, v := range m {
				condition[k] = v
			}
		}
	}
	if len(condition) == 0 {
		if id, ok := data[t.key]; ok && id != nil {
			condition[t.key] = id
		}
	}
	if len(condition) == 0 {
		return t.Create(data)
	}

	createData := t.upsertCreateData(data, condition)
	if item, err := t.upsertNative(createData, condition); err == nil && item != nil {
		return item, nil
	}

	item, err := t.First(condition)
	if err != nil {
		return nil, wrapErr(t.name+".upsert.first", ErrInvalidQuery, err)
	}
	if item == nil {
		return t.Create(createData)
	}
	return t.Change(item, data)
}

func (t *sqlTable) UpsertMany(items []Map, args ...Any) ([]Map, error) {
	if len(items) == 0 {
		return []Map{}, nil
	}
	out := make([]Map, 0, len(items))
	err := t.base.Tx(func(db DataBase) error {
		tb := db.Table(t.name)
		for _, item := range items {
			one, err := tb.Upsert(item, args...)
			if err != nil {
				return wrapErr(t.name+".upsertMany.item", ErrInvalidUpdate, err)
			}
			out = append(out, one)
		}
		return nil
	})
	return out, wrapErr(t.name+".upsertMany", ErrInvalidUpdate, err)
}

func (t *sqlTable) Change(item Map, data Map) (Map, error) {
	if item == nil || item[t.key] == nil {
		return nil, wrapErr(t.name+".change.key", ErrInvalidUpdate, fmt.Errorf("missing primary key %s", t.key))
	}

	assigns, vals, err := t.compileAssignments(data, item, 1)
	if err != nil {
		return nil, wrapErr(t.name+".change.assign", ErrInvalidUpdate, err)
	}
	if len(assigns) == 0 {
		return item, nil
	}

	d := t.base.conn.Dialect()
	vals = append(vals, item[t.key])

	sqlText := fmt.Sprintf("UPDATE %s SET %s WHERE %s = %s", t.base.sourceExpr(t.schema, t.source), strings.Join(assigns, ","), d.Quote(t.key), d.Placeholder(len(vals)))
	if _, err := t.base.currentExec().ExecContext(context.Background(), sqlText, toInterfaces(vals)...); err != nil {
		statsFor(t.base.inst.Name).Errors.Add(1)
		return nil, wrapErr(t.name+".change.exec", ErrInvalidUpdate, classifySQLError(err))
	}
	statsFor(t.base.inst.Name).Writes.Add(1)
	cacheVersionBump(t.base.inst.Name)

	out := Map{}
	for k, v := range item {
		out[k] = v
	}
	setMap, _, _, _, _ := t.parseUpdateData(data, item, true)
	for k, v := range setMap {
		out[k] = v
	}
	return out, nil
}

func (t *sqlTable) Remove(args ...Any) (Map, error) {
	item, err := t.First(args...)
	if err != nil || item == nil {
		if err != nil {
			return nil, wrapErr(t.name+".remove.first", ErrNotFound, err)
		}
		return nil, wrapErr(t.name+".remove.first", ErrNotFound, ErrNotFound)
	}
	d := t.base.conn.Dialect()
	sqlText := fmt.Sprintf("DELETE FROM %s WHERE %s = %s", t.base.sourceExpr(t.schema, t.source), d.Quote(t.key), d.Placeholder(1))
	if _, err := t.base.currentExec().ExecContext(context.Background(), sqlText, item[t.key]); err != nil {
		statsFor(t.base.inst.Name).Errors.Add(1)
		return nil, wrapErr(t.name+".remove.exec", ErrInvalidQuery, classifySQLError(err))
	}
	statsFor(t.base.inst.Name).Writes.Add(1)
	cacheVersionBump(t.base.inst.Name)
	return item, nil
}

func (t *sqlTable) Update(sets Map, args ...Any) (int64, error) {
	assign, vals, err := t.compileAssignments(sets, nil, 1)
	if err != nil {
		return 0, wrapErr(t.name+".update.assign", ErrInvalidUpdate, err)
	}
	if len(assign) == 0 {
		return 0, nil
	}

	q, err := ParseQuery(args...)
	if err != nil {
		return 0, wrapErr(t.name+".update.parse", ErrInvalidQuery, err)
	}
	d := t.base.conn.Dialect()

	builder := NewSQLBuilder(d)
	builder.index = len(vals) + 1
	where, p, err := builder.CompileWhere(q)
	if err != nil {
		return 0, wrapErr(t.name+".update.where", ErrInvalidQuery, err)
	}
	for _, arg := range p {
		vals = append(vals, arg)
	}

	sqlText := fmt.Sprintf("UPDATE %s SET %s WHERE %s", t.base.sourceExpr(t.schema, t.source), strings.Join(assign, ","), where)
	res, err := t.base.currentExec().ExecContext(context.Background(), sqlText, toInterfaces(vals)...)
	if err != nil {
		statsFor(t.base.inst.Name).Errors.Add(1)
		return 0, wrapErr(t.name+".update.exec", ErrInvalidUpdate, classifySQLError(err))
	}
	statsFor(t.base.inst.Name).Writes.Add(1)
	cacheVersionBump(t.base.inst.Name)
	affected, err := res.RowsAffected()
	if err != nil {
		statsFor(t.base.inst.Name).Errors.Add(1)
		return 0, wrapErr(t.name+".update.rows", ErrInvalidQuery, classifySQLError(err))
	}
	return affected, nil
}

func (t *sqlTable) Delete(args ...Any) (int64, error) {
	q, err := ParseQuery(args...)
	if err != nil {
		return 0, wrapErr(t.name+".delete.parse", ErrInvalidQuery, err)
	}
	b := NewSQLBuilder(t.base.conn.Dialect())
	where, params, err := b.CompileWhere(q)
	if err != nil {
		return 0, wrapErr(t.name+".delete.where", ErrInvalidQuery, err)
	}
	sqlText := fmt.Sprintf("DELETE FROM %s WHERE %s", t.base.sourceExpr(t.schema, t.source), where)
	res, err := t.base.currentExec().ExecContext(context.Background(), sqlText, toInterfaces(params)...)
	if err != nil {
		statsFor(t.base.inst.Name).Errors.Add(1)
		return 0, wrapErr(t.name+".delete.exec", ErrInvalidQuery, classifySQLError(err))
	}
	statsFor(t.base.inst.Name).Writes.Add(1)
	cacheVersionBump(t.base.inst.Name)
	affected, err := res.RowsAffected()
	if err != nil {
		statsFor(t.base.inst.Name).Errors.Add(1)
		return 0, wrapErr(t.name+".delete.rows", ErrInvalidQuery, classifySQLError(err))
	}
	return affected, nil
}

func (t *sqlTable) Entity(id Any) (Map, error) {
	return t.First(Map{t.key: id})
}

func (t *sqlTable) Page(offset, limit int64, args ...Any) (PageResult, error) {
	return t.sqlView.Page(offset, limit, args...)
}

func (t *sqlTable) compileAssignments(input Map, current Map, start int) ([]string, []Any, error) {
	d := t.base.conn.Dialect()
	setPart, incPart, unsetPart, pathPart, err := t.parseUpdateData(input, current, current != nil)
	if err != nil {
		return nil, nil, err
	}

	setVal, err := mapChange(t.fields, setPart)
	if err != nil {
		return nil, nil, err
	}
	setVal = t.normalizeWriteMap(setVal)

	clauses := make([]string, 0, len(setVal)+len(incPart)+len(unsetPart)+len(pathPart))
	vals := make([]Any, 0, len(setVal)+len(incPart)+len(pathPart)*2)
	placeholderIdx := start

	for k, v := range setVal {
		if k == t.key {
			continue
		}
		vals = append(vals, v)
		clauses = append(clauses, d.Quote(k)+" = "+d.Placeholder(placeholderIdx))
		placeholderIdx++
	}

	for k, v := range incPart {
		if k == t.key {
			continue
		}
		vals = append(vals, v)
		f := d.Quote(k)
		clauses = append(clauses, f+" = COALESCE("+f+",0) + "+d.Placeholder(placeholderIdx))
		placeholderIdx++
	}

	for _, k := range unsetPart {
		if k == t.key {
			continue
		}
		clauses = append(clauses, d.Quote(k)+" = NULL")
	}

	for path, value := range pathPart {
		parts := strings.Split(path, ".")
		if len(parts) < 2 {
			continue
		}
		field := strings.TrimSpace(parts[0])
		if field == "" || field == t.key {
			continue
		}
		segments := make([]string, 0, len(parts)-1)
		for _, seg := range parts[1:] {
			seg = strings.TrimSpace(seg)
			if seg != "" {
				segments = append(segments, seg)
			}
		}
		if len(segments) == 0 {
			continue
		}
		raw, err := json.Marshal(value)
		if err != nil {
			return nil, nil, err
		}
		fieldExpr := d.Quote(field)
		name := strings.ToLower(d.Name())
		switch {
		case name == "pgsql" || name == "postgres":
			vals = append(vals, "{"+strings.Join(segments, ",")+"}", string(raw))
			pathPH := d.Placeholder(placeholderIdx)
			placeholderIdx++
			valPH := d.Placeholder(placeholderIdx)
			placeholderIdx++
			clauses = append(clauses, fieldExpr+" = jsonb_set(COALESCE("+fieldExpr+", '{}'::jsonb), "+pathPH+"::text[], "+valPH+"::jsonb, true)")
		case name == "mysql":
			vals = append(vals, "$."+strings.Join(segments, "."), string(raw))
			pathPH := d.Placeholder(placeholderIdx)
			placeholderIdx++
			valPH := d.Placeholder(placeholderIdx)
			placeholderIdx++
			clauses = append(clauses, fieldExpr+" = JSON_SET(COALESCE("+fieldExpr+", JSON_OBJECT()), "+pathPH+", CAST("+valPH+" AS JSON))")
		case name == "sqlite":
			vals = append(vals, "$."+strings.Join(segments, "."), string(raw))
			pathPH := d.Placeholder(placeholderIdx)
			placeholderIdx++
			valPH := d.Placeholder(placeholderIdx)
			placeholderIdx++
			clauses = append(clauses, fieldExpr+" = json_set(COALESCE("+fieldExpr+", '{}'), "+pathPH+", json("+valPH+"))")
		default:
			return nil, nil, wrapErr("update.path", ErrUnsupported, fmt.Errorf("dialect %s does not support %s", d.Name(), UpdSetPath))
		}
	}

	return clauses, vals, nil
}

func (t *sqlTable) parseUpdateData(input Map, current Map, allowCollection bool) (Map, Map, []string, Map, error) {
	setPart := Map{}
	incPart := Map{}
	unsetPart := make([]string, 0)
	pathPart := Map{}
	hasCollection := false

	for k, v := range input {
		switch k {
		case UpdSet:
			if m, ok := v.(Map); ok {
				for kk, vv := range m {
					setPart[kk] = vv
				}
			}
		case UpdInc:
			if m, ok := v.(Map); ok {
				for kk, vv := range m {
					incPart[kk] = vv
				}
			}
		case UpdUnset:
			switch vv := v.(type) {
			case string:
				if strings.TrimSpace(vv) != "" {
					unsetPart = append(unsetPart, strings.TrimSpace(vv))
				}
			case []string:
				for _, field := range vv {
					field = strings.TrimSpace(field)
					if field != "" {
						unsetPart = append(unsetPart, field)
					}
				}
			case []Any:
				for _, item := range vv {
					if field, ok := item.(string); ok {
						field = strings.TrimSpace(field)
						if field != "" {
							unsetPart = append(unsetPart, field)
						}
					}
				}
			case Map:
				for field, on := range vv {
					if yes, ok := parseBool(on); ok && yes {
						unsetPart = append(unsetPart, field)
					}
				}
			}
		case UpdSetPath:
			if m, ok := v.(Map); ok {
				for path, value := range m {
					path = strings.TrimSpace(path)
					if path != "" {
						pathPart[path] = value
					}
				}
			}
		case UpdPush, UpdPull, UpdAddToSet:
			hasCollection = true
		default:
			if !strings.HasPrefix(k, "$") {
				setPart[k] = v
			}
		}
	}

	if hasCollection && !allowCollection {
		return nil, nil, nil, nil, wrapErr("update.collection", ErrUnsupported, fmt.Errorf("%s/%s/%s needs entity context", UpdPush, UpdPull, UpdAddToSet))
	}

	setPart = t.applyCollectionUpdates(setPart, current, input)
	return setPart, incPart, unsetPart, pathPart, nil
}

func (t *sqlTable) applyCollectionUpdates(setPart Map, current Map, input Map) Map {
	if raw, ok := input[UpdPush].(Map); ok {
		for field, value := range raw {
			base := setPart[field]
			if base == nil && current != nil {
				base = current[field]
			}
			setPart[field] = collectionAppend(base, value, false)
		}
	}
	if raw, ok := input[UpdAddToSet].(Map); ok {
		for field, value := range raw {
			base := setPart[field]
			if base == nil && current != nil {
				base = current[field]
			}
			setPart[field] = collectionAppend(base, value, true)
		}
	}
	if raw, ok := input[UpdPull].(Map); ok {
		for field, value := range raw {
			base := setPart[field]
			if base == nil && current != nil {
				base = current[field]
			}
			setPart[field] = collectionRemove(base, value)
		}
	}
	return setPart
}

func (t *sqlTable) normalizeWriteMap(input Map) Map {
	out := Map{}
	for k, v := range input {
		out[k] = t.normalizeWriteValue(k, v)
	}
	return out
}

func (t *sqlTable) normalizeWriteValue(field string, value Any) Any {
	cfg, hasField := t.fields[field]
	typeName := ""
	if hasField {
		typeName = strings.ToLower(strings.TrimSpace(cfg.Type))
	}
	dialect := strings.ToLower(t.base.conn.Dialect().Name())

	if typeName == "json" || typeName == "jsonb" || strings.HasPrefix(typeName, "map") {
		if b, err := json.Marshal(value); err == nil {
			return string(b)
		}
	}

	if strings.HasPrefix(typeName, "array") || strings.HasPrefix(typeName, "[]") {
		if dialect == "pgsql" || dialect == "postgres" {
			return pgArrayLiteral(value)
		}
		if b, err := json.Marshal(value); err == nil {
			return string(b)
		}
	}

	// Default: marshal map/slice into JSON string for portability.
	switch value.(type) {
	case Map, []Map, []Any, []string, []int, []int64, []float64, []float32, []bool:
		if b, err := json.Marshal(value); err == nil {
			return string(b)
		}
	}

	return value
}

func collectionAppend(current Any, appendVal Any, unique bool) Any {
	arr := toAnySlice(current)
	for _, v := range toAnySlice(appendVal) {
		if unique && containsAny(arr, v) {
			continue
		}
		arr = append(arr, v)
	}
	return arr
}

func collectionRemove(current Any, removeVal Any) Any {
	arr := toAnySlice(current)
	remove := toAnySlice(removeVal)
	out := make([]Any, 0, len(arr))
	for _, item := range arr {
		if containsAny(remove, item) {
			continue
		}
		out = append(out, item)
	}
	return out
}

func containsAny(items []Any, value Any) bool {
	target := fmt.Sprintf("%v", value)
	for _, item := range items {
		if fmt.Sprintf("%v", item) == target {
			return true
		}
	}
	return false
}

func toAnySlice(v Any) []Any {
	switch vv := v.(type) {
	case nil:
		return []Any{}
	case []Any:
		return vv
	case []string:
		out := make([]Any, 0, len(vv))
		for _, one := range vv {
			out = append(out, one)
		}
		return out
	case []int:
		out := make([]Any, 0, len(vv))
		for _, one := range vv {
			out = append(out, one)
		}
		return out
	case []int64:
		out := make([]Any, 0, len(vv))
		for _, one := range vv {
			out = append(out, one)
		}
		return out
	case []float64:
		out := make([]Any, 0, len(vv))
		for _, one := range vv {
			out = append(out, one)
		}
		return out
	case []Map:
		out := make([]Any, 0, len(vv))
		for _, one := range vv {
			out = append(out, one)
		}
		return out
	default:
		return []Any{v}
	}
}

func (t *sqlTable) upsertNative(data Map, condition Map) (Map, error) {
	if len(condition) != 1 {
		return nil, nil
	}
	d := t.base.conn.Dialect()
	name := strings.ToLower(d.Name())
	if name != "pgsql" && name != "postgres" && name != "mysql" && name != "sqlite" {
		return nil, nil
	}

	var conflictKey string
	var conflictVal Any
	for k, v := range condition {
		conflictKey = k
		conflictVal = v
	}

	merged := Map{}
	for k, v := range data {
		merged[k] = v
	}
	if _, ok := merged[conflictKey]; !ok {
		merged[conflictKey] = conflictVal
	}

	createVal, err := mapCreate(t.fields, merged)
	if err != nil {
		return nil, wrapErr(t.name+".upsert.native.map", ErrInvalidUpdate, err)
	}
	if len(createVal) == 0 {
		return nil, wrapErr(t.name+".upsert.native.empty", ErrInvalidUpdate, fmt.Errorf("empty upsert data"))
	}

	keys := make([]string, 0, len(createVal))
	for k := range createVal {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	insertCols := make([]string, 0, len(keys))
	insertPH := make([]string, 0, len(keys))
	insertVals := make([]Any, 0, len(keys))
	for _, k := range keys {
		insertCols = append(insertCols, d.Quote(k))
		insertVals = append(insertVals, createVal[k])
		insertPH = append(insertPH, d.Placeholder(len(insertVals)))
	}

	updateCols := make([]string, 0, len(keys))
	for _, k := range keys {
		if k == conflictKey {
			continue
		}
		qk := d.Quote(k)
		switch name {
		case "pgsql", "postgres":
			updateCols = append(updateCols, qk+" = EXCLUDED."+qk)
		case "sqlite":
			updateCols = append(updateCols, qk+" = excluded."+qk)
		case "mysql":
			updateCols = append(updateCols, qk+" = VALUES("+qk+")")
		}
	}
	if len(updateCols) == 0 {
		updateCols = append(updateCols, d.Quote(conflictKey)+" = "+d.Quote(conflictKey))
	}

	source := t.base.sourceExpr(t.schema, t.source)
	sqlText := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", source, strings.Join(insertCols, ","), strings.Join(insertPH, ","))
	switch name {
	case "pgsql", "postgres":
		sqlText += " ON CONFLICT (" + d.Quote(conflictKey) + ") DO UPDATE SET " + strings.Join(updateCols, ",")
		sqlText += " RETURNING " + d.Quote(t.key)
	case "sqlite":
		sqlText += " ON CONFLICT(" + d.Quote(conflictKey) + ") DO UPDATE SET " + strings.Join(updateCols, ",")
		if d.SupportsReturning() {
			sqlText += " RETURNING " + d.Quote(t.key)
		}
	case "mysql":
		sqlText += " ON DUPLICATE KEY UPDATE " + strings.Join(updateCols, ",")
	}

	out := Map{}
	for k, v := range createVal {
		out[k] = v
	}
	if d.SupportsReturning() {
		var id any
		if err := t.base.currentExec().QueryRowContext(context.Background(), sqlText, toInterfaces(insertVals)...).Scan(&id); err != nil {
			statsFor(t.base.inst.Name).Errors.Add(1)
			return nil, wrapErr(t.name+".upsert.native.return", ErrInvalidUpdate, classifySQLError(err))
		}
		out[t.key] = id
		statsFor(t.base.inst.Name).Writes.Add(1)
		cacheVersionBump(t.base.inst.Name)
		return out, nil
	}
	res, err := t.base.currentExec().ExecContext(context.Background(), sqlText, toInterfaces(insertVals)...)
	if err != nil {
		statsFor(t.base.inst.Name).Errors.Add(1)
		return nil, wrapErr(t.name+".upsert.native.exec", ErrInvalidUpdate, classifySQLError(err))
	}
	if id, err := res.LastInsertId(); err == nil && id > 0 {
		out[t.key] = id
	}
	statsFor(t.base.inst.Name).Writes.Add(1)
	cacheVersionBump(t.base.inst.Name)
	return out, nil
}

func (t *sqlTable) upsertCreateData(data Map, condition Map) Map {
	setPart, incPart, _, _, _ := t.parseUpdateData(data, nil, false)
	out := Map{}
	for k, v := range condition {
		out[k] = v
	}
	for k, v := range setPart {
		out[k] = v
	}
	// Mongo-style insert behavior for $inc in upsert: initialize with inc value.
	for k, v := range incPart {
		if _, exists := out[k]; !exists {
			out[k] = v
		}
	}
	return out
}
