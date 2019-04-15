package sqlxx

import (
	"bytes"
	"database/sql"
	"github.com/fatih/structs"
	"github.com/jmoiron/sqlx"
	"log"
	"strings"
	"unicode"
)

type SelectOner interface {
	SelectOne() string
}

type Selecter interface {
	Select() string
}

type Saver interface {
	Save() string
}

type Updater interface {
	Update() string
}

type Deleter interface {
	Delete() string
}

type Mapper interface {
	Selecter
	Saver
	Updater
	Deleter
}

func Open(driverName, dataSourceName string) (*sqlx.DB, error) {
	return sqlx.Open(driverName, dataSourceName)
}

type sqlCache map[string]string

func setCache(dest interface{}, s *structs.Struct, selectFields []string) sqlCache {
	sc := make(sqlCache)
	if v, ok := dest.(SelectOner); ok {
		sc["selectOne"] = v.SelectOne()
	} else {
		sc["selectOne"], _ = buildSelect(s, false, true)
	}
	if v, ok := dest.(Selecter); ok {
		sc["select"] = v.Select()
	} else {
		sc["select"], _ = buildSelect(s, false, true)
	}
	if v, ok := dest.(Saver); ok {
		sc["save"] = v.Save()
	} else {
		sc["save"], _ = buildInsert(s, false, false)
	}
	if v, ok := dest.(Updater); ok {
		sc["update"] = v.Update()
	} else {
		sc["update"], _ = buildUpdate(s, false, false)
	}

	if v, ok := dest.(Deleter); ok {
		sc["delete"] = v.Delete()
	} else {
		sc["delete"], _ = buildDelete(s)
	}

	return sc
}

func getPkValue(s *structs.Struct) (pk string, value interface{}) {
	for _, v := range s.Fields() {
		if v.Tag("pk") != "" {
			pk, value = v.Name(), v.Value()
		} else if v.Name() == "Id" {
			pk, value = v.Tag("db"), v.Value()
		}
	}
	if pk == "" {
		panic("must be id set")
	}
	return
}

func buildInsert(s *structs.Struct, notNull bool, allField bool) (string, []interface{}) {
	n, v, values := setFieldNames(s, notNull, allField)
	var sb bytes.Buffer
	sb.WriteString("INSERT INTO ")
	sb.WriteString(setTableName(s))
	sb.WriteString("(")
	sb.WriteString(strings.Join(n, ","))
	sb.WriteString(") VALUES (")
	sb.WriteString(strings.Join(v, ","))
	sb.WriteString(")")
	return sb.String(), values
}

func buildUpdate(s *structs.Struct, notNull bool, allField bool) (string, []interface{}) {
	n, _, values := setFieldNames(s, notNull, allField)
	var sb bytes.Buffer
	sb.WriteString("UPDATE ")
	sb.WriteString(setTableName(s))
	sb.WriteString(" SET ")
	setField := strings.Join(n, " = ? ,")
	sb.WriteString(setField + " = ?")
	sb.WriteString(" WHERE id = ?")
	return sb.String(), values
}

func buildUpdatew(s *structs.Struct, w *structs.Struct, notNull bool, allField bool) (string, []interface{}) {
	n, _, sv := setFieldNames(s, notNull, allField)
	var sb bytes.Buffer
	sb.WriteString("UPDATE ")
	sb.WriteString(setTableName(s))
	sb.WriteString(" SET ")
	setField := strings.Join(n, " = ? ,")
	sb.WriteString(setField + " = ?")
	sb.WriteString(" WHERE ")

	nv, _, values := setFieldNames(w, true, true)
	if len(nv) == 1 {
		sb.WriteString(nv[0])
	} else {
		whereField := strings.Join(nv, " = ? ,")
		sb.WriteString(whereField)
	}
	sb.WriteString(" = ?")
	sv = append(sv, values...)
	return sb.String(), sv
}

func buildSelect(s *structs.Struct, notNull bool, allField bool) (string, []interface{}) {
	n, _, _ := setFieldNames(s, notNull, allField)
	var sb bytes.Buffer
	sb.WriteString("SELECT ")
	sb.WriteString(strings.Join(n, ","))
	sb.WriteString(" FROM ")
	sb.WriteString(setTableName(s))
	sb.WriteString(" WHERE ")

	nv, _, values := setFieldNames(s, true, true)
	if len(nv) == 1 {
		sb.WriteString(nv[0])
	} else {
		whereField := strings.Join(nv, " = ? ,")
		sb.WriteString(whereField)
	}
	sb.WriteString(" = ?")
	return sb.String(), values
}

func buildDelete(s *structs.Struct) (string, []interface{}) {
	var sb bytes.Buffer
	sb.WriteString("DELETE  FROM ")
	sb.WriteString(setTableName(s))
	sb.WriteString(" WHERE ")

	nv, _, values := setFieldNames(s, true, true)
	if len(nv) == 1 {
		sb.WriteString(nv[0])
	} else {
		whereField := strings.Join(nv, " = ? and ")
		sb.WriteString(whereField)
	}
	sb.WriteString(" = ?")
	return sb.String(), values
}

func setFieldNames(s *structs.Struct, notNull bool, allField bool) (names []string, valuePlaceholders []string, values []interface{}) {
	for _, v := range s.Fields() {
		if v.Tag("db") == "" && v.Tag("table") == "" {
			panic("must be exist db tag")
		} else {
			if !v.IsExported() {
				continue
			}
			if notNull {
				zeroValue := isZeroValue(v)
				if !zeroValue {
					names = append(names, v.Tag("db"))
					valuePlaceholders = append(valuePlaceholders, "?")
					values = append(values, v.Value())
				}
			} else {
				if !allField && v.Tag("db") == "id" {
					continue
				}
				names = append(names, v.Tag("db"))
				valuePlaceholders = append(valuePlaceholders, "?")
				values = append(values, v.Value())
			}
		}
	}
	return
}

func isZeroValue(v *structs.Field) bool {
	zeroValue := false
	switch val := v.Value().(type) {
	case int, int8, int16, int32, int64:
		if val == 0 {
			zeroValue = true
		}
	case float32, float64:
		if val == 0.0 {
			zeroValue = true
		}
	case string:
		if val == "" || len(val) == 0 {
			zeroValue = true
		}
	case sql.NullString:
		if !val.Valid {
			zeroValue = true
		}
	case sql.NullInt64:
		if !val.Valid {
			zeroValue = true
		}
	case sql.NullBool:
		if !val.Valid {
			zeroValue = true
		}
	case sql.NullFloat64:
		if !val.Valid {
			zeroValue = true
		}
	}
	return zeroValue
}

func setTableName(s *structs.Struct) string {
	if f, ok := s.FieldOk("table"); ok {
		if f.Tag("table") == "" {
			panic("must table tag")
		}
		return f.Tag("table")
	}
	return toTableName(s.Name())
}

func toTableName(structName string) string {
	var rs []rune
	for i, r := range structName {
		if i != 0 && unicode.IsUpper(r) {
			rs = append(rs, '_', unicode.ToLower(r))
		} else {
			if unicode.IsUpper(r) {
				rs = append(rs, unicode.ToLower(r))
			} else {
				rs = append(rs, r)
			}
		}
	}
	return string(rs)
}

type Sqlxx struct {
	dest       interface{}
	db         *sqlx.DB
	cache      sqlCache
	tableName  string
	fields     []*structs.Field
	fieldNames []string
	fieldLen   int
	values     []interface{}
	s          *structs.Struct
}

func New(dest interface{}, db *sqlx.DB) *Sqlxx {
	s := structs.New(dest)
	fields := s.Fields()
	fieldNames, _, _ := setFieldNames(s, false, true)

	return &Sqlxx{
		dest:       dest,
		db:         db,
		tableName:  setTableName(s),
		cache:      setCache(dest, s, fieldNames),
		fields:     fields,
		fieldNames: fieldNames,
		fieldLen:   len(fieldNames),
		s:          s,
	}
}

func (sqlxx *Sqlxx) SelectOne(args ...interface{}) (interface{}, error) {
	err := sqlxx.db.Get(sqlxx.dest, sqlxx.cache["selectOne"], args...)
	if err != nil {
		return nil, err
	}
	return sqlxx.dest, nil
}

func (sqlxx *Sqlxx) Select(dest interface{}, args ...interface{}) error {
	err := sqlxx.db.Select(dest, sqlxx.cache["select"], args...)
	return err
}

func (sqlxx *Sqlxx) SelectOnex(value interface{}) (interface{}, error) {
	s := structs.New(value)
	sql, args := buildSelect(s, false, true)
	err := sqlxx.db.Get(sqlxx.dest, sql, args...)
	return sqlxx.dest, err
}

func (sqlxx *Sqlxx) Selectx(dest interface{}, value interface{}) error {
	s := structs.New(value)
	sql, args := buildSelect(s, false, true)
	err := sqlxx.db.Select(dest, sql, args...)
	return err
}

func (sqlxx *Sqlxx) Update(args ...interface{}) (sql.Result, error) {
	res, err := sqlxx.db.Exec(sqlxx.cache["update"], args...)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (sqlxx *Sqlxx) Updatex(value interface{}) (sql.Result, error) {
	s := structs.New(value)
	sql, values := buildUpdate(s, false, false)
	_, pkVal := getPkValue(s)
	values = append(values, pkVal)
	res, err := sqlxx.db.Exec(sql, values...)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (sqlxx *Sqlxx) UpdatexNotNull(value interface{}) (sql.Result, error) {
	s := structs.New(value)
	sql, values := buildUpdate(s, true, false)
	_, pkVal := getPkValue(s)
	values = append(values, pkVal)
	res, err := sqlxx.db.Exec(sql, values...)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (sqlxx *Sqlxx) Updatexw(value interface{}, where interface{}) (sql.Result, error) {
	s := structs.New(value)
	w := structs.New(where)
	sql, values := buildUpdatew(s, w, false, true)
	log.Println(sql, values)
	res, err := sqlxx.db.Exec(sql, values...)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (sqlxx *Sqlxx) UpdatexwNotNull(value interface{}, where interface{}) (sql.Result, error) {
	s := structs.New(value)
	w := structs.New(where)
	sql, values := buildUpdatew(s, w, true, true)
	res, err := sqlxx.db.Exec(sql, values...)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (sqlxx *Sqlxx) Save(args ...interface{}) (sql.Result, error) {
	res, err := sqlxx.db.Exec(sqlxx.cache["save"], args...)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (sqlxx *Sqlxx) Savex(value interface{}) (sql.Result, error) {
	s := structs.New(value)
	sql, args := buildInsert(s, false, false)
	res, err := sqlxx.db.Exec(sql, args...)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (sqlxx *Sqlxx) SavexNotNull(value interface{}) (sql.Result, error) {
	s := structs.New(value)
	sql, args := buildInsert(s, true, false)
	res, err := sqlxx.db.Exec(sql, args...)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (sqlxx *Sqlxx) Delete(args ...interface{}) (sql.Result, error) {
	res, err := sqlxx.db.Exec(sqlxx.cache["delete"], args...)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (sqlxx *Sqlxx) Deletex(value interface{}) (sql.Result, error) {
	s := structs.New(value)
	sql, values := buildDelete(s)
	res, err := sqlxx.db.Exec(sql, values...)
	if err != nil {
		return nil, err
	}
	return res, nil
}
