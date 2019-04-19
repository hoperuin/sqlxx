package sqlxx

import (
	"bytes"
	"database/sql"
	"errors"
	"github.com/fatih/structs"
	"github.com/jmoiron/sqlx"
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

type Counter interface {
	Count() string
}

type Mapper interface {
	Selecter
	Saver
	Updater
	Deleter
	Counter
}

func Open(driverName, dataSourceName string) (*sqlx.DB, error) {
	return sqlx.Open(driverName, dataSourceName)
}

type sqlCache map[string]string

func setCache(dest interface{}, s *structs.Struct) sqlCache {
	sc := make(sqlCache)
	if v, ok := dest.(SelectOner); ok {
		sc["selectOne"] = v.SelectOne()
	}

	if v, ok := dest.(Selecter); ok {
		sc["select"] = v.Select()
	}

	if v, ok := dest.(Saver); ok {
		sc["save"] = v.Save()
	}

	if v, ok := dest.(Updater); ok {
		sc["update"] = v.Update()
	}

	if v, ok := dest.(Deleter); ok {
		sc["delete"] = v.Delete()
	}

	if v, ok := dest.(Counter); ok {
		sc["count"] = v.Count()
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

func buildCount(s *structs.Struct) (string, []interface{}) {
	var sb bytes.Buffer
	sb.WriteString("SELECT count(*)  FROM ")
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
	tx         *sqlx.Tx
	cache      sqlCache
	tableName  string
	fields     []*structs.Field
	fieldNames []string
	fieldLen   int
	values     []interface{}
	s          *structs.Struct
	isTx       bool
}

func New(dest interface{}, db *sqlx.DB) *Sqlxx {
	s := structs.New(dest)
	fields := s.Fields()
	fieldNames, _, _ := setFieldNames(s, false, true)

	return &Sqlxx{
		dest:       dest,
		db:         db,
		tableName:  setTableName(s),
		cache:      setCache(dest, s),
		fields:     fields,
		fieldNames: fieldNames,
		fieldLen:   len(fieldNames),
		s:          s,
	}
}

func (sqlxx *Sqlxx) Begin() (*Sqlxx, error) {
	tx, err := sqlxx.db.Beginx()
	if err != nil {
		return nil, err
	}
	sqlxx.tx = tx
	sqlxx.isTx = true
	return sqlxx, nil
}

func (sqlxx *Sqlxx) Commit() error {
	if sqlxx.tx == nil || sqlxx.isTx == false {
		return errors.New("No start Tx")
	}
	sqlxx.isTx = false
	err := sqlxx.tx.Commit()
	return err
}

func (sqlxx *Sqlxx) SelectOne(args ...interface{}) (interface{}, error) {
	if _, ok := sqlxx.dest.(SelectOner); !ok {
		return nil, errors.New("must be implement SelectOner interface")
	}
	var err error
	if sqlxx.isTx {
		err = sqlxx.tx.Get(sqlxx.dest, sqlxx.cache["selectOne"], args...)
	} else {
		err = sqlxx.db.Get(sqlxx.dest, sqlxx.cache["selectOne"], args...)
	}

	if err != nil {
		return nil, err
	}
	return sqlxx.dest, nil
}

func (sqlxx *Sqlxx) Select(dest interface{}, args ...interface{}) error {
	if _, ok := sqlxx.dest.(Selecter); !ok {
		return errors.New("must be implement Selecter interface")
	}
	var err error
	if sqlxx.isTx {
		err = sqlxx.tx.Select(dest, sqlxx.cache["select"], args...)
	} else {
		err = sqlxx.db.Select(dest, sqlxx.cache["select"], args...)
	}

	return err
}

func (sqlxx *Sqlxx) SelectOnex(value interface{}) (interface{}, error) {
	s := structs.New(value)
	sql, args := buildSelect(s, false, true)
	var err error
	if sqlxx.isTx {
		err = sqlxx.tx.Get(sqlxx.dest, sql, args...)
	} else {
		err = sqlxx.db.Get(sqlxx.dest, sql, args...)
	}
	return sqlxx.dest, err
}

func (sqlxx *Sqlxx) Selectx(dest interface{}, value interface{}) error {
	s := structs.New(value)
	sql, args := buildSelect(s, false, true)
	var err error
	if sqlxx.isTx {
		err = sqlxx.tx.Select(dest, sql, args...)
	} else {
		err = sqlxx.db.Select(dest, sql, args...)
	}
	return err
}

func (sqlxx *Sqlxx) Count(args ...interface{}) (int, error) {
	var c int
	if _, ok := sqlxx.dest.(Counter); !ok {
		return -1, errors.New("must be implement Counter interface")
	}
	var err error
	if sqlxx.isTx {
		err = sqlxx.tx.Get(&c, sqlxx.cache["count"], args...)
	} else {
		err = sqlxx.db.Get(&c, sqlxx.cache["count"], args...)
	}

	if err != nil {
		return -1, err
	}
	return c, nil
}

func (sqlxx *Sqlxx) Countx(value interface{}) (int, error) {
	var c int
	s := structs.New(value)
	sql, args := buildCount(s)
	var err error
	if sqlxx.isTx {
		err = sqlxx.tx.Get(&c, sql, args...)
	} else {
		err = sqlxx.db.Get(&c, sql, args...)
	}
	if err != nil {
		return -1, err
	}
	return c, nil
}

func (sqlxx *Sqlxx) Update(args ...interface{}) (sql.Result, error) {
	if _, ok := sqlxx.dest.(Updater); !ok {
		return nil, errors.New("must be implement Updater interface")
	}
	var err error
	var res sql.Result
	if sqlxx.isTx {
		res, err = sqlxx.tx.Exec(sqlxx.cache["update"], args...)
	} else {
		res, err = sqlxx.db.Exec(sqlxx.cache["update"], args...)
	}
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (sqlxx *Sqlxx) Updatex(value interface{}) (sql.Result, error) {
	s := structs.New(value)
	sqls, values := buildUpdate(s, false, false)
	_, pkVal := getPkValue(s)
	values = append(values, pkVal)
	var err error
	var res sql.Result
	if sqlxx.isTx {
		res, err = sqlxx.tx.Exec(sqls, values...)
	} else {
		res, err = sqlxx.db.Exec(sqls, values...)
	}
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (sqlxx *Sqlxx) UpdatexNotNull(value interface{}) (sql.Result, error) {
	s := structs.New(value)
	sqls, values := buildUpdate(s, true, false)
	_, pkVal := getPkValue(s)
	values = append(values, pkVal)
	var err error
	var res sql.Result
	if sqlxx.isTx {
		res, err = sqlxx.tx.Exec(sqls, values...)
	} else {
		res, err = sqlxx.db.Exec(sqls, values...)
	}
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (sqlxx *Sqlxx) Updatexw(value interface{}, where interface{}) (sql.Result, error) {
	s := structs.New(value)
	w := structs.New(where)
	sqls, values := buildUpdatew(s, w, false, true)
	var err error
	var res sql.Result
	if sqlxx.isTx {
		res, err = sqlxx.tx.Exec(sqls, values...)
	} else {
		res, err = sqlxx.db.Exec(sqls, values...)
	}
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (sqlxx *Sqlxx) UpdatexwNotNull(value interface{}, where interface{}) (sql.Result, error) {
	s := structs.New(value)
	w := structs.New(where)
	sqls, values := buildUpdatew(s, w, true, true)
	var err error
	var res sql.Result
	if sqlxx.isTx {
		res, err = sqlxx.tx.Exec(sqls, values...)
	} else {
		res, err = sqlxx.db.Exec(sqls, values...)
	}
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (sqlxx *Sqlxx) Save(args ...interface{}) (sql.Result, error) {
	if _, ok := sqlxx.dest.(Saver); !ok {
		return nil, errors.New("must be implement Saver interface")
	}
	var err error
	var res sql.Result
	if sqlxx.isTx {
		res, err = sqlxx.tx.Exec(sqlxx.cache["save"], args...)
	} else {
		res, err = sqlxx.db.Exec(sqlxx.cache["save"], args...)
	}
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (sqlxx *Sqlxx) Savex(value interface{}) (sql.Result, error) {
	s := structs.New(value)
	sqls, args := buildInsert(s, false, false)
	var err error
	var res sql.Result
	if sqlxx.isTx {
		res, err = sqlxx.tx.Exec(sqls, args...)
	} else {
		res, err = sqlxx.db.Exec(sqls, args...)
	}
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (sqlxx *Sqlxx) SavexNotNull(value interface{}) (sql.Result, error) {
	s := structs.New(value)
	sqls, args := buildInsert(s, true, false)
	var err error
	var res sql.Result
	if sqlxx.isTx {
		res, err = sqlxx.tx.Exec(sqls, args...)
	} else {
		res, err = sqlxx.db.Exec(sqls, args...)
	}
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (sqlxx *Sqlxx) Delete(args ...interface{}) (sql.Result, error) {
	if _, ok := sqlxx.dest.(Deleter); !ok {
		return nil, errors.New("must be implement Deleter interface")
	}
	var err error
	var res sql.Result
	if sqlxx.isTx {
		res, err = sqlxx.tx.Exec(sqlxx.cache["delete"], args...)
	} else {
		res, err = sqlxx.db.Exec(sqlxx.cache["delete"], args...)
	}
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (sqlxx *Sqlxx) Deletex(value interface{}) (sql.Result, error) {
	s := structs.New(value)
	sqls, values := buildDelete(s)
	var err error
	var res sql.Result
	if sqlxx.isTx {
		res, err = sqlxx.tx.Exec(sqls, values...)
	} else {
		res, err = sqlxx.db.Exec(sqls, values...)
	}
	if err != nil {
		return nil, err
	}
	return res, nil
}
