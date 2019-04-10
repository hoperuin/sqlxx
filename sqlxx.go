package sqlxx

import (
	"bytes"
	"database/sql"
	"fmt"
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
	sc["saveEntity"], _ = buildInsert(s, false)
	return sc
}

func buildInsert(s *structs.Struct, notNull bool) (string, []interface{}) {
	n, v, values := setFieldNames(s, notNull)
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

func setFieldNames(s *structs.Struct, notNull bool) (names []string, valuePlaceholders []string, values []interface{}) {
	for _, v := range s.Fields() {
		if v.Tag("db") == "" && v.Tag("table") == "" {
			panic("must be exist db tag")
		} else {
			if notNull {
				defaltValue := false
				if d, ok := v.Value().(int); ok {
					if d == 0 {
						defaltValue = true
					}

				} else if s, ok := v.Value().(string); ok {
					if s == "" {
						defaltValue = true
					}
				} else {
					defaltValue = false
				}
				if !defaltValue {
					names = append(names, v.Tag("db"))
					valuePlaceholders = append(valuePlaceholders, "?")
					values = append(values, v.Value())
				}
			} else {
				names = append(names, v.Tag("db"))
				valuePlaceholders = append(valuePlaceholders, "?")
			}
		}
	}
	return
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
	fieldNames, _, _ := setFieldNames(s, false)

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

func (sqlxx *Sqlxx) SelectOne(args ...interface{}) (interface{}, error) {
	err := sqlxx.db.Get(sqlxx.dest, sqlxx.cache["selectOne"], args...)
	if err != nil {
		return nil, err
	}
	return sqlxx.dest, nil
}

func (sqlxx *Sqlxx) Select(args ...interface{}) (interface{}, error) {
	err := sqlxx.db.Select(sqlxx.dest, sqlxx.cache["select"], args...)
	if err != nil {
		return nil, err
	}
	return sqlxx.dest, nil
}

func (sqlxx *Sqlxx) Update(args ...interface{}) (sql.Result, error) {
	res, err := sqlxx.db.Exec(sqlxx.cache["update"], args...)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (sqlxx *Sqlxx) UpdateEntity(value interface{}) (sql.Result, error) {
	s := structs.New(value)
	res, err := sqlxx.db.Exec(sqlxx.cache["updateNotNull"], s.Values()...)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (sqlxx *Sqlxx) UpdateEntityNotNull(value interface{}) (sql.Result, error) {
	s := structs.New(value)
	res, err := sqlxx.db.Exec(sqlxx.cache["updateNotNull"], s.Values()...)
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

func (sqlxx *Sqlxx) SaveEntity(value interface{}) (sql.Result, error) {
	s := structs.New(value)
	res, err := sqlxx.db.Exec(sqlxx.cache["saveEntity"], s.Values()...)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (sqlxx *Sqlxx) SaveEntityNotNull(value interface{}) (sql.Result, error) {
	s := structs.New(value)
	sql, values := buildInsert(s, true)
	res, err := sqlxx.db.Exec(sql, values...)
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
