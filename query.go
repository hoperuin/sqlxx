package sqlxx

import (
	"bytes"
	"github.com/jmoiron/sqlx"
)

type condition int
type sqlType int

const (
	Select sqlType = iota + 1
	From
	Where
	Order
	Group
	Having
)

const (
	Equal condition = iota + 1
	NotEqual
	LessThanOrEqual
	GreaterThanOrEqual
	LessThan
	GreaterThan
	NotNull
	IsNull
	Like
	NotLike
	In
	NotIn
	Between
	NotBetween
	None
)

type sqlValue struct {
	key   string
	value interface{}
	//between used
	value2 interface{}
	cond   condition
	st     sqlType
}

func newSqlValue(field string, cond condition, value interface{}, st sqlType) sqlValue {
	return sqlValue{
		key:   field,
		value: value,
		cond:  cond,
		st:    st,
	}
}

func newSelectSqlValue(field string) sqlValue {
	return newSqlValue(field, None, nil, Select)
}

func newFromSqlValue(table string) sqlValue {
	return newSqlValue(table, None, nil, From)
}

func newWhereSqlValue(field string, cond condition, value interface{}) sqlValue {
	return newSqlValue(field, cond, value, Where)
}

func newOrderSqlValue(field string, desc string) sqlValue {
	return newSqlValue(field, None, desc, Order)
}

func newGroupSqlValue(field string) sqlValue {
	return newSqlValue(field, None, nil, Group)
}

func newHavingSqlValue(field string, cond condition, value interface{}) sqlValue {
	return newSqlValue(field, cond, value, Having)
}

type query struct {
	dest        interface{}
	db          *sqlx.DB
	selectNames []string
	slt         []sqlValue
	from        []sqlValue
	where       []sqlValue
	order       []sqlValue
	group       []sqlValue
	having      []sqlValue
	whereValue  []interface{}
}

func newQuery(dest interface{}, db *sqlx.DB, selectNames []string) *query {
	return &query{
		dest:        dest,
		db:          db,
		selectNames: selectNames,
	}
}

func newQuery2(dest interface{}, db *sqlx.DB) *query {
	return &query{
		dest: dest,
		db:   db,
	}
}

func (q *query) Select(field ...string) *query {
	for _, f := range field {
		q.slt = append(q.slt, newSelectSqlValue(f))
	}
	return q
}

func (q *query) SelectDefault() *query {
	for _, f := range q.selectNames {
		q.slt = append(q.slt, newSelectSqlValue(f))
	}
	return q
}

func (q *query) From(table ...string) *query {
	for _, t := range table {
		q.from = append(q.from, newFromSqlValue(t))
	}
	return q
}

func (q *query) Where(field string, cond condition, value interface{}) *query {
	q.where = append(q.where, newWhereSqlValue(field, cond, value))
	return q
}

func (q *query) Between(field string, value interface{}, value2 interface{}) *query {
	sqlValue := newWhereSqlValue(field, Between, value)
	sqlValue.value2 = value2
	q.where = append(q.where, sqlValue)
	return q
}

func (q *query) Order(field string, desc string) *query {
	q.order = append(q.order, newOrderSqlValue(field, desc))
	return q
}

func (q *query) Group(field ...string) *query {
	for _, g := range field {
		q.group = append(q.group, newGroupSqlValue(g))
	}
	return q
}

func (q *query) Having(field string, cond condition, value interface{}) *query {
	q.where = append(q.where, newHavingSqlValue(field, cond, value))
	return q
}

func (q *query) build() (string, error) {
	var sb bytes.Buffer
	sb.WriteString("SELECT ")
	for i, s := range q.slt {
		sb.WriteString(s.key)
		if i != len(q.slt)-1 {
			sb.WriteString(",")
		}
	}
	sb.WriteString(" FROM ")
	for i, f := range q.from {
		sb.WriteString(f.key)
		if i != len(q.from)-1 {
			sb.WriteString(",")
		}
	}
	sb.WriteString(" WHERE ")
	for i, w := range q.where {
		q.whereValue = append(q.whereValue, w.value)
		if w.cond == Between {
			q.whereValue = append(q.whereValue, w.value2)
		}
		sb.WriteString(w.key)
		switch w.cond {
		case Equal:
			sb.WriteString(" = ? ")
		case NotEqual:
			sb.WriteString(" <> ? ")
		case LessThanOrEqual:
			sb.WriteString(" <= ? ")
		case GreaterThanOrEqual:
			sb.WriteString(" >= ? ")
		case LessThan:
			sb.WriteString(" < ? ")
		case GreaterThan:
			sb.WriteString(" > ? ")
		case NotNull:
			sb.WriteString(" is not null ")
		case IsNull:
			sb.WriteString(" is null ")
		case Like:
			sb.WriteString(" like ? ")
		case NotLike:
			sb.WriteString(" not like ? ")
		case In:
			sb.WriteString(" in ? ")
		case NotIn:
			sb.WriteString(" not in ? ")
		case Between:
			sb.WriteString(" between ? AND ? ")
		case NotBetween:
			sb.WriteString(" not between ? AND ? ")
		default:
			panic("unsupport condition!")

		}
		if i != len(q.from)-1 {
			sb.WriteString(" AND ")
		}
	}

	if len(q.group) > 0 {
		sb.WriteString(" GROUP BY ")
		for i, g := range q.group {
			sb.WriteString(g.key)
			if i != len(q.group)-1 {
				sb.WriteString(",")
			}
		}
	}

	if len(q.having) > 0 {
		sb.WriteString(" HAVING ")
		for i, h := range q.having {
			q.whereValue = append(q.whereValue, h.value)
			if h.cond == Between {
				q.whereValue = append(q.whereValue, h.value2)
			}
			sb.WriteString(h.key)
			switch h.cond {
			case Equal:
				sb.WriteString(" = ? ")
			case NotEqual:
				sb.WriteString(" <> ? ")
			case LessThanOrEqual:
				sb.WriteString(" <= ? ")
			case GreaterThanOrEqual:
				sb.WriteString(" >= ? ")
			case LessThan:
				sb.WriteString(" < ? ")
			case GreaterThan:
				sb.WriteString(" > ? ")
			case NotNull:
				sb.WriteString(" is not null ")
			case IsNull:
				sb.WriteString(" is null ")
			case Like:
				sb.WriteString(" like ? ")
			case NotLike:
				sb.WriteString(" not like ? ")
			case In:
				sb.WriteString(" in ? ")
			case NotIn:
				sb.WriteString(" not in ? ")
			case Between:
				sb.WriteString(" between ? AND ? ")
			case NotBetween:
				sb.WriteString(" not between ? AND ? ")
			default:
				panic("unsupport condition!")

			}
			if i != len(q.having)-1 {
				sb.WriteString(" AND ")
			}
		}
	}

	if len(q.order) > 0 {
		sb.WriteString(" ORDER BY ")
		for _, o := range q.order {
			sb.WriteString(o.key)
			sb.WriteString(" ")
			sb.WriteString(o.value.(string))
		}
	}

	return sb.String(), nil
}

func (q *query) Get() error {
	sql, err := q.build()
	if err != nil {
		return err
	}
	err = q.db.Get(q.dest, sql, q.whereValue...)
	return err
}

func (q *query) List() error {
	sql, err := q.build()
	if err != nil {
		return err
	}
	err = q.db.Select(q.dest, sql, q.whereValue...)
	return nil
}
