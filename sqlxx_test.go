package sqlxx

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"log"
	"testing"
)

type UserInfo struct {
	table   string         `table:"user"`
	Id      int            `json:"id" db:"id"`
	Name    string         `json:"name" db:"name"`
	Age     int            `json:"age" db:"age"`
	Email   sql.NullString `json:"email" db:"email"`
	Address string         `json:"address" db:"address"`
}

var userDao = New(&UserInfo{}, db())

func db() *sqlx.DB {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=true", "root", "rootroot", "localhost", 3306, "test")
	db, err := Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func TestSqlxx_Savex(t *testing.T) {
	ui := UserInfo{
		Name:    "abc",
		Age:     11,
		Email:   sql.NullString{Valid: true, String: "wf1337@email.com"},
		Address: "测试",
	}
	_, err := userDao.Savex(&ui)
	if err != nil {
		t.Error(err)
	}
}

func TestSqlxx_SavexNotNull(t *testing.T) {
	ui := UserInfo{
		Name:    "abc",
		Age:     11,
		Email:   sql.NullString{Valid: true, String: "wf1337@email.com"},
		Address: "测试",
	}
	_, err := userDao.SavexNotNull(&ui)
	if err != nil {
		t.Error(err)
	}
}

func TestSqlxx_Selectx(t *testing.T) {
	var uis []UserInfo
	err := userDao.Selectx(&uis, &UserInfo{Id: 8})
	if err != nil {
		t.Error(err)
	}
	log.Println(uis)
}

func TestSqlxx_SelectOnex(t *testing.T) {
	u, err := userDao.SelectOnex(&UserInfo{Id: 2})
	if err != nil {
		t.Error(err)
	}
	log.Println(u)
}

func TestSqlxx_Updatex(t *testing.T) {
	_, err := userDao.Updatex(&UserInfo{Id: 2, Age: 10, Name: "测试", Email: sql.NullString{String: "wf1337@email.com", Valid: true}})
	if err != nil {
		t.Error(err)
	}
}

func TestSqlxx_UpdatexNotNull(t *testing.T) {
	_, err := userDao.UpdatexNotNull(&UserInfo{Id: 2, Name: "测试"})
	if err != nil {
		t.Error(err)
	}
}

func TestSqlxx_Updatexw(t *testing.T) {
	_, err := userDao.Updatexw(&UserInfo{Name: "测试11"}, &UserInfo{Age: 11})
	if err != nil {
		t.Error(err)
	}
}

func TestSqlxx_UpdatexwNotNull(t *testing.T) {
	_, err := userDao.UpdatexwNotNull(&UserInfo{Name: "测试11"}, &UserInfo{Age: 11})
	if err != nil {
		t.Error(err)
	}
}

func TestSqlxx_Deletex(t *testing.T) {
	_, err := userDao.Deletex(&UserInfo{Name: "abc", Email: sql.NullString{"wf1337@email.com", true}})
	if err != nil {
		t.Error(err)
	}
}
