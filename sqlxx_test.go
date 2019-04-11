package sqlxx

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"log"
	"testing"
)

type UserInfo struct {
	table   string `table:"user"`
	Id      int    `json:"id" db:"id"`
	Name    string `json:"name" db:"name"`
	Age     int    `json:"age" db:"age"`
	Email   string `json:"email" db:"email"`
	Address string `json:"address" db:"address"`
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
		Address: "测试",
	}
	_, err := userDao.SavexNotNull(&ui)
	if err != nil {
		t.Error(err)
	}
}

func TestSqlxx_Selectx(t *testing.T) {
	var uis []UserInfo
	err := userDao.Selectx(&uis, &UserInfo{Id: 1})
	if err != nil {
		t.Error(err)
	}
	log.Println(uis)
}

func TestSqlxx_SelectOnex(t *testing.T) {
	u, err := userDao.SelectOnex(&UserInfo{Id: 1})
	if err != nil {
		t.Error(err)
	}
	log.Println(u)
}

func TestSqlxx_Updatex(t *testing.T) {
	_, err := userDao.Updatex(&UserInfo{Id: 1, Name: "测试"})
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

func TestSqlxx_DeletePrimaryKey(t *testing.T) {
	_, err := userDao.DeletePrimaryKey(&UserInfo{Id: 1})
	if err != nil {
		t.Error(err)
	}
}
