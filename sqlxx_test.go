package sqlxx

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"log"
	"testing"
)

/**
CREATE TABLE `user` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `name` varchar(20) DEFAULT NULL,
  `age` tinyint(4) DEFAULT NULL,
  `email` varchar(50) DEFAULT NULL,
  `address` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
*/

type UserInfo struct {
	table   string         `table:"user"`
	Id      int            `json:"id" db:"id"`
	Name    string         `json:"name" db:"name"`
	Age     int            `json:"age" db:"age"`
	Email   sql.NullString `json:"email" db:"email"`
	Address string         `json:"address" db:"address"`
}

func (u *UserInfo) Count() string {
	return "select count(*) from user where name = ?"
}

var ui = UserInfo{}
var userDao = New(&ui, db())

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

func TestSqlxx_Countx(t *testing.T) {
	c, err := userDao.Countx(&UserInfo{Name: "测试"})
	if err != nil {
		t.Error(err)
	}
	log.Println(c)
}

func TestSqlxx_Count(t *testing.T) {
	c, err := userDao.Count("测试")
	if err != nil {
		t.Error(err)
	}
	log.Println(c)
}

func TestSqlxx_Query(t *testing.T) {
	err := userDao.Query().SelectDefault().From("user").Where("name", Equal, "测试").Get()
	if err != nil {
		t.Error(err)
	}
	log.Println(ui)
}
