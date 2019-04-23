package sqlxx

import (
	"log"
	"testing"
)

func TestQuery_Get(t *testing.T) {
	u := UserInfo{}
	q := newQuery2(&u, db())
	err := q.Select("id", "name").From("user").Where("name", Equal, "测试").Get()
	if err != nil {
		t.Error(err)
	}
	log.Println(u)
}

func TestQuery_List(t *testing.T) {
	ul := []UserInfo{}
	q := newQuery2(&ul, db())
	err := q.Select("id", "name").From("user").Where("name", Equal, "测试").List(&ul)
	if err != nil {
		t.Error(err)
	}
	log.Println(ul)
}
