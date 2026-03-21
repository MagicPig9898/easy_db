package mysql

import (
	"context"
	"testing"
)

type TestUser struct {
	ID            int    `db:"id"`      // 主键ID
	Name          string `db:"name"`    // 姓名
	Age           int    `db:"age"`     // 年龄
	Sex           string `db:"sex"`     // 性别
	Email         string `db:"email"`   // 邮箱
	Phone         string `db:"phone"`   // 电话
	Address       string `db:"address"` // 地址
	Qualification string `db:"-"`       // 学历/资格
}

func TestQueryMany(t *testing.T) {
	mysqlCli, err := NewClient("localhost", 3306, "root", "123456", "demo")
	if err != nil {
		t.Errorf("NewMysqlClient error: %v", err)
	}
	defer mysqlCli.Close()

	query := `
		SELECT *
		FROM tb_user
		WHERE age > ? limit 10
	`
	ctx := context.Background()
	users := []TestUser{}
	err = mysqlCli.QueryMany(ctx, &users, query, 30)
	if err != nil {
		t.Errorf("TestQueryMany failed, err : %v", err)
	}
	for _, user := range users {
		t.Logf("user is %v", user)
	}
}
