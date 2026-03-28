package mysql

import (
	"context"
	"fmt"
	"testing"
	"time"
)

type UpdateOneNamedUserParam struct {
	ID      int    `db:"id"`
	Name    string `db:"name"`
	Age     int    `db:"age"`
	Address string `db:"address"`
}

type UpdateManyNamedUserParam struct {
	ID      int    `db:"id"`
	Name    string `db:"name"`
	Age     int    `db:"age"`
	Address string `db:"address"`
}

func TestUpdateOne(t *testing.T) {
	mysqlCli, err := NewClient("localhost", 3306, "root", "123456", "lhs")
	if err != nil {
		t.Errorf("NewMysqlClient error: %v", err)
	}
	defer mysqlCli.Close()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	updateSql := `
		UPDATE tb_user
		SET name = ?, age = ?, address = ?
		WHERE id = ?
	`
	ctx := context.Background()
	err = mysqlCli.UpdateOne(ctx, updateSql, "update_user_"+suffix, 25, "update_address", 9999)
	if err != nil {
		t.Errorf("TestUpdateOne failed, err : %v", err)
	}
}

func TestUpdateOneNamed(t *testing.T) {
	mysqlCli, err := NewClient("localhost", 3306, "root", "123456", "lhs")
	if err != nil {
		t.Errorf("NewMysqlClient error: %v", err)
	}
	defer mysqlCli.Close()
	updateSql := `
		UPDATE tb_user
		SET name = :name, age = :age, address = :address
		WHERE id = :id
	`
	param := &UpdateOneNamedUserParam{
		ID:      9998,
		Name:    "update_user",
		Age:     26,
		Address: "update_address_named",
	}
	ctx := context.Background()
	err = mysqlCli.UpdateOneNamed(ctx, updateSql, param)
	if err != nil {
		t.Errorf("TestUpdateOneNamed failed, err : %v", err)
	}
}
