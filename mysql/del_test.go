package mysql

import (
	"context"
	"testing"
)

type DeleteOneNamedUserParam struct {
	ID int `db:"id"`
}

func TestDeleteOne(t *testing.T) {
	mysqlCli, err := NewClient("localhost", 3306, "root", "123456", "lhs")
	if err != nil {
		t.Errorf("NewMysqlClient error: %v", err)
	}
	defer mysqlCli.Close()

	deleteSql := `
		DELETE FROM tb_user
		WHERE id = ?
	`
	ctx := context.Background()
	err = mysqlCli.DeleteOne(ctx, deleteSql, 9999)
	if err != nil {
		t.Errorf("TestDeleteOne failed, err : %v", err)
	}
}

func TestDeleteOneNamed(t *testing.T) {
	mysqlCli, err := NewClient("localhost", 3306, "root", "123456", "lhs")
	if err != nil {
		t.Errorf("NewMysqlClient error: %v", err)
	}
	defer mysqlCli.Close()

	deleteSql := `
		DELETE FROM tb_user
		WHERE id = :id
	`
	param := &DeleteOneNamedUserParam{
		ID: 9998,
	}
	ctx := context.Background()
	err = mysqlCli.DeleteOneNamed(ctx, deleteSql, param)
	if err != nil {
		t.Errorf("TestDeleteOneNamed failed, err : %v", err)
	}
}
