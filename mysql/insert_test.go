package mysql

import (
	"context"
	"fmt"
	"testing"
	"time"
)

type InsertOneNamedUserParam struct {
	ID      int    `db:"id"`
	Name    string `db:"name"`
	Age     int    `db:"age"`
	Sex     int    `db:"sex"`
	Email   string `db:"email"`
	Phone   string `db:"phone"`
	Address string `db:"address"`
}

type InsertManyNamedUserParam struct {
	ID      int    `db:"id"`
	Name    string `db:"name"`
	Age     int    `db:"age"`
	Sex     int    `db:"sex"`
	Email   string `db:"email"`
	Phone   string `db:"phone"`
	Address string `db:"address"`
}

func TestInsertOne(t *testing.T) {
	mysqlCli, err := NewClient("localhost", 3306, "root", "123456", "lhs")
	if err != nil {
		t.Errorf("NewMysqlClient error: %v", err)
	}
	defer mysqlCli.Close()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	insertSql := `
		INSERT INTO tb_user (id, name, age, sex, email, phone, address)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`
	ctx := context.Background()
	err = mysqlCli.InsertOne(
		ctx,
		insertSql,
		9999,
		"insert_user_"+suffix,
		20,
		1,
		"insert_"+suffix+"@test.com",
		"1"+suffix[len(suffix)-10:],
		"test_address",
	)
	if err != nil {
		t.Errorf("TestInsertOne failed, err : %v", err)
	}
}

func TestInsertOneNamed(t *testing.T) {
	mysqlCli, err := NewClient("localhost", 3306, "root", "123456", "test")
	if err != nil {
		t.Errorf("NewMysqlClient error: %v", err)
	}
	defer mysqlCli.Close()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	insertSql := `
		INSERT INTO tb_user (id, name, age, sex, email, phone, address)
		VALUES (:id, :name, :age, :sex, :email, :phone, :address)
	`
	param := &InsertOneNamedUserParam{
		ID:      9998,
		Name:    "insert_user_" + suffix,
		Age:     20,
		Sex:     1,
		Email:   "insert_" + suffix + "@test.com",
		Phone:   "1" + suffix[len(suffix)-10:],
		Address: "test_address",
	}
	ctx := context.Background()
	err = mysqlCli.InsertOneNamed(ctx, insertSql, param)
	if err != nil {
		t.Errorf("TestInsertOneNamed failed, err : %v", err)
	}
}

func TestInsertManyNamed(t *testing.T) {
	mysqlCli, err := NewClient("localhost", 3306, "root", "123456", "lhs")
	if err != nil {
		t.Errorf("NewMysqlClient error: %v", err)
	}
	defer mysqlCli.Close()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	insertSql := `
		INSERT INTO tb_user (id, name, age, sex, email, phone, address)
		VALUES (:id, :name, :age, :sex, :email, :phone, :address)
	`
	params := []InsertManyNamedUserParam{
		{
			ID:      9995,
			Name:    "insert_many_user_a_" + suffix,
			Age:     21,
			Sex:     1,
			Email:   "insert_many_a_" + suffix + "@test.com",
			Phone:   "1" + suffix[len(suffix)-10:],
			Address: "test_address_a",
		},
		{
			ID:      9994,
			Name:    "insert_many_user_b_" + suffix,
			Age:     22,
			Sex:     1,
			Email:   "insert_many_b_" + suffix + "@test.com",
			Phone:   "2" + suffix[len(suffix)-10:],
			Address: "test_address_b",
		},
	}

	ctx := context.Background()
	err = mysqlCli.InsertManyNamed(ctx, insertSql, &params)
	if err != nil {
		t.Errorf("TestInsertManyNamed failed, err : %v", err)
	}
}
