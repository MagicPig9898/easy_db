package mysql

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestCreateTableFromStruct_Simple(t *testing.T) {
	mysqlCli, err := NewClient("localhost", 3306, "root", "123456", "test")
	if err != nil {
		t.Skipf("skip test, connect mysql failed: %v", err)
	}
	defer mysqlCli.Close()

	// type AutoIncUser struct {
	// 	ID   int    `db:"id,auto_increment"`
	// 	Name string `db:"name"`
	// }

	// type AutoIncUser struct {
	// 	ID   string `db:"id,auto_increment"`
	// 	Name string `db:"name"`
	// }

	// type AutoIncUser struct {
	// 	ID   int    `db:"id"`
	// 	Name string `db:"name"`
	// }

	// type AutoIncUser struct {
	// 	ID   int32  `db:"id,auto_increment"`
	// 	Name string `db:"name"`
	// }

	type AutoIncUser struct {
		ID   int64  `db:"id,auto_increment"`
		Name string `db:"name"`
	}

	tableName := fmt.Sprintf("tb_auto_create_%d", time.Now().UnixNano())
	ctx := context.Background()

	if err := mysqlCli.CreateTableFromStruct(ctx, tableName, AutoIncUser{}); err != nil {
		t.Fatalf("CreateTableFromStruct failed: %v", err)
	}

	exists, err := mysqlCli.TableExists(ctx, tableName)
	if err != nil {
		t.Fatalf("TableExists failed: %v", err)
	}
	if !exists {
		t.Fatalf("table %s should exist after CreateTableFromStruct", tableName)
	}
}
