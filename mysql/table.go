package mysql

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"
)

// TableExists 检查指定表是否存在
func (c *Client) TableExists(ctx context.Context, tableName string) (bool, error) {
	query := "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?"
	var count int
	err := c.db.QueryRowContext(ctx, query, c.config.Database, tableName).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// extractTableName 从 INSERT SQL 中提取表名
// 支持格式: INSERT INTO tableName ... / INSERT INTO `tableName` ...
func extractTableName(insertSQL string) (string, error) {
	re := regexp.MustCompile(`(?i)INSERT\s+INTO\s+` + "`?" + `(\w+)` + "`?")
	matches := re.FindStringSubmatch(insertSQL)
	if len(matches) < 2 {
		return "", fmt.Errorf("cannot extract table name from sql: %s", insertSQL)
	}
	return matches[1], nil
}

// EnsureTableForInsert 插入前的表自动创建检查
// 流程：从 SQL 提取表名 → 检查表是否存在 → 如果不存在且 AutoCreateTable=true 则自动建表 → 否则返回原始错误
// 参数:
//   - ctx: 上下文
//   - insertSQL: 插入 SQL（用于提取表名）
//   - dest: 结构体指针，用于推导表结构
//
// 返回:
//   - error: 建表失败或配置不允许自动建表时返回错误
func (c *Client) EnsureTableForInsert(ctx context.Context, insertSQL string, dest interface{}) error {
	tableName, err := extractTableName(insertSQL)
	if err != nil {
		return err
	}

	exists, err := c.TableExists(ctx, tableName)
	if err != nil {
		return fmt.Errorf("check table existence failed: %w", err)
	}
	if exists {
		return nil
	}

	// 表不存在，检查是否开启了自动建表
	if !c.config.AutoCreateTable {
		return fmt.Errorf("table '%s' does not exist and AutoCreateTable is not enabled", tableName)
	}

	// 根据结构体自动建表
	return c.CreateTableFromStruct(ctx, tableName, dest)
}

// goTypeToMySQLType 将 Go 类型映射为 MySQL 列类型
func goTypeToMySQLType(t reflect.Type) string {
	switch t.Kind() {
	case reflect.Int, reflect.Int32:
		return "INT"
	case reflect.Int8:
		return "TINYINT"
	case reflect.Int16:
		return "SMALLINT"
	case reflect.Int64:
		return "BIGINT"
	case reflect.Uint, reflect.Uint32:
		return "INT UNSIGNED"
	case reflect.Uint8:
		return "TINYINT UNSIGNED"
	case reflect.Uint16:
		return "SMALLINT UNSIGNED"
	case reflect.Uint64:
		return "BIGINT UNSIGNED"
	case reflect.Float32:
		return "FLOAT"
	case reflect.Float64:
		return "DOUBLE"
	case reflect.Bool:
		return "TINYINT(1)"
	case reflect.String:
		return "VARCHAR(255)"
	default:
		// 特殊类型处理
		if t == reflect.TypeOf(time.Time{}) {
			return "DATETIME"
		}
		// 指针类型取底层类型
		if t.Kind() == reflect.Ptr {
			return goTypeToMySQLType(t.Elem())
		}
		return "TEXT"
	}
}

func parseDBColumnAndOptions(dbTag, fieldName string) (string, map[string]struct{}, bool) {
	if dbTag == "-" {
		return "", nil, true
	}
	if dbTag == "" {
		return toSnakeCase(fieldName), nil, false
	}

	parts := strings.Split(dbTag, ",")
	colName := strings.TrimSpace(parts[0])
	if colName == "" {
		colName = toSnakeCase(fieldName)
	}

	options := make(map[string]struct{})
	for _, part := range parts[1:] {
		opt := strings.ToLower(strings.TrimSpace(part))
		if opt == "" {
			continue
		}
		options[opt] = struct{}{}
	}

	return colName, options, false
}

func isIntegerType(t reflect.Type) bool {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	default:
		return false
	}
}

func hasAutoIncrementOption(options map[string]struct{}) bool {
	if len(options) == 0 {
		return false
	}

	if _, ok := options["auto_increment"]; ok {
		return true
	}
	if _, ok := options["autoincrement"]; ok {
		return true
	}
	return false
}

func isTruthyAutoIncrementTag(v string) bool {
	s := strings.TrimSpace(strings.ToLower(v))
	if s == "" {
		return false
	}
	switch s {
	case "0", "false", "off", "no":
		return false
	default:
		return true
	}
}

func hasAutoIncrementTag(field reflect.StructField, dbTagOptions map[string]struct{}) bool {
	if hasAutoIncrementOption(dbTagOptions) {
		return true
	}

	if isTruthyAutoIncrementTag(field.Tag.Get("auto_increment")) {
		return true
	}
	if isTruthyAutoIncrementTag(field.Tag.Get("autoincrement")) {
		return true
	}
	if isTruthyAutoIncrementTag(field.Tag.Get("autoIncrement")) {
		return true
	}

	mysqlTag := field.Tag.Get("mysql")
	if mysqlTag == "" {
		return false
	}
	parts := strings.Split(mysqlTag, ",")
	for _, part := range parts {
		opt := strings.ToLower(strings.TrimSpace(part))
		if opt == "auto_increment" || opt == "autoincrement" {
			return true
		}
	}
	return false
}

// CreateTableFromStruct 根据结构体定义自动创建表
// 规则：
//   - 字段名取 db tag，无 tag 则蛇形转换
//   - db:"-" 的字段跳过
//   - 未导出字段跳过
//   - 名为 "id" 的字段自动设为 PRIMARY KEY，同时如果 "id" 字段是整数类型，且有 AUTO_INCREMENT 标签，自动设为 AUTO_INCREMENT
func (c *Client) CreateTableFromStruct(ctx context.Context, tableName string, dest interface{}) error {
	structVal := reflect.ValueOf(dest)
	if structVal.Kind() == reflect.Ptr {
		structVal = structVal.Elem()
	}
	if structVal.Kind() != reflect.Struct {
		return fmt.Errorf("dest must be a struct or struct pointer")
	}

	structType := structVal.Type()
	var columns []string
	var primaryKey string

	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		// 跳过未导出字段
		if field.PkgPath != "" {
			continue
		}

		colName, dbTagOptions, skip := parseDBColumnAndOptions(field.Tag.Get("db"), field.Name)
		if skip {
			continue
		}

		mysqlType := goTypeToMySQLType(field.Type)
		colDef := fmt.Sprintf("`%s` %s", colName, mysqlType)

		// id 字段自动设为主键
		if colName == "id" {
			primaryKey = colName
			colDef += " NOT NULL"
			if isIntegerType(field.Type) && hasAutoIncrementTag(field, dbTagOptions) {
				colDef += " AUTO_INCREMENT"
			}
		}

		columns = append(columns, colDef)
	}

	if len(columns) == 0 {
		return fmt.Errorf("struct has no exportable fields for table creation")
	}

	if primaryKey != "" {
		columns = append(columns, fmt.Sprintf("PRIMARY KEY (`%s`)", primaryKey))
	}

	createSQL := fmt.Sprintf("CREATE TABLE `%s` (\n  %s\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
		tableName, strings.Join(columns, ",\n  "))

	fmt.Println("Auto create table:", createSQL)
	_, err := c.db.ExecContext(ctx, createSQL)
	if err != nil {
		return fmt.Errorf("auto create table '%s' failed: %w", tableName, err)
	}

	return nil
}
