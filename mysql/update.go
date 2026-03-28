package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// UpdateOne 更新一条记录, 非命名参数，需要自己按顺序传递
func (c *Client) UpdateOne(ctx context.Context, updateSql string, args ...interface{}) error {
	if err := validateUpdateSQL(updateSql); err != nil {
		return &QueryError{Sql: updateSql, Args: args, Err: err}
	}

	result, err := c.executeUpdate(ctx, updateSql, args...)
	if err != nil {
		return &QueryError{Sql: updateSql, Args: args, Err: err}
	}

	if err := validateSingleUpdateAffected(result); err != nil {
		return &QueryError{Sql: updateSql, Args: args, Err: err}
	}

	return nil
}

// UpdateOneNamed 更新一条记录
// dest 是一个结构体指针，用于存储更新后的记录
func (c *Client) UpdateOneNamed(ctx context.Context, updateSql string, dest interface{}) error {
	if err := validateUpdateSQL(updateSql); err != nil {
		return &QueryError{Sql: updateSql, Err: err}
	}

	processedQuery, positionalArgs, err := processUpdateStructNamedParams(updateSql, dest)
	if err != nil {
		return &QueryError{Sql: updateSql, Args: positionalArgs, Err: err}
	}

	result, err := c.executeUpdate(ctx, processedQuery, positionalArgs...)
	if err != nil {
		return &QueryError{Sql: processedQuery, Args: positionalArgs, Err: err}
	}

	if err := validateSingleUpdateAffected(result); err != nil {
		return &QueryError{Sql: processedQuery, Args: positionalArgs, Err: err}
	}

	return nil
}

func validateUpdateSQL(updateSql string) error {
	if strings.TrimSpace(updateSql) == "" {
		return errors.New("update sql is empty")
	}
	return nil
}

func (c *Client) executeUpdate(ctx context.Context, updateSql string, args ...interface{}) (sql.Result, error) {
	return c.db.ExecContext(ctx, updateSql, args...)
}

func validateSingleUpdateAffected(result sql.Result) error {
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return fmt.Errorf("update one expect 1 row affected, got %d", rowsAffected)
	}
	return nil
}

func processUpdateStructNamedParams(query string, params interface{}) (string, []interface{}, error) {
	structVal := reflect.ValueOf(params)
	if structVal.Kind() != reflect.Ptr || structVal.IsNil() {
		return "", nil, fmt.Errorf("params not pointer")
	}

	structVal = structVal.Elem()
	if structVal.Kind() != reflect.Struct {
		return "", nil, fmt.Errorf("params not struct pointer")
	}

	structType := structVal.Type()
	paramsMap := make(map[string]interface{})
	for i := 0; i < structVal.NumField(); i++ {
		fieldType := structType.Field(i)
		fieldVal := structVal.Field(i)
		if fieldType.PkgPath != "" {
			continue
		}

		name := fieldType.Tag.Get("db")
		if name == "-" {
			continue
		}
		if name == "" {
			name = toSnakeCase(fieldType.Name)
		}
		paramsMap[name] = fieldVal.Interface()
	}

	return processMapNamedParams(query, paramsMap)
}
