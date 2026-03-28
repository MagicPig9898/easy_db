package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

func (c *Client) DeleteOne(ctx context.Context, deleteSql string, args ...interface{}) error {
	if err := validateDeleteSQL(deleteSql); err != nil {
		return &QueryError{Sql: deleteSql, Args: args, Err: err}
	}

	result, err := c.executeDelete(ctx, deleteSql, args...)
	if err != nil {
		return &QueryError{Sql: deleteSql, Args: args, Err: err}
	}

	if err := validateSingleDeleteAffected(result); err != nil {
		return &QueryError{Sql: deleteSql, Args: args, Err: err}
	}

	return nil
}

func (c *Client) DeleteOneNamed(ctx context.Context, deleteSql string, dest interface{}) error {
	if err := validateDeleteSQL(deleteSql); err != nil {
		return &QueryError{Sql: deleteSql, Err: err}
	}

	processedQuery, positionalArgs, err := processDeleteStructNamedParams(deleteSql, dest)
	if err != nil {
		return &QueryError{Sql: deleteSql, Args: positionalArgs, Err: err}
	}

	result, err := c.executeDelete(ctx, processedQuery, positionalArgs...)
	if err != nil {
		return &QueryError{Sql: processedQuery, Args: positionalArgs, Err: err}
	}

	if err := validateSingleDeleteAffected(result); err != nil {
		return &QueryError{Sql: processedQuery, Args: positionalArgs, Err: err}
	}

	return nil
}

func validateDeleteSQL(deleteSql string) error {
	if strings.TrimSpace(deleteSql) == "" {
		return errors.New("delete sql is empty")
	}
	return nil
}

func (c *Client) executeDelete(ctx context.Context, deleteSql string, args ...interface{}) (sql.Result, error) {
	return c.db.ExecContext(ctx, deleteSql, args...)
}

func validateSingleDeleteAffected(result sql.Result) error {
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return fmt.Errorf("delete one expect 1 row affected, got %d", rowsAffected)
	}
	return nil
}

func processDeleteStructNamedParams(query string, params interface{}) (string, []interface{}, error) {
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
