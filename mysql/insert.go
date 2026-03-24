package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// InsertOne 执行单条插入语句（INSERT）并校验只影响 1 行记录。
// 参数:
//   - ctx: 上下文，用于控制超时、取消请求，避免数据库操作无限阻塞
//   - insertSql: 插入 SQL，通常形如 INSERT INTO ... VALUES (...)
//   - args: SQL 占位符参数，按顺序对应 insertSql 中的 ? 位置
//
// 返回:
//   - error: 当 SQL 非法、执行失败、或影响行数不为 1 时返回错误
//
// 设计说明:
//
//	该方法参考 QueryOne 的错误封装风格，统一返回 QueryError，
//	便于上层定位是哪个 SQL 以及对应参数导致问题。
//	方法内部将流程拆分为“校验 SQL -> 执行插入 -> 校验影响行数”三个步骤，
//	以保证主流程短小清晰，后续维护或扩展（如加审计、重试）更容易。
func (c *Client) InsertOne(ctx context.Context, insertSql string, args ...interface{}) error {
	// 1) 基础校验：避免空 SQL 直接打到数据库层
	if err := validateInsertSQL(insertSql); err != nil {
		return &QueryError{Sql: insertSql, Args: args, Err: err}
	}

	// 2) 执行插入：调用 ExecContext，让 ctx 生效（可超时/取消）
	result, err := c.executeInsert(ctx, insertSql, args...)
	if err != nil {
		return &QueryError{Sql: insertSql, Args: args, Err: err}
	}

	// 3) 语义校验：InsertOne 约定必须“只插入 1 行”
	if err := validateSingleRowAffected(result); err != nil {
		return &QueryError{Sql: insertSql, Args: args, Err: err}
	}

	return nil
}

// InsertOneNamed 执行单条命名参数插入，并校验只影响 1 行记录。
// 参数:
//   - ctx: 上下文，用于控制超时、取消请求
//   - insertSql: 使用命名参数的插入 SQL，例如 VALUES (:name, :age)
//   - dest: 结构体指针，字段通过 db 标签映射参数名；无 db 标签时使用蛇形小写字段名
//
// 返回:
//   - error: 当参数解析失败、执行失败或影响行数不为 1 时返回错误
//
// 处理流程:
// 1. 校验 SQL 非空
// 2. 将结构体指针转为命名参数并转换为位置参数
// 3. 执行插入并校验影响行数
func (c *Client) InsertOneNamed(ctx context.Context, insertSql string, dest interface{}) error {
	if err := validateInsertSQL(insertSql); err != nil {
		return &QueryError{Sql: insertSql, Err: err}
	}

	processedQuery, positionalArgs, err := processInsertStructNamedParams(insertSql, dest)
	if err != nil {
		return &QueryError{Sql: insertSql, Args: positionalArgs, Err: err}
	}

	result, err := c.executeInsert(ctx, processedQuery, positionalArgs...)
	if err != nil {
		return &QueryError{Sql: processedQuery, Args: positionalArgs, Err: err}
	}

	if err := validateSingleRowAffected(result); err != nil {
		return &QueryError{Sql: processedQuery, Args: positionalArgs, Err: err}
	}

	return nil
}

// InsertManyNamed 批量执行命名参数插入，采用“单 SQL 多值 + 分批 + 事务”策略。
// 核心流程:
// 1. 先将命名参数 SQL 转为位置参数 SQL，并提取 VALUES 的单行模板
// 2. 按 batchSize 组装成 INSERT ... VALUES (...), (...), ... 批量语句
// 3. 在同一事务中逐批执行，任一批失败立即回滚
//
// 说明:
//   - dest 要求是结构体切片指针（*[]T 或 *[]*T）
//   - 字段取值规则与 InsertOneNamed 一致：优先 db tag，其次蛇形字段名
//   - 每批执行后会校验 RowsAffected，确保写入条数符合预期
func (c *Client) InsertManyNamed(ctx context.Context, insertSql string, dest interface{}) error {
	if err := validateInsertSQL(insertSql); err != nil {
		return &QueryError{Sql: insertSql, Err: err}
	}
	records, err := parseInsertManyDest(dest)
	if err != nil {
		return &QueryError{Sql: insertSql, Err: err}
	}
	// 用首条记录解析出“标准位置参数 SQL”与参数数量，后续批次复用该模板
	singleSQL, firstArgs, err := processInsertStructNamedParams(insertSql, records[0])
	if err != nil {
		return &QueryError{Sql: insertSql, Args: firstArgs, Err: err}
	}
	argsPerRecord := len(firstArgs)
	if argsPerRecord == 0 {
		return &QueryError{Sql: singleSQL, Err: fmt.Errorf("no positional args generated for insert")}
	}
	// 将 INSERT SQL 拆成 prefix + tuple + suffix，便于拼接多值批量语句
	prefix, tuple, suffix, err := splitInsertSQLTemplate(singleSQL)
	if err != nil {
		return &QueryError{Sql: singleSQL, Err: err}
	}
	// 事务保证批量写入的原子性：中途任一错误都会回滚
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return &QueryError{Sql: insertSql, Err: err}
	}
	successCount := 0
	const batchSize = 200
	// 分批执行，控制单条 SQL 大小，避免语句过长或触发包大小限制
	for start := 0; start < len(records); start += batchSize {
		end := start + batchSize
		if end > len(records) {
			end = len(records)
		}
		currentBatchCount := end - start
		currentBatchSQL := buildBatchInsertSQL(prefix, tuple, suffix, currentBatchCount)
		currentBatchArgs := make([]interface{}, 0, argsPerRecord*currentBatchCount)
		for i := start; i < end; i++ {
			if i == 0 {
				currentBatchArgs = append(currentBatchArgs, firstArgs...)
				continue
			}
			processedQuery, positionalArgs, err := processInsertStructNamedParams(insertSql, records[i])
			if err != nil {
				_ = tx.Rollback()
				return &QueryError{Sql: insertSql, Args: positionalArgs, Err: err}
			}
			if processedQuery != singleSQL {
				_ = tx.Rollback()
				return &QueryError{
					Sql:  processedQuery,
					Args: positionalArgs,
					Err:  fmt.Errorf("processed insert sql mismatch in batch"),
				}
			}
			if len(positionalArgs) != argsPerRecord {
				_ = tx.Rollback()
				return &QueryError{
					Sql:  processedQuery,
					Args: positionalArgs,
					Err:  fmt.Errorf("args count mismatch in batch, expect %d got %d", argsPerRecord, len(positionalArgs)),
				}
			}
			currentBatchArgs = append(currentBatchArgs, positionalArgs...)
		}
		// 一批只发一次 SQL，显著减少数据库往返次数
		result, err := tx.ExecContext(ctx, currentBatchSQL, currentBatchArgs...)
		if err != nil {
			_ = tx.Rollback()
			return &QueryError{Sql: currentBatchSQL, Args: currentBatchArgs, Err: err}
		}
		if err := validateExpectedRowsAffected(result, int64(currentBatchCount)); err != nil {
			_ = tx.Rollback()
			return &QueryError{Sql: currentBatchSQL, Args: currentBatchArgs, Err: err}
		}
		successCount += currentBatchCount
	}
	if successCount != len(records) {
		_ = tx.Rollback()
		return &QueryError{
			Sql: insertSql,
			Err: fmt.Errorf("insert many expect %d success, got %d", len(records), successCount),
		}
	}
	// 全部批次成功后统一提交事务
	if err := tx.Commit(); err != nil {
		return &QueryError{Sql: insertSql, Err: err}
	}
	return nil
}

func splitInsertSQLTemplate(insertSQL string) (string, string, string, error) {
	lowerSQL := strings.ToLower(insertSQL)
	valuesKeywordPos := strings.LastIndex(lowerSQL, "values")
	if valuesKeywordPos == -1 {
		return "", "", "", fmt.Errorf("insert sql must contain values keyword")
	}

	valuesBody := insertSQL[valuesKeywordPos+len("values"):]
	tupleStartInValues := strings.Index(valuesBody, "(")
	if tupleStartInValues == -1 {
		return "", "", "", fmt.Errorf("insert sql values clause missing tuple")
	}

	tupleStartPos := valuesKeywordPos + len("values") + tupleStartInValues
	tupleEndPos, err := findRightParenthesis(insertSQL, tupleStartPos)
	if err != nil {
		return "", "", "", err
	}

	prefix := insertSQL[:tupleStartPos]
	tuple := insertSQL[tupleStartPos : tupleEndPos+1]
	suffix := insertSQL[tupleEndPos+1:]
	return prefix, tuple, suffix, nil
}

func findRightParenthesis(s string, leftPos int) (int, error) {
	depth := 0
	inSingleQuote := false
	inDoubleQuote := false
	inBacktick := false

	for i := leftPos; i < len(s); i++ {
		ch := s[i]
		if ch == '\\' && i+1 < len(s) {
			i++
			continue
		}

		if !inDoubleQuote && !inBacktick && ch == '\'' {
			inSingleQuote = !inSingleQuote
			continue
		}
		if !inSingleQuote && !inBacktick && ch == '"' {
			inDoubleQuote = !inDoubleQuote
			continue
		}
		if !inSingleQuote && !inDoubleQuote && ch == '`' {
			inBacktick = !inBacktick
			continue
		}
		if inSingleQuote || inDoubleQuote || inBacktick {
			continue
		}

		if ch == '(' {
			depth++
			continue
		}
		if ch == ')' {
			depth--
			if depth == 0 {
				return i, nil
			}
		}
	}

	return -1, fmt.Errorf("insert sql values tuple parenthesis not closed")
}

func buildBatchInsertSQL(prefix, tuple, suffix string, batchCount int) string {
	var builder strings.Builder
	builder.Grow(len(prefix) + len(tuple)*batchCount + len(suffix) + batchCount)
	builder.WriteString(prefix)
	for i := 0; i < batchCount; i++ {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(tuple)
	}
	builder.WriteString(suffix)
	return builder.String()
}

func validateExpectedRowsAffected(result sql.Result, expected int64) error {
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != expected {
		return fmt.Errorf("expect %d rows affected, got %d", expected, rowsAffected)
	}
	return nil
}

// validateInsertSQL 校验插入 SQL 的有效性。
// 当前仅检查“去除空白后不能为空”，
// 后续可按需要扩展（例如限制必须以 INSERT 开头等）。
func validateInsertSQL(insertSql string) error {
	if strings.TrimSpace(insertSql) == "" {
		return errors.New("insert sql is empty")
	}
	return nil
}

// executeInsert 执行插入语句并返回 sql.Result。
// 单独抽出该方法的目的：
// 1. 降低 InsertOne 主方法复杂度
// 2. 便于后续扩展统一埋点、日志或重试策略
func (c *Client) executeInsert(ctx context.Context, insertSql string, args ...interface{}) (sql.Result, error) {
	return c.db.ExecContext(ctx, insertSql, args...)
}

// validateSingleRowAffected 校验插入结果影响行数是否为 1。
// InsertOne 的语义是“单条插入”，因此影响行数必须严格等于 1：
//   - = 1: 符合预期
//   - = 0: 可能 SQL 条件不满足或数据库策略导致未写入
//   - > 1: 与 InsertOne 语义不符，应视为异常并返回错误
func validateSingleRowAffected(result sql.Result) error {
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return fmt.Errorf("insert one expect 1 row affected, got %d", rowsAffected)
	}
	return nil
}

// processInsertStructNamedParams 将结构体参数转换为命名参数格式。
// 1. 校验 params 是否为指针非 nil 结构体实例
// 2. 遍历结构体字段，提取 db 标签值作为参数名，忽略未导出字段和 db:"-" 标签
// 3. 使用 processMapNamedParams 处理参数映射，生成最终 SQL 和参数列表
func processInsertStructNamedParams(sql string, params interface{}) (string, []interface{}, error) {
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
		if fieldType.PkgPath != "" { // 忽略未导出字段, PkgPath 非空表示未导出
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

	return processMapNamedParams(sql, paramsMap)
}

// parseInsertManyDest 解析批量插入参数，支持 *[]T 和 *[]*T 两种输入形式。
// 返回值中的每个元素都会被规范化为“结构体指针”，供后续命名参数解析复用。
func parseInsertManyDest(dest interface{}) ([]interface{}, error) {
	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Ptr || destVal.IsNil() {
		return nil, fmt.Errorf("dest not pointer")
	}

	sliceVal := destVal.Elem()
	if sliceVal.Kind() != reflect.Slice {
		return nil, fmt.Errorf("dest not slice pointer")
	}
	if sliceVal.Len() == 0 {
		return nil, fmt.Errorf("dest slice is empty")
	}

	records := make([]interface{}, 0, sliceVal.Len())
	for i := 0; i < sliceVal.Len(); i++ {
		item := sliceVal.Index(i)
		switch item.Kind() {
		case reflect.Ptr:
			if item.IsNil() || item.Elem().Kind() != reflect.Struct {
				return nil, fmt.Errorf("dest[%d] not struct pointer", i)
			}
			records = append(records, item.Interface())
		case reflect.Struct:
			records = append(records, item.Addr().Interface())
		default:
			return nil, fmt.Errorf("dest[%d] not struct", i)
		}
	}
	return records, nil
}
