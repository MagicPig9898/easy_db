package mysql

import (
	"context"
	"errors"
	"reflect"
)

// QueryMany 执行查询并将结果扫描到目标切片中，并完成自定义sql和参数
// 参数:
//   - ctx: 上下文，用于控制查询的超时和取消
//   - dest: 目标切片指针，必须是 *[]T 类型，其中 T 为结构体
//   - query: SQL 查询语句
//   - args: 查询参数
//
// 返回值:
//   - error: 查询过程中的错误，包括参数错误、数据库错误等
//
// 功能说明:
//
//	该方法支持将多行查询结果自动映射到结构体切片中。通过反射自动匹配
//	数据库列名与结构体字段的 db 标签，实现灵活的结果集映射。
func (c *Client) QueryMany(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	// 参数检查
	if dest == nil {
		return &QueryError{Sql: query, Args: args, Err: errors.New("dest is nil")}
	}
	// 获取反射值
	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Ptr {
		return &QueryError{Sql: query, Args: args, Err: errors.New("dest not pointer")}
	}
	// 解引用指针，获取指向的值(安全执行 Elem()，因为 destVal 是指针)
	sliceVal := destVal.Elem()
	if sliceVal.Kind() != reflect.Slice {
		return &QueryError{Sql: query, Args: args, Err: errors.New("dest not slice")}
	}
	// 执行查询
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return &QueryError{Sql: query, Args: args, Err: err}
	}
	defer rows.Close()
	// 获取数据表列信息
	columns, err := rows.Columns()
	if err != nil {
		return &QueryError{Sql: query, Args: args, Err: err}
	}
	// 获取切片元素类型（结构体类型）
	elemType := sliceVal.Type().Elem()
	// 遍历结果集
	// 必须先调用 Next() 来准备第一行数据, 只有调用 Next() 返回 true 后，rows才能调用 Scan(), 因为
	// 1.惰性加载：结果集可能很大，不会一次性全部加载到内存，流式处理：数据是从数据库服务器"流式"传输过来的，
	// 2.Next() 请求下一批/下一行数据，Scan() 只能读取当前已准备好的数据
	for rows.Next() {
		// 创建新的结构体实例
		elem := reflect.New(elemType).Elem()
		// 扫描到结构体
		if err := c.scanRowToStruct(rows, columns, elem); err != nil {
			return &QueryError{Sql: query, Args: args, Err: err}
		}
		// 添加到切片
		// 切片值在反射中是不可变的
		// 错误做法：直接赋值不会生效
		// sliceVal = reflect.Append(sliceVal, elem)  // 只改变了局部变量
		// 正确做法：需要 Set() 来修改指针指向的值
		sliceVal.Set(reflect.Append(sliceVal, elem))
	}
	// 检查遍历错误
	// 假设在这个循环过程中：
	// 1. 数据库连接突然断开
	// 2. 网络中断
	// 3. 内存不足
	// 4. 数据格式异常
	// 这些错误可能不会在 rows.Next() 或 rows.Scan() 中立即抛出
	if err := rows.Err(); err != nil {
		return &QueryError{Sql: query, Args: args, Err: err}
	}
	return nil
}

// QueryOne 执行单行查询并将结果扫描到目标结构体中，并完成自定义sql和参数
// 参数:
//   - ctx: 上下文
//   - dest: 目标结构体指针
//   - query: SQL查询语句
//   - args: 查询参数
//
// 返回:
//   - error: 查询过程中的错误，包括参数错误、数据库错误等
//
// 功能说明:
//
//	该方法支持将单行查询结果自动映射到结构体中。通过反射自动匹配
//	数据库列名与结构体字段的 db 标签，实现灵活的结果集映射。
func (c *Client) QueryOne(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	// 参数检查
	if dest == nil {
		return &QueryError{Sql: query, Args: args, Err: errors.New("dest is nil")}
	}
	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Ptr {
		return &QueryError{Sql: query, Args: args, Err: errors.New("dest not pointer")}
	}
	structVal := destVal.Elem()
	if structVal.Kind() != reflect.Struct {
		return &QueryError{Sql: query, Args: args, Err: errors.New("dest not struct")}
	}
	// 执行查询
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return &QueryError{Sql: query, Args: args, Err: err}
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return &QueryError{Sql: query, Args: args, Err: err}
	}
	if rows.Next() { // 只尝试获取一行
		if err := c.scanRowToStruct(rows, columns, structVal); err != nil {
			return &QueryError{Sql: query, Args: args, Err: err}
		}
		// 检查迭代过程中是否有错误
		if err := rows.Err(); err != nil {
			return &QueryError{Sql: query, Args: args, Err: err}
		}
	}
	return &QueryError{Sql: query, Args: args, Err: errors.New("no data")}
}

// 命名传参
// 参数:
//   - ctx: 上下文
//   - dest: 目标切片指针，必须是 *[]T 类型，其中 T 为结构体
//   - query: SQL 查询语句，使用命名参数（如 :username, :id）
//   - params: 参数结构体，字段通过 db 标签指定参数名
//
// 返回:
//   - error: 查询过程中的错误，包括参数错误、数据库错误等
//
// 功能说明:
//
//	该方法支持将多行查询结果自动映射到结构体切片中，并使用命名参数。
//	通过反射自动匹配数据库列名与结构体字段的 db 标签，实现灵活的结果集映射。
//	命名参数语法为 :paramName，会自动替换为标准 SQL 的 ? 占位符。
func (c *Client) QueryManyNamed(ctx context.Context, dest interface{}, query string, params interface{}) error {
	// 将命名参数转换为位置参数
	processedQuery, positionalArgs, err := processStructNamedParams(query, params)
	if err != nil {
		return &QueryError{Sql: query, Args: positionalArgs, Err: err}
	}
	return c.QueryMany(ctx, dest, processedQuery, positionalArgs...)
}
