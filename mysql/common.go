package mysql

import (
	"database/sql"
	"errors"
	"reflect"
	"strings"
)

// scanRowToStruct 扫描当前行数据到结构体
// 参数:
//   - rows: 数据库查询结果集
//   - columns: 数据库列名列表
//   - dest: 目标结构体指针
//
// 返回值:
//   - error: 扫描过程中的错误，包括参数错误、数据库错误等
//
// 功能说明:
//
//	该方法根据数据库列名和结构体字段的 db 标签，将当前行数据扫描到目标结构体中。
//	如果数据库列名与结构体字段的 db 标签不匹配，会使用字段名的小写作为匹配键。
func (c *Client) scanRowToStruct(rows *sql.Rows, columns []string, dest reflect.Value) error {
	if dest.Kind() != reflect.Struct {
		return errors.New("scan row to struct need a struct, but dest isn't")
	}
	// 准备指针集
	values := make([]interface{}, len(columns)) // 收集各个字段的指针，列数需要和数据库中的列数一致
	fieldMap := parseStructFieldMap(dest)

	for i, col := range columns {
		if colPtr, ok := fieldMap[col]; ok {
			values[i] = colPtr
		} else {
			var placeholder interface{}
			values[i] = &placeholder // 占位符指针
		}
	}
	// 关键：Scan 函数会将数据库中的数据直接写入这些指针指向的内存位置
	// 扫描当前行数据到指针
	return rows.Scan(values...)
}

// parseStructFieldMap 解析结构体字段的 db 标签
// 参数:
//   - dest: 目标结构体指针
//
// 返回值:
//   - map[string]interface{}: 字段名到指针的映射
//
// 功能说明:
//
//	该方法根据结构体字段的 db 标签，将字段名映射到对应的指针。
//	如果字段没有 db 标签，会使用字段名的小写作为映射键。
func parseStructFieldMap(dest reflect.Value) map[string]interface{} {
	fieldMap := make(map[string]interface{})
	// 这里再次的Type()，但是这次不是sliceVal.Type()，而是structVal.Type()
	destType := dest.Type()

	for i := 0; i < destType.NumField(); i++ {
		// 所以可以直接通过 destType.Field(i) 获取字段的元数据
		// field: 到这一步，field中拥有的就是结构体中每一个字段的信息
		field := destType.Field(i)

		// 获取db tag
		tag := field.Tag.Get("db")
		if tag == "-" {
			continue
		}
		if tag == "" {
			// 如果没有db tag，使用字段名的小写
			tag = toSnakeCase(field.Name)
		}

		// 添加到映射
		fieldMap[tag] = dest.Field(i).Addr().Interface()
	}

	return fieldMap
}

// toSnakeCase 将驼峰命名法转换为下划线命名法
// 参数:
//   - s: 输入字符串
//
// 返回值:
//   - string: 转换后的下划线命名法字符串
//
// 功能说明:
//
//	该方法将输入字符串中的大写字母转换为小写字母，并在大写字母之间添加下划线。
//	例如，"HelloWorld" 转换为 "hello_world"。
func toSnakeCase(s string) string {
	var result []rune
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			result = append(result, '_')
		}
		result = append(result, r)
	}
	return strings.ToLower(string(result))
}
