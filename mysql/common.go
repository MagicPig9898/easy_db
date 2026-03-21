package mysql

import (
	"database/sql"
	"errors"
	"fmt"
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

// processMapNamedParams 将带有命名参数的SQL查询转换为标准SQL（使用?占位符）
// 参数:
//   - query: 原始SQL查询字符串，包含命名参数（如 :name, :age）
//   - params: 命名参数到具体值的映射
//
// 返回值:
//   - string: 转换后的SQL查询字符串，命名参数被替换为?
//   - []interface{}: 按顺序排列的参数值数组，对应转换后SQL中的?占位符
//   - error: 处理过程中的错误，包括参数缺失、重复参数等
//
// 功能说明:
//
//	该方法解析SQL查询中的命名参数（以冒号开头的标识符），将其替换为标准SQL的?占位符。
//	同时收集参数值并按顺序返回，以便与数据库驱动配合使用。
//	支持字符串字面量、单行注释、多行注释中的冒号跳过处理，避免误解析。
//	参数名支持字母、数字、下划线、点号以及方括号（用于数组/对象访问）。
//	注意：当前实现不支持同一参数的重复使用，每个命名参数在SQL中只能出现一次。
func processMapNamedParams(query string, params map[string]interface{}) (string, []interface{}, error) {
	var args []interface{}             // 存储每个 ? 对应的参数值, 顺序严格对应SQL中 ? 的顺序
	var argPositions []string          // 存储每个 ? 对应的参数名
	paramIndex := make(map[string]int) // 参数名 -> 在 args 中的索引, 用于检测重复参数
	var result strings.Builder         // 高效拼接字符串, 存储转换后的SQL
	result.Grow(len(query))            // 提前分配内存，避免频繁扩容
	i := 0                             // 使用 i 手动控制索引
	for i < len(query) {
		ch := query[i]
		// 跳过字符串字面量中的冒号
		if ch == '\'' || ch == '"' || ch == '`' { // 如果识别到'单引号'、"双引号"、`反引号`，则跳过
			start := i
			i++
			for i < len(query) {
				if query[i] == '\\' && i+1 < len(query) {
					// 跳过转义字符
					i += 2
					continue
				}
				if query[i] == ch { // 如果找到对应的结束符，则跳出循环
					break
				}
				i++ // 一直找到'单引号'、"双引号"、`反引号`对应的结束符
			}
			if i < len(query) {
				result.WriteString(query[start : i+1]) // 先保存，再移动索引，继续处理
				i++
			}
			continue
		}
		// 跳过注释中的冒号
		if ch == '-' && i+1 < len(query) && query[i+1] == '-' {
			// 单行注释
			start := i
			for i < len(query) && query[i] != '\n' {
				i++
			}
			result.WriteString(query[start:i])
			continue
		}
		if ch == '/' && i+1 < len(query) && query[i+1] == '*' {
			// 多行注释
			start := i
			i += 2
			for i+1 < len(query) && !(query[i] == '*' && query[i+1] == '/') {
				i++
			}
			if i+1 < len(query) {
				i += 2
			}
			result.WriteString(query[start:i])
			continue
		}
		if ch == ':' && i+1 < len(query) { // 如果识别到冒号，则提取参数名
			start := i + 1
			end := start
			// 提取参数名
			for end < len(query) {
				c := query[end]
				// 支持字母、数字、下划线、点号（用于对象访问）
				if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
					(c >= '0' && c <= '9') || c == '_' || c == '.' || c == '[' || c == ']' {
					end++
				} else {
					break
				}
			}
			if end > start {
				paramName := query[start:end]
				result.WriteByte('?')
				// 检查这个参数是否已经处理过
				if _, exists := paramIndex[paramName]; exists {
					// 重复参数，报错提示
					return "", nil,
						fmt.Errorf("Duplicate parameter: %s exist, named parameters are not currently supported", paramName)
				} else {
					// 新参数
					argPositions = append(argPositions, paramName)
					// 获取参数值
					val, ok := params[paramName]
					if !ok {
						// 若所需参数没有传入，报错提示
						return "", nil, fmt.Errorf("missing parameter: %s", paramName)
					}
					// 存储参数值和索引
					args = append(args, val)
					paramIndex[paramName] = len(args) - 1
				}
				i = end
				continue
			}
		}

		result.WriteByte(ch)
		i++
	}

	return result.String(), args, nil
}

// processStructNamedParams 将带有命名参数的SQL查询转换为标准SQL（使用?占位符）
// 参数:
//   - query: 原始SQL查询字符串，包含命名参数（如 :name, :age）
//   - params: 结构体类型的参数，结构体字段通过 db 标签指定参数名
//
// 返回值:
//   - string: 转换后的SQL查询字符串，命名参数被替换为?
//   - []interface{}: 按顺序排列的参数值数组，对应转换后SQL中的?占位符
//   - error: 处理过程中的错误，包括参数类型错误、参数缺失等
//
// 功能说明:
//
//	该方法将结构体类型的参数转换为map，然后调用 processMapNamedParams 处理SQL查询中的命名参数。
//	结构体字段通过 db 标签指定参数名，如果没有 db 标签或标签为"-"，则使用字段名的蛇形命名作为参数名。
//	未导出的字段（小写字母开头）会被忽略。
//	最终命名参数会被替换为标准SQL的?占位符，参数值按顺序返回。
func processStructNamedParams(query string, params interface{}) (string, []interface{}, error) {
	structVal := reflect.ValueOf(params)
	structType := reflect.TypeOf(params)
	if structVal.Kind() != reflect.Struct {
		return "", nil, fmt.Errorf("params not struct")
	}
	paramsMap := make(map[string]interface{})
	for i := 0; i < structVal.NumField(); i++ {
		fieldType := structType.Field(i)
		fieldVal := structVal.Field(i)
		// 忽略未导出字段
		if fieldType.PkgPath != "" {
			continue
		}
		// 读取 db tag
		name := fieldType.Tag.Get("db")
		if name == "" || name == "-" {
			name = toSnakeCase(fieldType.Name) // 注意这里不管dbtag是不是-，一律转为蛇形命令
		}
		paramsMap[name] = fieldVal.Interface()
	}

	return processMapNamedParams(query, paramsMap)
}
