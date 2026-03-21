// MySQL 客户端包
// 提供 MySQL 数据库连接和基本操作功能

package mysql

// 定义类型和配置

import (
	"database/sql"
	"time"
)

// Config MySQL配置
type Config struct {
	Host     string
	Port     int
	Username string
	Password string
	Database string
	// 可选配置
	MaxOpenConns    int           // 最大打开连接数
	MaxIdleConns    int           // 最大空闲连接数
	ConnMaxLifetime time.Duration // 连接最大生命周期
}

// Client MySQL客户端
type Client struct {
	db     *sql.DB
	config *Config
}

// QueryError 错误类型
type QueryError struct {
	Sql  string
	Args []interface{}
	Err  error
}

func (e *QueryError) Error() string {
	return "error: " + e.Err.Error()
}
