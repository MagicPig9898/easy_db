// Package mysql 提供MySQL数据库客户端的创建和管理功能
// 包含连接池配置、连接生命周期管理等核心功能
package mysql

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// NewMysqlClient 创建新的MySQL客户端
// 参数:
//   - host: MySQL服务器地址
//   - port: MySQL服务器端口
//   - username: 数据库用户名
//   - password: 数据库密码
//   - database: 数据库名称
//
// 返回:
//   - *Client: MySQL客户端实例
//   - error: 创建过程中的错误信息
//
// 使用默认连接池配置:
//   - MaxOpenConns: 25 (最大打开连接数)
//   - MaxIdleConns: 10 (最大空闲连接数)
//   - ConnMaxLifetime: 1小时 (连接最大生命周期)
func NewClient(host string, port int, username, password, database string) (*Client, error) {
	config := &Config{
		Host:            host,
		Port:            port,
		Username:        username,
		Password:        password,
		Database:        database,
		MaxOpenConns:    25,
		MaxIdleConns:    10,
		ConnMaxLifetime: time.Hour,
	}

	return NewMysqlClientWithConfig(config)
}

// NewMysqlClientWithConfig 使用配置创建MySQL客户端
// 参数:
//   - config: MySQL连接配置对象，包含所有连接参数
//
// 返回:
//   - *Client: MySQL客户端实例
//   - error: 创建过程中的错误信息
//
// 该方法允许使用自定义配置创建客户端，包括连接池参数等高级设置
func NewMysqlClientWithConfig(config *Config) (*Client, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		config.Username,
		config.Password,
		config.Host,
		config.Port,
		config.Database,
	)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, &QueryError{
			Sql: "",
			Err: err,
		}
	}

	// 测试连接
	if err = db.Ping(); err != nil {
		return nil, &QueryError{
			Sql: "",
			Err: err,
		}
	}

	// 设置连接池
	// database/sql 自带连接池，设置的不是"新建连接"，而是限制连接池行为
	if config.MaxOpenConns > 0 {
		db.SetMaxOpenConns(config.MaxOpenConns) // 最大打开连接数（正在使用 + 空闲），防止 MySQL 被打爆，控制并发上限
	}
	if config.MaxIdleConns > 0 {
		db.SetMaxIdleConns(config.MaxIdleConns) // 连接池中最大空闲连接数，减少频繁创建 / 销毁连接的开销
	}
	if config.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(config.ConnMaxLifetime) // 一个连接最多存活多久
	}

	return &Client{
		db:     db,
		config: config,
	}, nil
}

// Close 关闭数据库连接
func (c *Client) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// Ping 测试数据库连接
func (c *Client) Ping() error {
	return c.db.Ping()
}
