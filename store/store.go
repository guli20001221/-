// Package store 提供了缓存存储的接口定义和基础配置
package store

import "time"

// Value 接口定义了缓存值的接口，要求实现Len()方法
// 用于获取值的大小（以字节为单位），便于缓存大小管理
type Value interface {
	Len() int
}

// Store 接口定义了缓存存储的基本操作接口
// 包括获取、设置、删除、清理等操作
type Store interface {
	// Get 从缓存中获取指定key的值
	// 返回值和是否存在
	Get(key string) (Value, bool)

	// Set 设置key-value到缓存
	Set(key string, value Value) error

	// SetWithExpiration 设置带过期时间的key-value到缓存
	SetWithExpiration(key string, value Value, expiration time.Duration) error

	// Delete 从缓存中删除指定key的值
	// 返回是否成功删除
	Delete(key string) bool

	// Clear 清空所有缓存
	Clear()

	// Len 获取缓存中条目的数量
	Len() int

	// Close 关闭缓存，释放相关资源
	Close()
}

// CacheType 定义了缓存类型的字符串类型
// 用于区分不同的缓存算法实现
type CacheType string

// 定义了可用的缓存类型常量
const (
	LRU  CacheType = "lru"  // LRU(Least Recently Used) 最近最少使用算法
	LRU2 CacheType = "lru2" // LRU-2(Least Recently Used 2) 一种改进的LRU算法
)

// Options 结构体定义了缓存的配置选项
// 包含最大字节数、桶数量、每个桶的容量等参数
type Options struct {
	MaxBytes        int64                         // 最大的缓存字节数（用于 lru）
	BucketCount     uint16                        // 缓存的桶数量（用于 lru-2）
	CapPerBucket    uint16                        // 每个桶的容量（用于 lru-2）
	Level2Cap       uint16                        // lru-2 中二级缓存的容量（用于 lru-2）
	CleanupInterval time.Duration                 // 清理过期键的时间间隔
	OnEvicted       func(key string, value Value) // 当缓存项被驱逐时的回调函数
}

// NewOptions 返回默认的缓存配置选项
func NewOptions() Options {
	return Options{
		MaxBytes:        8192,        // 默认最大缓存8KB
		BucketCount:     16,          // 默认桶数量为16
		CapPerBucket:    512,         // 默认每个桶容量为512
		Level2Cap:       256,         // 默认二级缓存容量为256
		CleanupInterval: time.Minute, // 默认每分钟清理一次过期键
		OnEvicted:       nil,         // 默认没有驱逐回调
	}
}

// NewStore 根据指定的缓存类型创建相应的缓存实例
// 如果类型不匹配，默认返回LRU缓存
func NewStore(cacheType CacheType, options Options) Store {
	switch cacheType {
	case LRU:
		// 返回LRU缓存实现
		return newLRUCache(options)
	case LRU2:
		// 返回LRU2缓存实现
		return newLRU2Cache(options)
	default:
		return newLRUCache(options)
	}
}
