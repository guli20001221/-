package consistenthash

import "hash/crc32"

// Config
// Config controls replica counts and balancing thresholds.
type Config struct {

	DefaultReplicas int

	MinReplicas int

	MaxReplicas int

	HashFunc func(data []byte) uint32

	LoadBalanceThreshold float64
}

// DefaultConfig
// DefaultConfig provides baseline ring settings.
var DefaultConfig = &Config{
	DefaultReplicas:      50,
	MinReplicas:          10,
	MaxReplicas:          200,
	HashFunc:             crc32.ChecksumIEEE,
	LoadBalanceThreshold: 0.25,
}
