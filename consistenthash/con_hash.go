package consistenthash

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Map
// Map holds the ring state and load stats for rebalancing.
type Map struct {
	mu            sync.RWMutex
	config        *Config
	keys          []int
	hashMap       map[int]string
	nodeReplicas  map[string]int
	nodeCounts    map[string]int64
	totalRequests int64
}

type Option func(*Map)

// WithConfig
func WithConfig(config *Config) Option {
	return func(m *Map) {
		m.config = config
	}
}

// New
// New builds a hash ring and starts the load balancer loop.
func New(opts ...Option) *Map {
	m := &Map{
		config:       DefaultConfig,
		hashMap:      make(map[int]string),
		nodeReplicas: make(map[string]int),
		nodeCounts:   make(map[string]int64),
	}
	for _, opt := range opts {
		opt(m)
	}
	m.startBalancer()
	return m
}

// Add
// Add inserts nodes with virtual replicas into the ring.
func (m *Map) Add(nodes ...string) error {
	if len(nodes) == 0 {
		return errors.New("nodes can not be empty")
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, node := range nodes {
		if node == "" {
			continue
		}

		m.addNode(node, m.config.DefaultReplicas)
	}


	sort.Ints(m.keys)
	return nil
}

// Reomove
func (m *Map) Remove(node string) error {
	if len(node) == 0 {
		return errors.New("Invalid node ")
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.removeNodeLocked(node); err != nil {
		return err
	}
	return nil
}

func (m *Map) removeNodeLocked(node string) error {
	replicas := m.nodeReplicas[node]
	if replicas == 0 {
		return fmt.Errorf("node %s not found", node)
	}

	for i := 0; i < replicas; i++ {
		hash := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d", node, i))))
		delete(m.hashMap, hash)
		for j := 0; j < len(m.keys); j++ {
			if m.keys[j] == hash {
				m.keys = append(m.keys[:j], m.keys[j+1:]...)
				break
			}
		}
	}
	delete(m.nodeReplicas, node)
	delete(m.nodeCounts, node)
	return nil
}

// Get
// Get returns the node responsible for key and updates load stats.
func (m *Map) Get(key string) string {
	if key == "" {
		return ""
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.keys) == 0 {
		return ""
	}
	hash := int(m.config.HashFunc([]byte(key)))

	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})


	if idx == len(m.keys) {
		idx = 0
	}

	node := m.hashMap[m.keys[idx]]
	count := m.nodeCounts[node]
	m.nodeCounts[node] = count + 1
	atomic.AddInt64(&m.totalRequests, 1)
	return node
}

func (m *Map) addNode(node string, replicas int) {
	for i := 0; i < replicas; i++ {
		hash := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d", node, i))))
		m.hashMap[hash] = node
		m.keys = append(m.keys, hash)
	}
	m.nodeReplicas[node] = replicas
}

//checkAndBalance
// checkAndBalance evaluates load skew and triggers rebalance.
func (m *Map) checkAndBalance() {
	if atomic.LoadInt64(&m.totalRequests) < 1000 {
		return
	}


	m.mu.RLock()
	avgLoad := float64(atomic.LoadInt64(&m.totalRequests)) / float64(len(m.nodeReplicas))
	var maxDiff float64
	for _, count := range m.nodeCounts {
		diff := float64(count) - avgLoad
		if diff/avgLoad > maxDiff {
			maxDiff = diff / avgLoad
		}
	}
	m.mu.RUnlock()


	if maxDiff > m.config.LoadBalanceThreshold {
		m.rebalanceNodes()
	}
}

// startBalancer periodically checks load distribution.
func (m *Map) startBalancer() {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for range ticker.C {
			m.checkAndBalance()
		}
	}()
}

// rebalanceNodes
// rebalanceNodes adjusts virtual replicas based on observed load.
func (m *Map) rebalanceNodes() {
	m.mu.Lock()
	defer m.mu.Unlock()

	avgLoad := float64(atomic.LoadInt64(&m.totalRequests)) / float64(len(m.nodeReplicas))


	for node, count := range m.nodeCounts {
		replicas := m.nodeReplicas[node]
		loadRatio := float64(count) / avgLoad
		var newReplicas int
		if loadRatio > 1.0 {

			newReplicas = int(float64(replicas) / loadRatio)
		} else {

			newReplicas = int(float64(replicas) * (2 - loadRatio))
		}


		if newReplicas < m.config.MinReplicas {
			newReplicas = m.config.MinReplicas
		}
		if newReplicas > m.config.MaxReplicas {
			newReplicas = m.config.MaxReplicas
		}

		if newReplicas != replicas {

			if err := m.removeNodeLocked(node); err != nil {
				continue
			}
			m.addNode(node, newReplicas)
		}
	}

	for node := range m.nodeCounts {
		m.nodeCounts[node] = 0
	}
	atomic.StoreInt64(&m.totalRequests, 0)
	sort.Ints(m.keys)
}

//GetStats
func (m *Map) GetStats() map[string]int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]int64)
	total := atomic.LoadInt64(&m.totalRequests)
	if total == 0 {
		return stats
	}
	for node, count := range m.nodeCounts {
		stats[node] = count
	}
	return stats
}
