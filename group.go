package Group_Cache

import (
	"Group_Cache/singleflight"
	"context"
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"sync"
	"sync/atomic"
	"time"
)

var (
	groupsMu sync.RWMutex
	groups   = make(map[string]*Group)
)

// ErrKeyRequired
var ErrKeyRequired = errors.New("key is required")

// ErrValueRequired
var ErrValueRequired = errors.New("value is required")

// ErrGroupClosed
var ErrGroupClosed = errors.New("group closed")

// Getter
type Getter interface {
	Get(ctx context.Context, key string) ([]byte, error)
}

// GetterFunc Getter
type GetterFunc func(ctx context.Context, key string) ([]byte, error)

// Get Getter
func (f GetterFunc) Get(ctx context.Context, key string) ([]byte, error) { return f(ctx, key) }

// Group
// Group is a named cache namespace with optional peer replication.
type Group struct {
	name       string
	getter     Getter
	mainCache  *Cache
	peers      PeerPicker
	loader     *singleflight.Group
	expiration time.Duration
	closed     int32
	stats      groupStats
}

// groupStats
type groupStats struct {
	loads        int64
	localMisses  int64
	localHits    int64
	peerHits     int64
	peerMisses   int64
	loaderHits   int64
	loaderErrors int64
	loadDuration int64
}

// GroupOption
type GroupOption func(*Group)

// WithExpiration
func WithExpiration(expiration time.Duration) GroupOption {
	return func(g *Group) {
		g.expiration = expiration
	}
}

// WithPeers
func WithPeers(peers PeerPicker) GroupOption {
	return func(g *Group) {
		g.peers = peers
	}
}

// WithCacheOptions
func WithCacheOptions(opts CacheOptions) GroupOption {
	return func(g *Group) {
		g.mainCache = NewCache(opts)
	}
}

// NewGroup Group
// NewGroup constructs a group, applies options, and registers it globally.
func NewGroup(name string, cacheBytes int64, getter Getter, opts ...GroupOption) *Group {
	if getter == nil {
		panic("nil Getter")
	}

	cacheOpts := DefaultCacheOptions()
	cacheOpts.MaxBytes = cacheBytes
	g := &Group{
		name:      name,
		getter:    getter,
		mainCache: NewCache(cacheOpts),
		loader:    &singleflight.Group{},
	}


	for _, opt := range opts {
		opt(g)
	}


	groupsMu.Lock()
	defer groupsMu.Unlock()

	if _, dup := groups[name]; dup {
		panic("duplicate registration of group " + name)
	}

	groups[name] = g
	logrus.Infof("cache group %s created with cacheBytes= %d,expiration= %s", name, cacheBytes, g.expiration)
	return g
}

// GetGroup
func GetGroup(name string) *Group {
	groupsMu.RLock()
	defer groupsMu.RUnlock()
	return groups[name]
}

// Get
// Get reads from local cache first, then falls back to load().
func (g *Group) Get(ctx context.Context, key string) (ByteView, error) {

	if atomic.LoadInt32(&g.closed) == 1 {
		return ByteView{}, ErrGroupClosed
	}

	if key == "" {
		return ByteView{}, ErrKeyRequired
	}


	if v, ok := g.mainCache.Get(ctx, key); ok {
		atomic.AddInt64(&g.stats.localHits, 1)
		return v, nil
	}
	atomic.AddInt64(&g.stats.localMisses, 1)
	return g.load(ctx, key)
}

// Set
func (g *Group) Set(ctx context.Context, key string, value []byte) error {

	if atomic.LoadInt32(&g.closed) == 1 {
		return ErrGroupClosed
	}

	if key == "" {
		return ErrKeyRequired
	}
	if len(value) == 0 {
		return ErrValueRequired
	}


	isPeerRequest := ctx.Value("from_peer") != nil


	view := ByteView{b: cloneBytes(value)}


	if g.expiration > 0 {
		g.mainCache.AddWithExpiration(key, view, time.Now().Add(g.expiration))
	} else {
		g.mainCache.Add(key, view)
	}


	if !isPeerRequest && g.peers != nil {
		go g.syncToPeers(ctx, "set", key, value)
	}
	return nil
}

// Delete
func (g *Group) Delete(ctx context.Context, key string) error {
	if atomic.LoadInt32(&g.closed) == 1 {
		return ErrGroupClosed
	}
	if key == "" {
		return ErrKeyRequired
	}


	g.mainCache.Delete(key)


	isPeerRequest := ctx.Value("from_peer") != nil


	if !isPeerRequest && g.peers != nil {
		go g.syncToPeers(ctx, "delete", key, nil)
	}
	return nil
}

// syncToPeers
// syncToPeers sends mutations to the responsible peer (best-effort).
func (g *Group) syncToPeers(ctx context.Context, op string, key string, value []byte) {
	if g.peers == nil {
		return
	}


	peer, ok, isSelf := g.peers.PickPeer(key)
	if !ok || isSelf {
		return
	}


	syncCtx := context.WithValue(ctx, "from_peer", true)

	var err error
	switch op {
	case "set":
		err = peer.Set(syncCtx, g.name, key, value)
	case "delete":
		_, err = peer.Delete(syncCtx, g.name, key)
	}

	if err != nil {
		logrus.Errorf("sync %s to peer %s failed: %v", op, peer, err)
	}
}

// Clear
func (g *Group) Clear() {

	if atomic.LoadInt32(&g.closed) == 1 {
		return
	}
	g.mainCache.Clear()
	logrus.Infof("cache group %s cleared", g.name)
}

//Close

func (g *Group) Close() error {

	if !atomic.CompareAndSwapInt32(&g.closed, 0, 1) {
		return nil
	}


	if g.mainCache != nil {
		g.mainCache.Close()
	}


	groupsMu.Lock()
	defer groupsMu.Unlock()
	delete(groups, g.name)
	logrus.Infof("cache group %s closed", g.name)
	return nil
}

// load
// load uses singleflight to collapse concurrent loads of the same key.
func (g *Group) load(ctx context.Context, key string) (value ByteView, err error) {
	//singleflight.Group
	startTime := time.Now()
	viewi, err := g.loader.Do(key, func() (interface{}, error) { return g.loadData(ctx, key) })


	loadDuration := time.Since(startTime).Nanoseconds()
	atomic.AddInt64(&g.stats.loadDuration, loadDuration)
	atomic.AddInt64(&g.stats.loads, 1)

	if err != nil {
		atomic.AddInt64(&g.stats.loaderErrors, 1)
		return ByteView{}, err
	}
	view := viewi.(ByteView)


	if g.expiration > 0 {
		g.mainCache.AddWithExpiration(key, view, time.Now().Add(g.expiration))
	} else {
		g.mainCache.Add(key, view)
	}
	return view, nil
}

// loadData
// loadData tries peers first, then falls back to the user getter.
func (g *Group) loadData(ctx context.Context, key string) (interface{}, error) {

	if g.peers != nil {
		if peer, ok, isSelf := g.peers.PickPeer(key); ok && !isSelf {
			value, err := g.getFromPeer(ctx, peer, key)
			if err == nil {
				atomic.AddInt64(&g.stats.peerHits, 1)
				return value, nil
			}
			atomic.AddInt64(&g.stats.peerMisses, 1)
			logrus.Warnf("Failed to get value from peer : %v", err)
		}
	}


	bytes, err := g.getter.Get(ctx, key)
	if err != nil {
		return ByteView{}, fmt.Errorf("getter failed: %v", err)
	}
	atomic.AddInt64(&g.stats.loaderHits, 1)
	return ByteView{b: cloneBytes(bytes)}, nil
}

// getFromPeer
func (g *Group) getFromPeer(ctx context.Context, peer Peer, key string) (ByteView, error) {
	bytes, err := peer.Get(ctx, g.name, key)
	if err != nil {
		return ByteView{}, fmt.Errorf("peer get failed: %v", err)
	}
	return ByteView{b: cloneBytes(bytes)}, nil
}

// RegisterPeers PeerPicker
func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeers called more than once")
	}
	g.peers = peers
	logrus.Infof("cache group %s registered peers", g.name)
}

// Stats
// Stats reports group stats plus underlying cache stats.
func (g *Group) Stats() map[string]interface{} {
	stats := map[string]interface{}{
		"name":          g.name,
		"closed":        atomic.LoadInt32(&g.closed) == 1,
		"expiration":    g.expiration,
		"loads":         atomic.LoadInt64(&g.stats.loads),
		"local_hits":    atomic.LoadInt64(&g.stats.localHits),
		"local_misses":  atomic.LoadInt64(&g.stats.localMisses),
		"peer_hits":     atomic.LoadInt64(&g.stats.peerHits),
		"peer_misses":   atomic.LoadInt64(&g.stats.peerMisses),
		"loader_hits":   atomic.LoadInt64(&g.stats.loaderHits),
		"loader_errors": atomic.LoadInt64(&g.stats.loaderErrors),
	}

	totalGets := atomic.LoadInt64(&g.stats.localHits) + atomic.LoadInt64(&g.stats.localMisses)
	if totalGets > 0 {
		stats["local_hit_rate"] = float64(atomic.LoadInt64(&g.stats.localHits)) / float64(totalGets)
	}

	totalLoads := atomic.LoadInt64(&g.stats.loaderHits) + atomic.LoadInt64(&g.stats.loaderErrors)
	if totalLoads > 0 {
		stats["loader_hit_rate"] = float64(atomic.LoadInt64(&g.stats.loaderHits)) / float64(totalLoads)
	}


	if g.mainCache != nil {
		cacheStats := g.mainCache.Stats()
		for k, v := range cacheStats {
			stats["cache_"+k] = v
		}
	}
	return stats
}

//ListGroups
func ListGroups() []string {
	groupsMu.RLock()
	defer groupsMu.RUnlock()
	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}
	return names
}

//DestroyGroup
func DestroyGroup(name string) bool {
	groupsMu.Lock()
	defer groupsMu.Unlock()

	if g, ok := groups[name]; ok {
		g.Close()
		delete(groups, name)
		logrus.Infof("cache group %s destroyed", name)
		return true
	}
	return false
}

//DestroyAllGroups
func DestroyAllGroups() {
	groupsMu.Lock()
	defer groupsMu.Unlock()
	for name, g := range groups {
		g.Close()
		delete(groups, name)
		logrus.Infof("cache group %s destroyed", name)
	}
}
