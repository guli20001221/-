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

// ErrKeyRequired 键不能为空错误
var ErrKeyRequired = errors.New("key is required")

// ErrValueRequired 值不能为空错误
var ErrValueRequired = errors.New("value is required")

// ErrGroupClosed 组已关闭错误
var ErrGroupClosed = errors.New("group closed")

// Getter 加载键值的回调函数接口
type Getter interface {
	Get(ctx context.Context, key string) ([]byte, error)
}

// GetterFunc 函数类型实现Getter接口
type GetterFunc func(ctx context.Context, key string) ([]byte, error)

// Get 实现Getter接口
func (f GetterFunc) Get(ctx context.Context, key string) ([]byte, error) { return f(ctx, key) }

// Group 是一个缓存命名空间
type Group struct {
	name       string
	getter     Getter
	mainCache  *Cache
	peers      PeerPicker
	loader     *singleflight.Group
	expiration time.Duration // 缓存过期时间,0表示永不过期
	closed     int32         // 是否关闭
	stats      groupStats    // 统计信息
}

// groupStats 保存组的统计信息
type groupStats struct {
	loads        int64 // 加载次数
	localMisses  int64 //本地缓存未命中次数
	localHits    int64 // 本地缓存命中次数
	peerHits     int64 //从对等节点获取的缓存命中次数
	peerMisses   int64 //从对等节点获取的缓存未命中次数
	loaderHits   int64 //从加载器获取成功次数
	loaderErrors int64 //从加载器获取失败次数
	loadDuration int64 //加载总耗时
}

// GroupOption 缓存组配置项
type GroupOption func(*Group)

// WithExpiration 设置缓存过期时间
func WithExpiration(expiration time.Duration) GroupOption {
	return func(g *Group) {
		g.expiration = expiration
	}
}

// WithPeers 设置分布式节点
func WithPeers(peers PeerPicker) GroupOption {
	return func(g *Group) {
		g.peers = peers
	}
}

// WithCacheOptions 设置缓存选项
func WithCacheOptions(opts CacheOptions) GroupOption {
	return func(g *Group) {
		g.mainCache = NewCache(opts)
	}
}

// NewGroup 创建一个新的Group实例
func NewGroup(name string, cacheBytes int64, getter Getter, opts ...GroupOption) *Group {
	if getter == nil {
		panic("nil Getter")
	}
	//创建默认缓存选项
	cacheOpts := DefaultCacheOptions()
	cacheOpts.MaxBytes = cacheBytes
	g := &Group{
		name:      name,
		getter:    getter,
		mainCache: NewCache(cacheOpts),
		loader:    &singleflight.Group{},
	}

	//应用选项
	for _, opt := range opts {
		opt(g)
	}

	//注册到全局组映射
	groupsMu.Lock()
	defer groupsMu.Unlock()

	if _, dup := groups[name]; dup {
		panic("duplicate registration of group " + name)
	}

	groups[name] = g
	logrus.Infof("cache group %s created with cacheBytes= %d,expiration= %s", name, cacheBytes, g.expiration)
	return g
}

// GetGroup 获取指定名称的缓存组
func GetGroup(name string) *Group {
	groupsMu.RLock()
	defer groupsMu.RUnlock()
	return groups[name]
}

// Get 获取缓存值
func (g *Group) Get(ctx context.Context, key string) (ByteView, error) {
	//检查缓存组是否已关闭
	if atomic.LoadInt32(&g.closed) == 1 {
		return ByteView{}, ErrGroupClosed
	}

	if key == "" {
		return ByteView{}, ErrKeyRequired
	}

	//从本地缓存中获取
	if v, ok := g.mainCache.Get(ctx, key); ok {
		atomic.AddInt64(&g.stats.localHits, 1)
		return v, nil
	}
	atomic.AddInt64(&g.stats.localMisses, 1)
	return g.load(ctx, key)
}

// Set 设置缓存值
func (g *Group) Set(ctx context.Context, key string, value []byte) error {
	//检查缓存组是否已关闭
	if atomic.LoadInt32(&g.closed) == 1 {
		return ErrGroupClosed
	}

	if key == "" {
		return ErrKeyRequired
	}
	if len(value) == 0 {
		return ErrValueRequired
	}

	//检查是否是从其他节点同步过来的请求
	isPeerRequest := ctx.Value("from_peer") != nil

	//创建缓存视图
	view := ByteView{b: cloneBytes(value)}

	//设置到本地缓存
	if g.expiration > 0 {
		g.mainCache.AddWithExpiration(key, view, time.Now().Add(g.expiration))
	} else {
		g.mainCache.Add(key, view)
	}

	//如果不是从其他节点同步过来的请求，且启用了分布式模式，同步到其他节点
	if !isPeerRequest && g.peers != nil {
		go g.syncToPeers(ctx, "set", key, value)
	}
	return nil
}

// Delete 删除缓存值
func (g *Group) Delete(ctx context.Context, key string) error {
	if atomic.LoadInt32(&g.closed) == 1 {
		return ErrGroupClosed
	}
	if key == "" {
		return ErrKeyRequired
	}

	//从本地缓存中删除
	g.mainCache.Delete(key)

	//检查是否是从其他节点同步过来的请求
	isPeerRequest := ctx.Value("from_peer") != nil

	//如果不是从其他节点同步过来的请求，且启用了分布式模式，同步到其他节点
	if !isPeerRequest && g.peers != nil {
		go g.syncToPeers(ctx, "delete", key, nil)
	}
	return nil
}

// syncToPeers 同步缓存到其他节点
func (g *Group) syncToPeers(ctx context.Context, op string, key string, value []byte) {
	if g.peers == nil {
		return
	}

	//选择对等节点
	peer, ok, isSelf := g.peers.PickPeer(key)
	if !ok || isSelf {
		return
	}

	//创建同步请求上下文
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

// Clear 清空缓存
func (g *Group) Clear() {
	//检查组是否已关闭
	if atomic.LoadInt32(&g.closed) == 1 {
		return
	}
	g.mainCache.Clear()
	logrus.Infof("cache group %s cleared", g.name)
}

//Close 关闭组并释放资源

func (g *Group) Close() error {
	//如果组已关闭，则直接返回
	if !atomic.CompareAndSwapInt32(&g.closed, 0, 1) {
		return nil
	}

	//关闭本地缓存
	if g.mainCache != nil {
		g.mainCache.Close()
	}

	//从全局组映射中删除组
	groupsMu.Lock()
	defer groupsMu.Unlock()
	delete(groups, g.name)
	logrus.Infof("cache group %s closed", g.name)
	return nil
}

// load 从加载器获取缓存值
func (g *Group) load(ctx context.Context, key string) (value ByteView, err error) {
	//使用singleflight.Group进行并发控制
	startTime := time.Now()
	viewi, err := g.loader.Do(key, func() (interface{}, error) { return g.loadData(ctx, key) })

	//记录缓存加载时间
	loadDuration := time.Since(startTime).Nanoseconds()
	atomic.AddInt64(&g.stats.loadDuration, loadDuration)
	atomic.AddInt64(&g.stats.loads, 1)

	if err != nil {
		atomic.AddInt64(&g.stats.loaderErrors, 1)
		return ByteView{}, err
	}
	view := viewi.(ByteView)

	//设置到本地缓存
	if g.expiration > 0 {
		g.mainCache.AddWithExpiration(key, view, time.Now().Add(g.expiration))
	} else {
		g.mainCache.Add(key, view)
	}
	return view, nil
}

// loadData 实际加载数据的方法
func (g *Group) loadData(ctx context.Context, key string) (interface{}, error) {
	//尝试从远程节点获取数据
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

	//从数据源获取数据
	bytes, err := g.getter.Get(ctx, key)
	if err != nil {
		return ByteView{}, fmt.Errorf("getter failed: %v", err)
	}
	atomic.AddInt64(&g.stats.loaderHits, 1)
	return ByteView{b: cloneBytes(bytes)}, nil
}

// getFromPeer 从指定节点获取数据
func (g *Group) getFromPeer(ctx context.Context, peer Peer, key string) (ByteView, error) {
	bytes, err := peer.Get(ctx, g.name, key)
	if err != nil {
		return ByteView{}, fmt.Errorf("peer get failed: %v", err)
	}
	return ByteView{b: cloneBytes(bytes)}, nil
}

// RegisterPeers 注册PeerPicker
func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeers called more than once")
	}
	g.peers = peers
	logrus.Infof("cache group %s registered peers", g.name)
}

// Stats 获取缓存组统计信息
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
	//计算各种命中率
	totalGets := atomic.LoadInt64(&g.stats.localHits) + atomic.LoadInt64(&g.stats.localMisses)
	if totalGets > 0 {
		stats["local_hit_rate"] = float64(atomic.LoadInt64(&g.stats.localHits)) / float64(totalGets)
	}

	totalLoads := atomic.LoadInt64(&g.stats.loaderHits) + atomic.LoadInt64(&g.stats.loaderErrors)
	if totalLoads > 0 {
		stats["loader_hit_rate"] = float64(atomic.LoadInt64(&g.stats.loaderHits)) / float64(totalLoads)
	}

	//添加缓存大小
	if g.mainCache != nil {
		cacheStats := g.mainCache.Stats()
		for k, v := range cacheStats {
			stats["cache_"+k] = v
		}
	}
	return stats
}

//ListGroups 返回所有缓存组的名称
func ListGroups() []string {
	groupsMu.RLock()
	defer groupsMu.RUnlock()
	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}
	return names
}

//DestroyGroup 销毁指定名称的缓存组
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

//DestroyAllGroups 销毁所有缓存组
func DestroyAllGroups() {
	groupsMu.Lock()
	defer groupsMu.Unlock()
	for name, g := range groups {
		g.Close()
		delete(groups, name)
		logrus.Infof("cache group %s destroyed", name)
	}
}
