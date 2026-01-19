package Group_Cache

import (
	"Group_Cache/consistenthash"
	"context"
	"errors"
	"sync"
)

const defaultSvcName = "GroupCache"

//PeerPicker peer
type PeerPicker interface {
	PickPeer(key string) (peer Peer, ok bool, self bool)
	Close() error
}

//Peer
type Peer interface {
	Get(ctx context.Context, group string, key string) ([]byte, error)
	Set(ctx context.Context, group string, key string, value []byte) error
	Delete(ctx context.Context, group string, key string) (bool, error)
	Close() error
}

//ClientPicker PeerPicker
// ClientPicker maintains a consistent-hash ring and peer clients.
type ClientPicker struct {
	self  string
	mu    sync.RWMutex
	hash  *consistenthash.Map
	peers map[string]Peer
}

// LocalPeer is a loopback peer used for single-process testing.
type LocalPeer struct{}

func (p *LocalPeer) Get(ctx context.Context, group string, key string) ([]byte, error) {
	g := GetGroup(group)
	if g == nil {
		return nil, errors.New("group not found")
	}
	v, err := g.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	return v.ByteSlice(), nil
}

func (p *LocalPeer) Set(ctx context.Context, group string, key string, value []byte) error {
	g := GetGroup(group)
	if g == nil {
		return errors.New("group not found")
	}
	return g.Set(ctx, key, value)
}

func (p *LocalPeer) Delete(ctx context.Context, group string, key string) (bool, error) {
	g := GetGroup(group)
	if g == nil {
		return false, errors.New("group not found")
	}
	return true, g.Delete(ctx, key)
}

func (p *LocalPeer) Close() error { return nil }

func NewClientPicker(self string) *ClientPicker {
	return &ClientPicker{
		self:  self,
		hash: consistenthash.New(),
	}
}

// SetPeers replaces peers and rebuilds the hash ring.
func (p *ClientPicker) SetPeers(peers map[string]Peer) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.peers = make(map[string]Peer, len(peers))
	nodes := make([]string, 0, len(peers))
	for node, peer := range peers {
		if node == "" || peer == nil {
			continue
		}
		p.peers[node] = peer
		nodes = append(nodes, node)
	}
	p.hash = consistenthash.New()
	if len(nodes) > 0 {
		_ = p.hash.Add(nodes...)
	}
}

// UpdatePeers replaces peers using addresses and a peer factory.
func (p *ClientPicker) UpdatePeers(addrs []string, newPeer func(addr string) (Peer, error)) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if newPeer == nil {
		return
	}
	nextPeers := make(map[string]Peer, len(addrs))
	nodes := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		if addr == "" {
			continue
		}
		if existing, ok := p.peers[addr]; ok {
			nextPeers[addr] = existing
			nodes = append(nodes, addr)
			continue
		}
		peer, err := newPeer(addr)
		if err != nil || peer == nil {
			continue
		}
		nextPeers[addr] = peer
		nodes = append(nodes, addr)
	}

	for addr, peer := range p.peers {
		if _, ok := nextPeers[addr]; !ok && peer != nil {
			_ = peer.Close()
		}
	}

	p.peers = nextPeers
	p.hash = consistenthash.New()
	if len(nodes) > 0 {
		_ = p.hash.Add(nodes...)
	}
}

// PickPeer returns the peer responsible for key, plus whether it's self.
func (p *ClientPicker) PickPeer(key string) (peer Peer, ok bool, self bool) {
	if key == "" {
		return nil, false, false
	}
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.hash == nil || len(p.peers) == 0 {
		return nil, false, false
	}
	node := p.hash.Get(key)
	if node == "" {
		return nil, false, false
	}
	if node == p.self {
		return nil, true, true
	}
	peer, ok = p.peers[node]
	return peer, ok, false
}

func (p *ClientPicker) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, peer := range p.peers {
		_ = peer.Close()
	}
	p.peers = nil
	p.hash = nil
	return nil
}
