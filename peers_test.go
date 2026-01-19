package Group_Cache

import (
	"context"
	"testing"
)

type fakePeer struct{}

func (p *fakePeer) Get(ctx context.Context, group string, key string) ([]byte, error) {
	return nil, nil
}

func (p *fakePeer) Set(ctx context.Context, group string, key string, value []byte) error {
	return nil
}

func (p *fakePeer) Delete(ctx context.Context, group string, key string) (bool, error) {
	return true, nil
}

func (p *fakePeer) Close() error { return nil }

func TestClientPickerPickPeer(t *testing.T) {
	picker := NewClientPicker("node1")
	picker.SetPeers(map[string]Peer{
		"node1": &fakePeer{},
		"node2": &fakePeer{},
	})

	peer, ok, self := picker.PickPeer("key1")
	if !ok {
		t.Fatal("expected picker to return a node")
	}
	if self && peer != nil {
		t.Fatal("expected nil peer for self")
	}
	if !self && peer == nil {
		t.Fatal("expected peer for non-self node")
	}
}

func TestClientPickerEmptyKey(t *testing.T) {
	picker := NewClientPicker("node1")
	if _, ok, _ := picker.PickPeer(""); ok {
		t.Fatal("expected ok=false for empty key")
	}
}
