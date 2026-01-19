package consistenthash

import "testing"

func TestAddGetRemove(t *testing.T) {
	m := New()

	if err := m.Add("node1", "node2"); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if len(m.keys) != m.config.DefaultReplicas*2 {
		t.Fatalf("unexpected virtual node count: %d", len(m.keys))
	}

	node := m.Get("key1")
	if node == "" {
		t.Fatal("Get returned empty node")
	}

	if err := m.Remove("node1"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	node = m.Get("key1")
	if node == "" {
		t.Fatal("Get returned empty node after remove")
	}
	if node != "node2" {
		t.Fatalf("expected node2 after remove, got %s", node)
	}
}

func TestRemoveMissingNode(t *testing.T) {
	m := New()
	if err := m.Remove("missing"); err == nil {
		t.Fatal("expected error removing missing node")
	}
}
