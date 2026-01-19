package Group_Cache

// ByteView holds an immutable view of cached bytes.
type ByteView struct {
	b []byte
}

func (b ByteView) Len() int {
	return len(b.b)
}

// ByteSlice returns a copy to prevent external mutation.
func (b ByteView) ByteSlice() []byte {
	return cloneBytes(b.b)
}

func (b ByteView) String() string {
	return string(b.b)
}

// cloneBytes is a small helper used to enforce immutability.
func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
