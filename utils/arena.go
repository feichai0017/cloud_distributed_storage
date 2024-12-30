 package utils

 import (
	 "github.com/pkg/errors"
	 "log"
	 "sync/atomic"
	 "unsafe"
 )
 
 const (
	 offsetSize = int(unsafe.Sizeof(uint32(0)))
 
	 // Always align nodes on 64-bit boundaries, even on 32-bit architectures,
	 // so that the node.value field is 64-bit aligned. This is necessary because
	 // node.getValueOffset uses atomic.LoadUint64, which expects its input
	 // pointer to be 64-bit aligned.
	 nodeAlign = int(unsafe.Sizeof(uint64(0))) - 1
 
	 MaxNodeSize = int(unsafe.Sizeof(node{}))
 )
 

type Arena struct {
	n			uint32 //offset
	shouldGrow 	bool
	buf 		[]byte
}

func newArena(sz int64) *Arena {
	buf := make([]byte, sz)
	return &Arena{
		buf: buf,
		n:   1,
	}
}

func (a *Arena) allocate(sz uint32) uint32 {
	offset := atomic.AddUint32(&a.n, sz)

	if !a.shouldGrow {
		AssertTrue(offset <= uint32(len(a.buf)))
		return offset - sz
	}

	if int(offset) > len(a.buf) - MaxNodeSize {
		growBy := uint32(len(a.buf))
		if growBy > 1<<30 {
			growBy = 1 << 30
		} 
		if growBy < sz {
			growBy = sz
		}
		newBuf := make([]byte, len(a.buf)+int(growBy))
		AssertTrue(len(a.buf) == copy(newBuf, a.buf))
		a.buf = newBuf
	}
	return offset - sz
}

func (a *Arena) size() int64 {
	return int64(atomic.LoadUint32(&a.n))
}

// putNode allocates a node in the arena. The node is aligned on a pointer-sized
 // boundary. The arena offset of the node is returned.
 func (s *Arena) putNode(height int) uint32 {
	// Compute the amount of the tower that will never be used, since the height
	// is less than maxHeight.
	unusedSize := (maxHeight - height) * offsetSize

	// Pad the allocation with enough bytes to ensure pointer alignment.
	l := uint32(MaxNodeSize - unusedSize + nodeAlign)
	n := s.allocate(l)

	// Return the aligned offset.
	m := (n + uint32(nodeAlign)) & ^uint32(nodeAlign)
	return m
}

func (a *Arena) putVal(val ValueStruct) uint32 {
	l := uint32(val.EncodedSize())
	offset := a.allocate(l)
	val.EncodeValue(a.buf[offset:])
	return offset
}

func (a *Arena) putKey(key []byte) uint32 {
	keySz := uint32(len(key))
	offset := a.allocate(keySz)
	buf := a.buf[offset : offset+keySz]
	AssertTrue(len(key) == copy(buf, key))
	return offset
}

func (a *Arena) getNode(offset uint32) *node {
	if offset == 0 {
		return nil
	}
	return (*node)(unsafe.Pointer(&a.buf[offset]))
}

func (a *Arena) getKey(offset uint32, sz uint16) []byte {
	if offset == 0 {
		return nil
	}
	return a.buf[offset : offset+uint32(sz)]
}

func (a *Arena) getVal(offset uint32, sz uint32) (ret ValueStruct) {
	if offset == 0 {
		return
	}
	ret.DecodeValue(a.buf[offset : offset+sz])
	return
}

func (a *Arena) getNodeOffset(n *node) uint32 {
	if n == nil {
		return 0
	}
	return uint32(uintptr(unsafe.Pointer(n)) - uintptr(unsafe.Pointer(&a.buf[0])))
}

// AssertTrue asserts that b is true. Otherwise, it would log fatal.
func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}
