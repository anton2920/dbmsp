package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/anton2920/gofa/debug"
	"github.com/anton2920/gofa/util"
)

type Node struct {
	PageHeader

	/* Data is structured as follows: | N*sizeof(uint16) bytes of keyOffsets | keys... | ...empty space... | N*sizeof(int64) bytes of children | */
	Data [PageSize - PageHeaderSize]byte
}

/* TODO(anton2920): find the best constant for time-space tradeoff. */
const NodeExtraOffsetAfter = 8

func init() {
	var page Page
	page.Init(PageTypeNode)

	node := page.Node()
	debug.Printf("[node]: len(Node.Data) == %d\n", len(node.Data))

	node.Init([]byte{1, 2, 3, 4}, 0, 1)
	debug.Printf("[node]: %v\n", node)

	node.InsertKeyChildAt([]byte{5, 6, 7, 8, 9, 10}, 2, 1)
	debug.Printf("[node]: %v\n", node)

	node.InsertKeyChildAt([]byte{192, 168, 0, 1}, 3, 1)
	debug.Printf("[node]: %v\n", node)

	node.SetKeyAt([]byte{254}, 0)
	node.SetChildAt(254, 0)
	debug.Printf("[node]: %v\n", node)

	node.SetKeyAt([]byte{255, 255, 255, 255, 255, 255, 255, 255}, 1)
	node.SetChildAt(255, 1)
	debug.Printf("[node]: %v\n", node)
}

func (n *Node) Init(key []byte, child0 int64, child1 int64) {
	var keyOffset uint16

	if len(key) > TreeMaxKeyLength {
		panic("no space left for key")
	}

	n.SetChildAt(child0, -1)
	n.SetChildAt(child1, 0)

	n.Head = uint16(NodeExtraOffsetAfter * int(unsafe.Sizeof(keyOffset)))
	n.Tail = uint16(unsafe.Sizeof(child0)) * 2
	n.N = 1

	binary.LittleEndian.PutUint16(n.Data[n.GetKeyOffsetInData(0):], uint16(NodeExtraOffsetAfter*int(unsafe.Sizeof(keyOffset))))
	n.SetKeyAt(key, 0)
}

func (n *Node) Find(key []byte) int {
	if res := bytes.Compare(key, n.GetKeyAt(int(n.N)-1)); res >= 0 {
		return int(n.N) - 1
	}
	for i := 0; i < int(n.N); i++ {
		if bytes.Compare(key, n.GetKeyAt(i)) < 0 {
			return i - 1
		}
	}
	return int(n.N) - 1
}

func (n *Node) GetChildOffsetInData(index int) int {
	var i int64
	return len(n.Data) - (index+2)*int(unsafe.Sizeof(i))
}

func (n *Node) GetKeyOffsetInData(index int) int {
	var keyOffset uint16
	return int(unsafe.Sizeof(keyOffset)) * index
}

func (n *Node) GetKeyOffsetAndLength(index int) (offset int, length int) {
	switch {
	case index < int(n.N)-1:
		offset = int(binary.LittleEndian.Uint16(n.Data[n.GetKeyOffsetInData(index):]))
		length = int(binary.LittleEndian.Uint16(n.Data[n.GetKeyOffsetInData(index+1):])) - offset
	case index == int(n.N)-1:
		offset = int(binary.LittleEndian.Uint16(n.Data[n.GetKeyOffsetInData(index):]))
		length = int(n.Head) - offset
	case index > int(n.N)-1:
		offset = int(n.Head)
		length = 0
	}
	return
}

func (n *Node) GetChildAt(index int) int64 {
	return int64(binary.LittleEndian.Uint64(n.Data[n.GetChildOffsetInData(index):]))
}

func (n *Node) GetKeyAt(index int) []byte {
	offset, length := n.GetKeyOffsetAndLength(index)
	return n.Data[offset : offset+length]
}

func (n *Node) IncKeyOffset(index int, inc uint16) {
	buffer := n.Data[n.GetKeyOffsetInData(index):]
	binary.LittleEndian.PutUint16(buffer, binary.LittleEndian.Uint16(buffer)+inc)
}

func (n *Node) InsertKeyChildAt(key []byte, child int64, index int) bool {
	var keyOffset uint16

	if len(key) > TreeMaxKeyLength {
		return false
	}

	extraOffset := util.Bool2Int((n.N+1)%NodeExtraOffsetAfter == 1) * int(unsafe.Sizeof(keyOffset)) * NodeExtraOffsetAfter

	offset, _ := n.GetKeyOffsetAndLength(index)
	if int(n.Head)+int(n.Tail)+len(key)+int(unsafe.Sizeof(child))+extraOffset > len(n.Data) {
		return false
	}

	if extraOffset > 0 {
		for i := 0; i < index; i++ {
			n.IncKeyOffset(i, uint16(extraOffset))
		}
	}
	for i := index; i < int(n.N); i++ {
		n.IncKeyOffset(i, uint16(len(key)+int(extraOffset)))
	}

	copy(n.Data[offset+len(key)+int(extraOffset):], n.Data[offset:n.Head])
	copy(n.Data[n.GetKeyOffsetInData(int(n.N))+int(extraOffset):], n.Data[n.GetKeyOffsetInData(int(n.N)):offset])
	copy(n.Data[offset+int(extraOffset):], key)

	copy(n.Data[n.GetKeyOffsetInData(index+1):], n.Data[n.GetKeyOffsetInData(index):n.GetKeyOffsetInData(int(n.N))])
	binary.LittleEndian.PutUint16(n.Data[n.GetKeyOffsetInData(index):], uint16(offset+int(extraOffset)))

	copy(n.Data[n.GetChildOffsetInData(int(n.N)):], n.Data[n.GetChildOffsetInData(int(n.N)-1):n.GetChildOffsetInData(index-1)])
	n.SetChildAt(child, index)

	n.Head += uint16(len(key) + int(extraOffset))
	n.Tail += uint16(unsafe.Sizeof(child))
	n.N++

	return true
}

func (dst *Node) MoveData(src *Node, where int, from int, to int) {
}

func (n *Node) SetChildAt(offset int64, index int) {
	binary.LittleEndian.PutUint64(n.Data[n.GetChildOffsetInData(index):], uint64(offset))
}

func (n *Node) SetKeyAt(key []byte, index int) bool {
	if len(key) > TreeMaxKeyLength {
		return false
	}

	offset, length := n.GetKeyOffsetAndLength(index)
	if int(n.Head)+int(n.Tail)+len(key)-length > len(n.Data) {
		return false
	}

	for i := index + 1; i < int(n.N); i++ {
		n.IncKeyOffset(i, uint16(len(key)-length))
	}

	/* TODO(anton2920): find the minimum number of bytes so that this key is still distinct from other keys. */
	copy(n.Data[offset+len(key):], n.Data[offset+length:n.Head])
	copy(n.Data[offset:], key)

	n.Head += uint16(len(key) - length)
	return true
}

func (n *Node) String() string {
	var buf bytes.Buffer

	buf.WriteString("{ Children: [")
	for i := -1; i < int(n.N); i++ {
		if i > -1 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "%d", n.GetChildAt(i))
	}

	/*
		buf.WriteString("], Offsets: [")
		for i := 0; i < int(n.N); i++ {
			if i > 0 {
				buf.WriteString(", ")
			}
			offset, _ := n.GetKeyOffsetAndLength(i)
			fmt.Fprintf(&buf, "%d", offset)
		}
	*/

	buf.WriteString("], Keys: [")
	for i := 0; i < int(n.N); i++ {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "%v", n.GetKeyAt(i))
	}

	buf.WriteString("] }")
	return buf.String()
}
