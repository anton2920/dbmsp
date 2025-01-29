package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/anton2920/gofa/debug"
)

type Node struct {
	PageHeader

	Data [PageSize - PageHeaderSize]byte
}

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

	n.Nbytes = uint16(int(unsafe.Sizeof(child0))*2 + int(unsafe.Sizeof(keyOffset)))
	n.N = 1

	binary.LittleEndian.PutUint16(n.Data[n.GetKeyOffsetInData(0):], uint16(int(unsafe.Sizeof(child0))*2+int(unsafe.Sizeof(keyOffset))))
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
	return int(unsafe.Sizeof(i)) * (index + 1)
}

func (n *Node) GetKeyLengthAndOffset(index int) (length int, offset int) {
	switch {
	case index < int(n.N)-1:
		offset = int(binary.LittleEndian.Uint16(n.Data[n.GetKeyOffsetInData(index):]))
		length = int(binary.LittleEndian.Uint16(n.Data[n.GetKeyOffsetInData(index+1):])) - offset
	case index == int(n.N)-1:
		offset = int(binary.LittleEndian.Uint16(n.Data[n.GetKeyOffsetInData(index):]))
		length = int(n.Nbytes) - offset
	case index > int(n.N)-1:
		offset = int(n.Nbytes)
		length = 0
	}
	return
}

func (n *Node) GetKeyOffsetInData(index int) int {
	var keyOffset uint16
	return n.GetChildOffsetInData(int(n.N)) + int(unsafe.Sizeof(keyOffset))*index
}

func (n *Node) GetChildAt(index int) int64 {
	return int64(binary.LittleEndian.Uint64(n.Data[n.GetChildOffsetInData(index):]))
}

func (n *Node) GetKeyAt(index int) []byte {
	length, offset := n.GetKeyLengthAndOffset(index)
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

	_, offset := n.GetKeyLengthAndOffset(index)
	if int(n.Nbytes)+len(key)+int(unsafe.Sizeof(keyOffset))+int(unsafe.Sizeof(child)) > len(n.Data) {
		return false
	}

	for i := 0; i < index; i++ {
		n.IncKeyOffset(i, uint16(int(unsafe.Sizeof(keyOffset))+int(unsafe.Sizeof(child))))
	}
	for i := index; i < int(n.N); i++ {
		n.IncKeyOffset(i, uint16(len(key)+int(unsafe.Sizeof(keyOffset))+int(unsafe.Sizeof(child))))
	}

	/* TODO(anton2920): find minimum number of bytes so that this key is still distinct from other keys. */
	copy(n.Data[offset+len(key)+int(unsafe.Sizeof(keyOffset))+int(unsafe.Sizeof(child)):], n.Data[offset:n.Nbytes])
	copy(n.Data[n.GetKeyOffsetInData(int(n.N))+int(unsafe.Sizeof(keyOffset))+int(unsafe.Sizeof(child)):], n.Data[n.GetKeyOffsetInData(int(n.N)):offset])
	copy(n.Data[offset+int(unsafe.Sizeof(keyOffset))+int(unsafe.Sizeof(child)):], key)

	copy(n.Data[n.GetKeyOffsetInData(index+1)+int(unsafe.Sizeof(child)):], n.Data[n.GetKeyOffsetInData(index):n.GetKeyOffsetInData(int(n.N))])
	copy(n.Data[n.GetKeyOffsetInData(0)+int(unsafe.Sizeof(child)):], n.Data[n.GetKeyOffsetInData(0):n.GetKeyOffsetInData(index)])
	binary.LittleEndian.PutUint16(n.Data[n.GetKeyOffsetInData(index)+int(unsafe.Sizeof(child)):], uint16(offset+int(unsafe.Sizeof(keyOffset))+int(unsafe.Sizeof(child))))

	copy(n.Data[n.GetChildOffsetInData(index+1):], n.Data[n.GetChildOffsetInData(index):n.GetChildOffsetInData(int(n.N))])
	n.SetChildAt(child, index)

	n.Nbytes += uint16(len(key) + int(unsafe.Sizeof(keyOffset)) + int(unsafe.Sizeof(child)))
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

	length, offset := n.GetKeyLengthAndOffset(index)
	if int(n.Nbytes)-length+len(key) > len(n.Data) {
		return false
	}

	/* TODO(anton2920): find minimum number of bytes so that this key is still distinct from other keys. */
	copy(n.Data[offset+len(key):], n.Data[offset+length:n.Nbytes])
	copy(n.Data[offset:], key)

	for i := index + 1; i < int(n.N); i++ {
		n.IncKeyOffset(i, uint16(len(key)-length))
	}

	n.Nbytes += uint16(len(key) - length)
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

	buf.WriteString("], Offsets: [")
	for i := 0; i < int(n.N); i++ {
		if i > 0 {
			buf.WriteString(", ")
		}
		_, offset := n.GetKeyLengthAndOffset(i)
		fmt.Fprintf(&buf, "%d", offset)
	}

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
