package main

import (
	"bytes"
	"fmt"
)

type Node struct {
	PageHeader

	Children [TreeMaxOrder - 1]int64
	Child0   int64

	KeyOffsets [TreeMaxOrder - 1]uint16
	Keys       [PageSize - PageHeaderSize - (TreeMaxOrder-1)*8 - 8 - (TreeMaxOrder-1)*2]byte
}

func init() {
	var page Page
	page.Init(PageTypeNode, 0)

	node := page.Node()
	fmt.Printf("[node]: len(Node.Keys) == %d\n", len(node.Keys))

	node.InsertKeyAt([]byte{1, 2, 3, 4}, 0)
	node.InsertChildAt(1, -1)
	fmt.Printf("[node]: %v\n", node)

	node.InsertKeyAt([]byte{5, 6, 7, 8, 9, 10}, 1)
	node.InsertChildAt(2, 0)
	fmt.Printf("[node]: %v\n", node)

	node.InsertKeyAt([]byte{192, 168, 0, 1}, 2)
	node.InsertChildAt(3, 1)
	fmt.Printf("[node]: %v\n", node)

	node.SetKeyAt([]byte{254}, 0)
	node.SetChildAt(254, 0)
	fmt.Printf("[node]: %v\n", node)

	node.SetKeyAt([]byte{255, 255, 255, 255, 255, 255, 255, 255}, 1)
	node.SetChildAt(255, 1)
	fmt.Printf("[node]: %v\n", node)
}

func (dst *Node) CopyChildren(src *Node, where int, from int, to int) {
	if (where == -1) && (from == -1) {
		dst.Child0 = src.Child0
		where++
		from++
	} else if where == -1 {
		dst.Child0 = src.Children[from]
		where++
		from++
	} else if from == -1 {
		dst.Children[where] = src.Child0
		where++
		from++
	}
	copy(dst.Children[where:], src.Children[from:to])
}

func (dst *Node) CopyKeys(src *Node, where int, from int, to int) {
	_, whereOffset := dst.FindKeyLengthAndOffset(where)
	_, fromOffset := src.FindKeyLengthAndOffset(from)
	_, toOffset := src.FindKeyLengthAndOffset(to)

	copy(dst.Keys[whereOffset:], src.Keys[fromOffset:toOffset])
	copy(dst.KeyOffsets[where:], src.KeyOffsets[from:to])
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

func (n *Node) FindKeyLengthAndOffset(index int) (length int, offset int) {
	switch {
	case index < int(n.N)-1:
		offset = int(n.KeyOffsets[index])
		length = int(n.KeyOffsets[index+1]) - offset
	case index == int(n.N)-1:
		offset = int(n.KeyOffsets[index])
		length = int(n.Nbytes) - offset
	case index > int(n.N)-1:
		offset = int(n.Nbytes)
		length = 0
	}
	return
}

func (n *Node) GetKeyAt(index int) []byte {
	length, offset := n.FindKeyLengthAndOffset(index)
	return n.Keys[offset : offset+length]
}

func (n *Node) GetChildAt(index int) int64 {
	if index == -1 {
		return n.Child0
	} else {
		return n.Children[index]
	}
}

func (n *Node) InsertChildAt(offset int64, index int) {
	if index == -1 {
		copy(n.Children[1:], n.Children[0:])
		n.Children[0] = n.Child0
		n.Child0 = offset
	} else {
		copy(n.Children[index+1:], n.Children[index:])
		n.Children[index] = offset
	}
	n.N++
}

func (n *Node) InsertKeyAt(key []byte, index int) bool {
	if len(key) > TreeMaxKeyLength {
		return false
	}

	_, offset := n.FindKeyLengthAndOffset(index)
	if int(n.Nbytes)+len(key) > len(n.Keys) {
		return false
	}

	/* TODO(anton2920): find minimum number of bytes so that this key is still distinct from other keys. */
	copy(n.Keys[offset+len(key):], n.Keys[offset:n.Nbytes])
	copy(n.Keys[offset:], key)

	for i := index; i < int(n.N); i++ {
		n.KeyOffsets[i] += uint16(len(key))
	}
	copy(n.KeyOffsets[index+1:], n.KeyOffsets[index:int(n.N)])
	n.KeyOffsets[index] = uint16(offset)

	n.Nbytes += uint16(len(key))
	return true
}

func (n *Node) SetChildAt(offset int64, index int) {
	if index == -1 {
		n.Child0 = offset
	} else {
		n.Children[index] = offset
	}
}

func (n *Node) SetKeyAt(key []byte, index int) bool {
	if len(key) > TreeMaxKeyLength {
		return false
	}

	length, offset := n.FindKeyLengthAndOffset(index)
	if int(n.Nbytes)-length+len(key) > len(n.Keys) {
		return false
	}

	/* TODO(anton2920): find minimum number of bytes so that this key is still distinct from other keys. */
	copy(n.Keys[offset+len(key):], n.Keys[offset+length:n.Nbytes])
	copy(n.Keys[offset:], key)

	for i := index + 1; i < int(n.N); i++ {
		n.KeyOffsets[i] += uint16(len(key)) - uint16(length)
	}

	n.Nbytes = n.Nbytes + uint16(len(key)) - uint16(length)
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
		fmt.Fprintf(&buf, "%d", n.KeyOffsets[i])
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
