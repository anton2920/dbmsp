package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/anton2920/gofa/debug"
	"github.com/anton2920/gofa/util"
)

type Leaf struct {
	PageHeader

	/* Data is structured as follows: | N*sizeof(uint16) byte of keyOffsets-valueOffsets pairs | key-value pairs... | */
	Data [PageSize - PageHeaderSize]byte
}

func init() {
	var page Page
	page.Init(PageTypeLeaf)

	leaf := page.Leaf()
	debug.Printf("[leaf]: len(Leaf.Data) == %d\n", len(leaf.Data))

	leaf.Init([]byte{1, 2, 3, 4}, []byte{5, 6, 7, 8, 9, 10})
	debug.Printf("[leaf]: %v\n", leaf)

	leaf.InsertKeyValueAt([]byte{192, 168, 0, 1}, []byte{253, 253, 253, 0}, 1)
	debug.Printf("[leaf]: %v\n", leaf)

	leaf.InsertKeyValueAt([]byte{254}, []byte{254}, 1)
	debug.Printf("[leaf]: %v\n", leaf)

	leaf.SetKeyValueAt([]byte{1, 2, 3}, []byte{4, 5, 6}, 0)
	debug.Printf("[leaf]: %v\n", leaf)

	leaf.SetKeyValueAt([]byte{255, 255, 255, 255, 255}, []byte{255, 255, 255, 255, 255}, 1)
	debug.Printf("[leaf]: %v\n", leaf)
}

func (l *Leaf) Init(key []byte, value []byte) {
	var keyOffset, valueOffset uint16

	if (len(key) > TreeMaxKeyLength) || (len(value) > TreeMaxValueLength) {
		panic("no space left for key or value")
	}

	keyOffset = uint16(int(unsafe.Sizeof(keyOffset)) + int(unsafe.Sizeof(valueOffset)))
	valueOffset = uint16(int(unsafe.Sizeof(keyOffset)) + int(unsafe.Sizeof(valueOffset)) + len(key))

	binary.LittleEndian.PutUint16(l.Data[l.GetKeyOffsetInData(0):], keyOffset)
	binary.LittleEndian.PutUint16(l.Data[l.GetValueOffsetInData(0):], valueOffset)

	copy(l.Data[keyOffset:], key)
	copy(l.Data[valueOffset:], value)

	l.Nbytes += uint16(len(key) + len(value) + int(unsafe.Sizeof(keyOffset)) + int(unsafe.Sizeof(valueOffset)))
	l.N++
}

/* TODO(anton2920): optimize using SIMD or some form of batch comparison. */
func (l *Leaf) Find(key []byte) (int, bool) {
	if l.N == 0 {
		return -1, false
	} else if res := bytes.Compare(key, l.GetKeyAt(int(l.N)-1)); res >= 0 {
		return int(l.N) - 1 - util.Bool2Int(res == 0), res == 0
	}

	for i := 0; i < int(l.N); i++ {
		if res := bytes.Compare(key, l.GetKeyAt(i)); res <= 0 {
			return i - 1, res == 0
		}
	}

	return int(l.N) - 1, false
}

func (l *Leaf) GetKeyOffsetAndLength(index int) (offset int, length int) {
	switch {
	case index <= int(l.N)-1:
		offset = int(binary.LittleEndian.Uint16(l.Data[l.GetKeyOffsetInData(index):]))
		length = int(binary.LittleEndian.Uint16(l.Data[l.GetValueOffsetInData(index):])) - offset
	case index > int(l.N)-1:
		offset = int(l.Nbytes)
		length = 0
	}
	return
}

func (l *Leaf) GetValueOffsetAndLength(index int) (offset int, length int) {
	switch {
	case index < int(l.N)-1:
		offset = int(binary.LittleEndian.Uint16(l.Data[l.GetValueOffsetInData(index):]))
		length = int(binary.LittleEndian.Uint16(l.Data[l.GetKeyOffsetInData(index+1):])) - offset
	case index == int(l.N)-1:
		offset = int(binary.LittleEndian.Uint16(l.Data[l.GetValueOffsetInData(index):]))
		length = int(l.Nbytes) - offset
	}
	return
}

func (l *Leaf) GetKeyOffsetInData(index int) int {
	var pair struct {
		keyOffset   uint16
		valueOffset uint16
	}
	return int(unsafe.Sizeof(pair)) * index
}

func (l *Leaf) GetValueOffsetInData(index int) int {
	var keyOffset uint16
	return l.GetKeyOffsetInData(index) + int(unsafe.Sizeof(keyOffset))
}

func (l *Leaf) GetKeyAt(index int) []byte {
	offset, length := l.GetKeyOffsetAndLength(index)
	return l.Data[offset : offset+length]
}

func (l *Leaf) GetValueAt(index int) []byte {
	offset, length := l.GetValueOffsetAndLength(index)
	return l.Data[offset : offset+length]
}

func (l *Leaf) IncKeyOffset(index int, inc uint16) {
	buffer := l.Data[l.GetKeyOffsetInData(index):]
	binary.LittleEndian.PutUint16(buffer, binary.LittleEndian.Uint16(buffer)+inc)
}

func (l *Leaf) IncValueOffset(index int, inc uint16) {
	buffer := l.Data[l.GetValueOffsetInData(index):]
	binary.LittleEndian.PutUint16(buffer, binary.LittleEndian.Uint16(buffer)+inc)
}

func (l *Leaf) InsertKeyValueAt(key []byte, value []byte, index int) bool {
	var pair struct {
		keyOffset   uint16
		valueOffset uint16
	}

	if (len(key) > TreeMaxKeyLength) || (len(value) > TreeMaxValueLength) {
		return false
	}

	offset, _ := l.GetKeyOffsetAndLength(index)
	if int(l.Nbytes)+len(key)+len(value) > len(l.Data) {
		return false
	}

	for i := 0; i < index; i++ {
		l.IncKeyOffset(i, uint16(int(unsafe.Sizeof(pair))))
		l.IncValueOffset(i, uint16(int(unsafe.Sizeof(pair))))
	}
	for i := index; i < int(l.N); i++ {
		l.IncKeyOffset(i, uint16(len(key)+len(value)+int(unsafe.Sizeof(pair))))
		l.IncValueOffset(i, uint16(len(key)+len(value)+int(unsafe.Sizeof(pair))))
	}

	copy(l.Data[offset+len(key)+len(value)+int(unsafe.Sizeof(pair)):], l.Data[offset:l.Nbytes])
	copy(l.Data[l.GetKeyOffsetInData(int(l.N))+int(unsafe.Sizeof(pair)):], l.Data[l.GetKeyOffsetInData(int(l.N)):offset])
	copy(l.Data[offset+int(unsafe.Sizeof(pair)):], key)
	copy(l.Data[offset+len(key)+int(unsafe.Sizeof(pair)):], value)

	copy(l.Data[l.GetKeyOffsetInData(index+1):], l.Data[l.GetKeyOffsetInData(index):l.GetKeyOffsetInData(int(l.N))])
	binary.LittleEndian.PutUint16(l.Data[l.GetKeyOffsetInData(index):], uint16(offset+int(unsafe.Sizeof(pair))))
	binary.LittleEndian.PutUint16(l.Data[l.GetValueOffsetInData(index):], uint16(offset+len(key)+int(unsafe.Sizeof(pair))))

	l.Nbytes += uint16(len(key) + len(value) + int(unsafe.Sizeof(pair)))
	l.N++

	return true
}

func (dst *Leaf) MoveData(src *Node, where int, from int, to int) {

}

func (l *Leaf) SetKeyValueAt(key []byte, value []byte, index int) bool {
	if (len(key) > TreeMaxKeyLength) || (len(value) > TreeMaxValueLength) {
		return false
	}

	offset, length := l.GetKeyOffsetAndLength(index)
	_, valueLength := l.GetValueOffsetAndLength(index)
	if int(l.Nbytes)-length-valueLength+len(key)+len(value) > len(l.Data) {
		return false
	}

	copy(l.Data[offset+len(key)+len(value):], l.Data[offset+length+valueLength:l.Nbytes])
	copy(l.Data[offset:], key)
	copy(l.Data[offset+len(key):], value)

	l.IncValueOffset(index, uint16(len(key)-length))
	for i := index + 1; i < int(l.N); i++ {
		l.IncKeyOffset(i, uint16(len(key)+len(value)-length-valueLength))
		l.IncValueOffset(i, uint16(len(key)+len(value)-length-valueLength))
	}

	l.Nbytes += uint16(len(key) + len(value) - length - valueLength)
	return true
}

func (l *Leaf) String() string {
	var buf bytes.Buffer

	buf.WriteString("{ Keys: [")
	for i := 0; i < int(l.N); i++ {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "%v", l.GetKeyAt(i))
	}

	buf.WriteString("], Values: [")

	for i := 0; i < int(l.N); i++ {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "%v", l.GetValueAt(i))
	}

	buf.WriteString("] }")
	return buf.String()
}
