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

	/* Data is structured as follows: | N*sizeof(uint16) bytes of keyOffsets | keys... | ...empty space... | ...values | N*sizeof(uint16) bytes of valueOffsets | */
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

	binary.LittleEndian.PutUint16(l.Data[l.GetKeyOffsetInData(0):], uint16(unsafe.Sizeof(keyOffset)))
	binary.LittleEndian.PutUint16(l.Data[l.GetValueOffsetInData(0):], uint16(len(l.Data)-int(unsafe.Sizeof(valueOffset))))

	l.Head = uint16(len(key) + int(unsafe.Sizeof(keyOffset)))
	l.Tail = uint16(len(value) + int(unsafe.Sizeof(valueOffset)))
	l.N = 1

	l.SetKeyValueAt(key, value, 0)
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
	case index < int(l.N)-1:
		offset = int(binary.LittleEndian.Uint16(l.Data[l.GetKeyOffsetInData(index):]))
		length = int(binary.LittleEndian.Uint16(l.Data[l.GetKeyOffsetInData(index+1):])) - offset
	case index == int(l.N)-1:
		offset = int(binary.LittleEndian.Uint16(l.Data[l.GetKeyOffsetInData(index):]))
		length = int(l.Head) - offset
	case index > int(l.N)-1:
		offset = int(l.Head)
		length = 0
	}
	return
}

/* value3 | value2 | value1 | value0 | offt3 | offt2 | offt1 | offt0 | */
/* value0 | offt0 | */
func (l *Leaf) GetValueOffsetAndLength(index int) (offset int, length int) {
	switch {
	case index < int(l.N)-1:
		offset = int(binary.LittleEndian.Uint16(l.Data[l.GetValueOffsetInData(index):]))
		length = offset - int(binary.LittleEndian.Uint16(l.Data[l.GetValueOffsetInData(index+1):]))
	case index == int(l.N)-1:
		offset = int(binary.LittleEndian.Uint16(l.Data[l.GetValueOffsetInData(index):]))
		length = offset - (len(l.Data) - int(l.Tail))
	case index > int(l.N)-1:
		offset = len(l.Data) - int(l.Tail)
		length = 0
	}
	return
}

func (l *Leaf) GetKeyOffsetInData(index int) int {
	var keyOffset uint16
	return int(unsafe.Sizeof(keyOffset)) * index
}

func (l *Leaf) GetValueOffsetInData(index int) int {
	var valueOffset uint16
	return len(l.Data) - int(unsafe.Sizeof(valueOffset))*(index+1)
}

func (l *Leaf) GetKeyAt(index int) []byte {
	offset, length := l.GetKeyOffsetAndLength(index)
	return l.Data[offset : offset+length]
}

func (l *Leaf) GetValueAt(index int) []byte {
	offset, length := l.GetValueOffsetAndLength(index)
	return l.Data[offset-length : offset]
}

func (l *Leaf) IncKeyOffset(index int, inc uint16) {
	buffer := l.Data[l.GetKeyOffsetInData(index):]
	binary.LittleEndian.PutUint16(buffer, binary.LittleEndian.Uint16(buffer)+inc)
}

func (l *Leaf) DecValueOffset(index int, dec uint16) {
	buffer := l.Data[l.GetValueOffsetInData(index):]
	binary.LittleEndian.PutUint16(buffer, binary.LittleEndian.Uint16(buffer)-dec)
}

func (l *Leaf) InsertKeyValueAt(key []byte, value []byte, index int) bool {
	var extraOffset uint16

	if (len(key) > TreeMaxKeyLength) || (len(value) > TreeMaxValueLength) {
		return false
	}

	keyOffset, _ := l.GetKeyOffsetAndLength(index)
	valueOffset, _ := l.GetValueOffsetAndLength(index)
	if int(l.Head)+int(l.Tail)+len(key)+len(value) > len(l.Data) {
		return false
	}

	for i := 0; i < index; i++ {
		l.IncKeyOffset(i, uint16(unsafe.Sizeof(extraOffset)))
		l.DecValueOffset(i, uint16(unsafe.Sizeof(extraOffset)))
	}
	for i := index; i < int(l.N); i++ {
		l.IncKeyOffset(i, uint16(len(key)+int(unsafe.Sizeof(extraOffset))))
		l.DecValueOffset(i, uint16(len(value)+int(unsafe.Sizeof(extraOffset))))
	}

	copy(l.Data[keyOffset+len(key)+int(unsafe.Sizeof(extraOffset)):], l.Data[keyOffset:l.Head])
	copy(l.Data[l.GetKeyOffsetInData(int(l.N))+int(unsafe.Sizeof(extraOffset)):], l.Data[l.GetKeyOffsetInData(int(l.N)):keyOffset])
	copy(l.Data[keyOffset+int(unsafe.Sizeof(extraOffset)):], key)

	/* value3 | value2 | value1 | value0 | offt3 | offt2 | offt1 | offt0 | */
	copy(l.Data[len(l.Data)-int(l.Tail)-len(value)-int(unsafe.Sizeof(extraOffset)):], l.Data[len(l.Data)-int(l.Tail):valueOffset])
	/* TODO(anton2920): maybe we don't need -1 */
	copy(l.Data[valueOffset-int(unsafe.Sizeof(extraOffset)):], l.Data[valueOffset:l.GetValueOffsetInData(int(l.N)-1)])
	copy(l.Data[valueOffset-len(value)-int(unsafe.Sizeof(extraOffset)):], value)

	copy(l.Data[l.GetKeyOffsetInData(index+1):], l.Data[l.GetKeyOffsetInData(index):l.GetKeyOffsetInData(int(l.N))])
	binary.LittleEndian.PutUint16(l.Data[l.GetKeyOffsetInData(index):], uint16(keyOffset+int(unsafe.Sizeof(extraOffset))))

	copy(l.Data[l.GetValueOffsetInData(int(l.N)):], l.Data[l.GetValueOffsetInData(int(l.N)-1):l.GetValueOffsetInData(index-1)])
	binary.LittleEndian.PutUint16(l.Data[l.GetValueOffsetInData(index):], uint16(valueOffset-int(unsafe.Sizeof(extraOffset))))

	l.Head += uint16(len(key) + int(unsafe.Sizeof(extraOffset)))
	l.Tail += uint16(len(value) + int(unsafe.Sizeof(extraOffset)))
	l.N++

	return true
}

func (dst *Leaf) MoveData(src *Node, where int, from int, to int) {

}

func (l *Leaf) SetKeyValueAt(key []byte, value []byte, index int) bool {
	if (index < 0) || (index >= int(l.N)) {
		panic("leaf index out of range")
	}

	if (len(key) > TreeMaxKeyLength) || (len(value) > TreeMaxValueLength) {
		return false
	}

	keyOffset, keyLength := l.GetKeyOffsetAndLength(index)
	valueOffset, valueLength := l.GetValueOffsetAndLength(index)
	if int(l.Head)+int(l.Tail)+len(key)+len(value)-keyLength-valueLength > len(l.Data) {
		return false
	}

	copy(l.Data[keyOffset+len(key):], l.Data[keyOffset+keyLength:l.Head])
	copy(l.Data[keyOffset:], key)

	/* value3 | value2 | value1 | value0 | offt3 | offt2 | offt1 | offt0 | */
	copy(l.Data[len(l.Data)-int(l.Tail)-len(value)+valueLength:], l.Data[len(l.Data)-int(l.Tail):valueOffset-valueLength])
	copy(l.Data[valueOffset-len(value):], value)

	for i := index + 1; i < int(l.N); i++ {
		l.IncKeyOffset(i, uint16(len(key)-keyLength))
		l.DecValueOffset(i, uint16(len(value)-valueLength))
	}

	l.Head += uint16(len(key) - keyLength)
	l.Tail += uint16(len(value) - valueLength)
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
