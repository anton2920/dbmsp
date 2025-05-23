package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/anton2920/gofa/debug"
	"github.com/anton2920/gofa/util"
)

type Leaf struct {
	PageHeader

	/* Data is structured as follows: | N*sizeof(uint16) bytes of keyOffsets | keys... | ...empty space... | ...values | N*sizeof(uint16) bytes of valueOffsets | */
	Data [PageSize - PageHeaderSize]byte
}

const LeafExtraOffsetAfter = 16

func init() {
	var page Page
	page.Init(PageTypeLeaf)

	leaf := page.Leaf()
	debug.Printf("[leaf]: len(Leaf.Data) == %d\n", len(leaf.Data))

	leaf.InsertKeyValueAt([]byte{1, 2, 3, 4}, []byte{5, 6, 7, 8, 9, 10}, 0)
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

func (l *Leaf) GetExtraOffset(count int) int {
	var keyValueOffset uint16

	if count > 0 {
		return (((count + (int(l.N) % LeafExtraOffsetAfter) - 1) / LeafExtraOffsetAfter) + util.Bool2Int((int(l.N)%LeafExtraOffsetAfter) == 0)) * int(unsafe.Sizeof(keyValueOffset)) * LeafExtraOffsetAfter
	} else {
		return (((-count + (LeafExtraOffsetAfter - (int(l.N) % LeafExtraOffsetAfter))) / LeafExtraOffsetAfter) - util.Bool2Int((int(l.N)%LeafExtraOffsetAfter) == 0)) * int(unsafe.Sizeof(keyValueOffset)) * LeafExtraOffsetAfter
	}
}

func (l *Leaf) GetKeyAt(index int) []byte {
	offset, length := l.GetKeyOffsetAndLength(index)
	return l.Data[offset : offset+length]
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

func (l *Leaf) GetKeyOffsetInData(index int) int {
	var keyOffset uint16
	return int(unsafe.Sizeof(keyOffset)) * index
}

func (l *Leaf) GetKeyOffsets() []uint16 {
	return *(*[]uint16)(unsafe.Pointer(&reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&l.Data[0])), Len: int(l.N), Cap: int(l.N)}))
}

func (l *Leaf) GetValueAt(index int) []byte {
	offset, length := l.GetValueOffsetAndLength(index)
	return l.Data[offset-length : offset]
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

func (l *Leaf) GetValueOffsetInData(index int) int {
	var valueOffset uint16
	return len(l.Data) - int(unsafe.Sizeof(valueOffset))*(index+1)
}

func (l *Leaf) GetValueOffsets() []uint16 {
	var valueOffset uint16

	return *(*[]uint16)(unsafe.Pointer(&reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&l.Data[0])) + uintptr(len(l.Data)) - unsafe.Sizeof(valueOffset)*uintptr(l.N), Len: int(l.N), Cap: int(l.N)}))
}

func (l *Leaf) InsertKeyValueAt(key []byte, value []byte, index int) bool {
	if (len(key) > TreeMaxKeyLength) || (len(value) > TreeMaxValueLength) {
		return false
	}

	extraOffset := l.GetExtraOffset(1)
	keyOffset, _ := l.GetKeyOffsetAndLength(index)
	valueOffset, _ := l.GetValueOffsetAndLength(index)
	if int(l.Head)+int(l.Tail)+len(key)+len(value)+2*extraOffset > len(l.Data) {
		return false
	}

	keyOffsets := l.GetKeyOffsets()
	valueOffsets := l.GetValueOffsets()
	if extraOffset > 0 {
		for i := 0; i < index; i++ {
			keyOffsets[i] += uint16(extraOffset)
			valueOffsets[int(l.N)-1-i] -= uint16(extraOffset)
		}
	}
	for i := index; i < int(l.N); i++ {
		keyOffsets[i] += uint16(len(key) + extraOffset)
		valueOffsets[int(l.N)-1-i] -= uint16(len(value) + extraOffset)
	}

	copy(l.Data[keyOffset+len(key)+extraOffset:], l.Data[keyOffset:l.Head])
	copy(l.Data[l.GetKeyOffsetInData(int(l.N))+extraOffset:], l.Data[l.GetKeyOffsetInData(int(l.N)):keyOffset])
	copy(l.Data[keyOffset+extraOffset:], key)
	copy(l.Data[l.GetKeyOffsetInData(index+1):], l.Data[l.GetKeyOffsetInData(index):l.GetKeyOffsetInData(int(l.N))])
	binary.LittleEndian.PutUint16(l.Data[l.GetKeyOffsetInData(index):], uint16(keyOffset+extraOffset))

	copy(l.Data[len(l.Data)-int(l.Tail)-len(value)-extraOffset:], l.Data[len(l.Data)-int(l.Tail):valueOffset])
	copy(l.Data[valueOffset-extraOffset:], l.Data[valueOffset:l.GetValueOffsetInData(int(l.N)-1)])
	copy(l.Data[valueOffset-len(value)-extraOffset:], value)
	copy(l.Data[l.GetValueOffsetInData(int(l.N)):], l.Data[l.GetValueOffsetInData(int(l.N)-1):l.GetValueOffsetInData(index-1)])
	binary.LittleEndian.PutUint16(l.Data[l.GetValueOffsetInData(index):], uint16(valueOffset-extraOffset))

	l.Head += uint16(len(key) + extraOffset)
	l.Tail += uint16(len(value) + extraOffset)
	l.N++

	return true
}

func (src *Leaf) MoveData(dst *Leaf, where int, from int, to int) {
	var keyLengths, valueLengths int

	if where > int(dst.N) {
		panic("move destination index forces sparseness")
	}
	if to == -1 {
		to = int(src.N)
	}
	count := to - from

	/* Bulk insert to 'dst[where:]' from 'src[from:to]'. */
	extraOffset := dst.GetExtraOffset(count)
	whereKeyOffset, _ := dst.GetKeyOffsetAndLength(where)
	whereValueOffset, _ := dst.GetValueOffsetAndLength(where)

	fromKeyOffset, fromKeyLength := src.GetKeyOffsetAndLength(from)
	keyLengths += fromKeyLength

	fromValueOffset, fromValueLength := src.GetValueOffsetAndLength(from)
	valueLengths += fromValueLength

	for i := from + 1; i < to; i++ {
		_, keyLength := src.GetKeyOffsetAndLength(i)
		keyLengths += int(keyLength)

		_, valueLength := src.GetValueOffsetAndLength(i)
		valueLengths += int(valueLength)
	}

	if int(dst.Head)+int(dst.Tail)+keyLengths+valueLengths+extraOffset > len(dst.Data) {
		panic("no space left in destination")
	}

	keyOffsets := dst.GetKeyOffsets()
	valueOffsets := dst.GetValueOffsets()
	if extraOffset > 0 {
		for i := 0; i < where; i++ {
			keyOffsets[i] += uint16(extraOffset)
			valueOffsets[int(dst.N)-1-i] += uint16(extraOffset)
		}
	}
	for i := where; i < int(dst.N); i++ {
		keyOffsets[i] += uint16(keyLengths + extraOffset)
		valueOffsets[int(dst.N)-1-i] += uint16(valueLengths + extraOffset)
	}

	copy(dst.Data[whereKeyOffset+keyLengths+extraOffset:], dst.Data[whereKeyOffset:dst.Head])
	copy(dst.Data[dst.GetKeyOffsetInData(int(dst.N))+extraOffset:], dst.Data[dst.GetKeyOffsetInData(int(dst.N)):whereKeyOffset])
	copy(dst.Data[whereKeyOffset+extraOffset:], src.Data[fromKeyOffset:fromKeyOffset+keyLengths])
	copy(dst.Data[dst.GetKeyOffsetInData(where+count):], dst.Data[dst.GetKeyOffsetInData(where):dst.GetKeyOffsetInData(int(dst.N))])

	offset := uint16(whereKeyOffset + extraOffset)
	binary.LittleEndian.PutUint16(dst.Data[dst.GetKeyOffsetInData(where):], offset)
	for i := where + 1; i < where+count; i++ {
		_, keyLength := src.GetKeyOffsetAndLength(i)
		offset += uint16(keyLength)
		binary.LittleEndian.PutUint16(dst.Data[dst.GetKeyOffsetInData(i):], offset)
	}

	/* value3 | value2 | value1 | value0 | offt3 | offt2 | offt1 | offt0 | */
	copy(dst.Data[len(dst.Data)-int(dst.Tail)-valueLengths-extraOffset:], dst.Data[len(dst.Data)-int(dst.Tail):whereValueOffset])
	copy(dst.Data[whereValueOffset-extraOffset:], dst.Data[whereValueOffset:dst.GetValueOffsetInData(int(dst.N)-1)])
	copy(dst.Data[whereValueOffset-valueLengths-extraOffset:], src.Data[fromValueOffset-valueLengths:fromValueOffset])
	copy(dst.Data[dst.GetValueOffsetInData(int(dst.N)+count-1):], dst.Data[dst.GetValueOffsetInData(int(dst.N)-1):dst.GetValueOffsetInData(where-1)])

	offset = uint16(whereValueOffset - extraOffset)
	for i := where + 1; i < where+count; i++ {
		_, valueLength := src.GetValueOffsetAndLength(i)
		offset -= uint16(valueLength)
		binary.LittleEndian.PutUint16(dst.Data[dst.GetValueOffsetInData(i):], offset)
	}

	dst.Head += uint16(keyLengths + extraOffset)
	dst.Tail += uint16(valueLengths + extraOffset)
	dst.N += uint8(count)

	/* Bulk remove of 'src[from:to]'.*/
	extraOffset = src.GetExtraOffset(-count)

	keyOffsets = src.GetKeyOffsets()
	valueOffsets = src.GetValueOffsets()
	if extraOffset > 0 {
		for i := 0; i < from; i++ {
			keyOffsets[i] -= uint16(extraOffset)
			valueOffsets[i] -= uint16(extraOffset)
		}
	}
	for i := to; i < int(src.N); i++ {
		keyOffsets[i] -= uint16(keyLengths + extraOffset)
		valueOffsets[i] -= uint16(valueLengths + extraOffset)
	}

	copy(src.Data[src.GetKeyOffsetInData(from):], src.Data[src.GetKeyOffsetInData(to):src.GetKeyOffsetInData(int(src.N))])
	copy(src.Data[src.GetKeyOffsetInData(int(src.N))-extraOffset:], src.Data[src.GetKeyOffsetInData(int(src.N)):fromKeyOffset])
	copy(src.Data[fromKeyOffset-extraOffset:], src.Data[fromKeyOffset+keyLengths:src.Head])

	copy(src.Data[src.GetValueOffsetInData(from):], src.Data[src.GetValueOffsetInData(int(src.N)-1):src.GetValueOffsetInData(to-1)])
	copy(src.Data[fromValueOffset+extraOffset:], src.Data[fromValueOffset:src.GetValueOffsetInData(int(src.N)-1)])
	copy(src.Data[len(src.Data)-int(src.Tail)+valueLengths+extraOffset:], src.Data[len(src.Data)-int(src.Tail):fromValueOffset-valueLengths])

	src.Head -= uint16(keyLengths + extraOffset)
	src.Tail -= uint16(valueLengths + extraOffset)
	src.N -= uint8(count)
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

	keyOffsets := l.GetKeyOffsets()
	valueOffsets := l.GetValueOffsets()
	for i := index + 1; i < int(l.N); i++ {
		keyOffsets[i] += uint16(len(key) - keyLength)
		valueOffsets[int(l.N)-1-i] -= uint16(len(value) - valueLength)
	}

	l.Head += uint16(len(key) - keyLength)
	l.Tail += uint16(len(value) - valueLength)
	return true
}

func (l *Leaf) SetValueAt(value []byte, index int) bool {
	if (index < 0) || (index >= int(l.N)) {
		panic("leaf index out of range")
	}

	if len(value) > TreeMaxValueLength {
		return false
	}

	panic("not implemented")
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

func (l *Leaf) Reset() {
	l.N = 0
	l.Head = 0
	l.Tail = 0
}
