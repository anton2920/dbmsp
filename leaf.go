package main

import (
	"bytes"

	"github.com/anton2920/gofa/util"
)

type Leaf struct {
	PageHeader
	Data [PageSize - PageHeaderSize]byte
}

func (dst *Leaf) CopyKeys(src *Leaf, from int, to int) {

}

func (dst *Leaf) CopyValues(src *Leaf, from int, to int) {

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

func (l *Leaf) GetKeyAt(index int) []byte {
	return nil
}

func (l *Leaf) GetValueAt(index int) []byte {
	return nil
}

func (l *Leaf) InsertKeyAt(key []byte, index int) {

}

func (l *Leaf) InsertValueAt(value []byte, index int) {

}

func (l *Leaf) SetKeyAt(key []byte, index int) {

}

func (l *Leaf) SetValueAt(value []byte, index int) {

}
