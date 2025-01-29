package main

import (
	"bytes"
	"fmt"

	"github.com/anton2920/gofa/debug"
	"github.com/anton2920/gofa/util"
)

type Leaf struct {
	PageHeader

	Data [PageSize - PageHeaderSize]byte
}

func init() {
	var page Page
	page.Init(PageTypeLeaf)

	leaf := page.Leaf()
	debug.Printf("[leaf]: len(Leaf.Data) == %d\n", len(leaf.Data))
}

func (dst *Leaf) CopyKeysAndValues(src *Leaf, from int, to int) {

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

func (l *Leaf) FindKeyLengthAndOffset(index int) (length int, offset int) {
	return
}

func (l *Leaf) GetKeyAt(index int) []byte {
	return nil
}

func (l *Leaf) GetValueAt(index int) []byte {
	return nil
}

func (l *Leaf) GetKeyValueAt(index int) ([]byte, []byte) {
	return nil, nil
}

func (l *Leaf) InsertKeyValueAt(key []byte, index int) {

}

func (l *Leaf) SetKeyValueAt(key []byte, index int) {

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
