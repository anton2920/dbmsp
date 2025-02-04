package main

import (
	"testing"
	"unsafe"
)

func Uint16ToBytes(x uint16) []byte {
	buf := make([]byte, unsafe.Sizeof(x))
	buf[0] = byte(x & 0xFF)
	buf[1] = byte((x >> 8) & 0xFF)
	return buf
}

func BenchmarkNodeInsertAt(b *testing.B) {
	var page Page
	page.Init(PageTypeNode)

	node := page.Node()
	key := Uint16ToBytes(0)
	b.Run("Prepend", func(b *testing.B) {
		node.Init(key, 0, 0)
		for i := 0; i < b.N; i++ {
			if node.InsertKeyChildAt(key, 0, 0) == false {
				node.Init(key, 0, 0)
			}
		}
	})
	b.Run("Append", func(b *testing.B) {
		node.Init(key, 0, 0)
		for i := 0; i < b.N; i++ {
			if node.InsertKeyChildAt(key, 0, int(node.N)) == false {
				node.Init(key, 0, 0)
			}
		}
	})
}
