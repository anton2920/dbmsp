package main

import "testing"

func BenchmarkLeafInsertKeyValueAt(b *testing.B) {
	var page Page
	page.Init(PageTypeLeaf)

	leaf := page.Leaf()
	key := Uint16ToBytes(0)
	value := Uint16ToBytes(0)

	b.Run("Prepend", func(b *testing.B) {
		leaf.Init(key, value)
		for i := 0; i < b.N; i++ {
			if leaf.InsertKeyChildAt(key, value, 0) == false {
				leaf.Init(key, value)
			}
		}
	})
	b.Run("Append", func(b *testing.B) {
		leaf.Init(key, 0, 0)
		for i := 0; i < b.N; i++ {
			if leaf.InsertKeyChildAt(key, value, int(leaf.N)) == false {
				leaf.Init(key, value)
			}
		}
	})
}
