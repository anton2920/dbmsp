package main

import (
	"testing"
	"unsafe"
)

func TestLeafGetExtraOffset(t *testing.T) {
	var leaf Leaf

	/* ((count + l.N%Extra - 1) / Extra) + (l.N%Extra==0) */
	/* ((count + (Extra - l.N%Extra)) / Extra) - (l.N%Extra==0)*/

	if LeafExtraOffsetAfter != 4 {
		t.Fatalf("Test expects LeafExtraOffsetAfter to be 4, but it is %d", LeafExtraOffsetAfter)
	}

	/* _ _ _ _   _ _ _ _   _ _ _ _ */
	/* 1 _ _ _   _ _ _ _   _ _ _ _ */
	/* 1 2 3 _   _ _ _ _   _ _ _ _ */
	/* 1 2 3 4   _ _ _ _   _ _ _ _ */
	/* 1 2 3 4   5 _ _ _   _ _ _ _ */
	/* 1 2 3 4   5 6 7 8   _ _ _ _ */
	/* 1 2 3 4   5 6 7 8   9 _ _ _ */
	tests := [...]struct {
		N     uint8
		Count int
		Extra int
	}{
		{0, 1, 1},
		{0, 4, 1},
		{0, 5, 2},
		{0, 8, 2},
		{0, 9, 3},
		{0, 12, 3},

		{1, -1, 1},
		{1, 3, 0},
		{1, 4, 1},
		{1, 7, 1},
		{1, 8, 2},

		{3, -3, 1},
		{3, -1, 0},
		{3, 1, 0},
		{3, 2, 1},
		{3, 5, 1},
		{3, 6, 2},

		{4, -4, 1},
		{4, -1, 0},
		{4, 1, 1},
		{4, 4, 1},
		{4, 5, 2},
		{4, 8, 2},

		{5, -5, 2},
		{5, -4, 1},
		{5, -1, 1},
		{5, 1, 0},
		{5, 3, 0},
		{5, 4, 1},
		{5, 7, 1},

		{8, -4, 1},
		{8, -3, 0},
		{8, -1, 0},
		{8, 1, 1},
		{8, 4, 1},
		{8, 5, 2},
		{8, 8, 2},

		{9, -5, 2},
		{9, -4, 1},
		{9, -1, 1},
		{9, 1, 0},
		{9, 3, 0},
		{9, 4, 1},
	}
	for _, test := range tests {
		leaf.N = test.N

		extra := leaf.GetExtraOffset(test.Count)
		extra /= LeafExtraOffsetAfter * int(unsafe.Sizeof(uint16(0)))

		if extra != test.Extra {
			t.Errorf("For N = %d, count = %d expected %d, but got %d", test.N, test.Count, test.Extra, extra)
		}
	}
}

func BenchmarkLeafInsertKeyValueAt(b *testing.B) {
	var page Page
	page.Init(PageTypeLeaf)

	leaf := page.Leaf()
	key := Uint16ToBytes(0)
	value := Uint16ToBytes(0)

	b.Run("Prepend", func(b *testing.B) {
		leaf.InsertKeyValueAt(key, value, 0)
		for i := 0; i < b.N; i++ {
			if leaf.InsertKeyValueAt(key, value, 0) == false {
				leaf.Reset()
			}
		}
	})
	b.Run("Append", func(b *testing.B) {
		leaf.InsertKeyValueAt(key, value, 0)
		for i := 0; i < b.N; i++ {
			if leaf.InsertKeyValueAt(key, value, int(leaf.N)) == false {
				leaf.Reset()
			}
		}
	})
}
