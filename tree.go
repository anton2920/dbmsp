package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"sync"
	"unsafe"

	"github.com/anton2920/gofa/trace"
	"github.com/anton2920/gofa/util"
)

/* Tree is an implementation of a B+tree. */
type Tree struct {
	sync.RWMutex

	Pager Pager
	Meta  Page
}

type pathItem struct {
	Page
	Offset int64
	Index  int
}

type TreeTx struct {
	Tree *Tree

	NextOffset int64
	Root       int64

	Pages      []Page
	Offsets    []*int64
	SearchPath []pathItem
}

const (
	// TreeMaxOrder = 1 << 8
	TreeMaxOrder = 5

	TreeTxMaxPages    = PageSize - 1
	TreeNewPageOffset = TreeTxMaxPages
)

func init() {
	var p Page
	var m Meta
	var n Node
	var l Leaf

	const (
		psize = unsafe.Sizeof(p)
		msize = unsafe.Sizeof(m)
		nsize = unsafe.Sizeof(n)
		lsize = unsafe.Sizeof(l)
	)

	if (psize != msize) || (psize != nsize) || (psize != lsize) {
		log.Panicf("[tree]: sizeof(Page) == %d, sizeof(Meta) == %d, sizeof(Node) == %d, sizeof(Leaf) == %d", psize, msize, nsize, lsize)
	}
}

func duplicate(buffer []byte, x []byte) []byte {
	if len(buffer) < len(x) {
		panic("insufficient space in buffer")
	}
	return buffer[:copy(buffer, x)]
}

func leafFind(pager Pager, leaf *Leaf, key []byte) (int, bool) {
	defer trace.End(trace.Begin(""))

	if leaf.N == 0 {
		return -1, false
	} else if res := bytes.Compare(key, leaf.GetKeyAt(int(leaf.N)-1)); res >= 0 {
		return int(leaf.N) - 1 - util.Bool2Int(res == 0), res == 0
	}

	for i := 0; i < int(leaf.N); i++ {
		if res := bytes.Compare(key, leaf.GetKeyAt(i)); res <= 0 {
			return i - 1, res == 0
		}
	}

	return int(leaf.N) - 1, false
}

func nodeFind(pager Pager, node *Node, key []byte) int {
	defer trace.End(trace.Begin(""))

	if res := bytes.Compare(key, node.GetKeyAt(int(node.N)-1)); res >= 0 {
		return int(node.N) - 1
	}
	for i := 0; i < int(node.N); i++ {
		if bytes.Compare(key, node.GetKeyAt(i)) < 0 {
			return i - 1
		}
	}
	return int(node.N) - 1
}

func slice2Int(buf []byte) int {
	return int(binary.LittleEndian.Uint64(buf))
}

func (t *Tree) Init(pager Pager) error {
	defer trace.End(trace.Begin(""))

	t.Pager = pager
	t.Meta.Init(PageTypeMeta)

	if err := t.Pager.ReadPagesAt(Page2Slice(&t.Meta), 0); err != nil {
		return fmt.Errorf("failed to read meta page: %v", err)
	}

	if t.Meta.Meta().Root == 0 {
		const (
			Meta = iota
			Root
			Count
		)

		var pages [Count]Page

		pages[Meta].Init(PageTypeMeta)
		meta := pages[Meta].Meta()
		meta.Version = Version
		meta.NextOffset = Count * PageSize
		meta.Root = Root * PageSize

		pages[Root].Init(PageTypeLeaf)

		if err := t.Pager.WritePagesAt(pages[:], 0); err != nil {
			return fmt.Errorf("failed to write initial pages: %v", err)
		}
		t.Meta = pages[Meta]
	}

	return nil

}

func (t *Tree) BeginTx() *TreeTx {
	defer trace.End(trace.Begin(""))

	var tx TreeTx

	tx.Tree = t
	meta := t.Meta.Meta()

	tx.Tree.RLock()
	tx.Root = meta.Root
	tx.Tree.RUnlock()

	return &tx
}

func (t *Tree) Get(key []byte) []byte {
	tx := t.BeginTx()
	return tx.Get(key)
}

func (t *Tree) Del(key []byte) error {
	tx := t.BeginTx()
	defer tx.Rollback()

	if err := tx.Del(key); err != nil {
		return fmt.Errorf("failed to delete key from tree: %v", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit Tx in Del: %v", err)
	}

	return nil
}

func (t *Tree) Has(key []byte) bool {
	tx := t.BeginTx()
	return tx.Has(key)
}

func (t *Tree) Set(key []byte, value []byte) error {
	tx := t.BeginTx()
	defer tx.Rollback()

	if err := tx.Set(key, value); err != nil {
		return fmt.Errorf("failed to associate key with value: %v", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit ephemeral transaction: %v", err)
	}

	return nil
}

func (t *Tree) String() string {
	var tx TreeTx

	tx.Tree = t
	tx.Root = t.Meta.Meta().Root

	return tx.String()
}

func (tx *TreeTx) readPage(page *Page, offset int64) error {
	defer trace.End(trace.Begin(""))

	if offset < TreeTxMaxPages {
		*page = tx.Pages[offset]
		return nil
	}
	return tx.Tree.Pager.ReadPagesAt(Page2Slice(page), offset)
}

func (tx *TreeTx) writePage(page *Page, offset int64) (int64, error) {
	defer trace.End(trace.Begin(""))

	n := int64(len(tx.Pages))

	switch offset {
	case tx.Root:
		tx.Root = n
	}

	if offset < TreeTxMaxPages {
		tx.Pages[offset] = *page
		return offset, nil
	}

	/* TODO(anton2920): if RefCount == 1 -> move to FreeList. */
	tx.Pages = append(tx.Pages, *page)
	return n, nil
}

func transformOffset(offset int64, startOffset int64) int64 {
	offset *= PageSize
	offset += startOffset
	return offset
}

func (tx *TreeTx) Commit() error {
	defer trace.End(trace.Begin(""))

	tx.Tree.Lock()
	defer tx.Tree.Unlock()

	meta := tx.Tree.Meta.Meta()

	startOffset := meta.NextOffset

	/* TODO(anton2920): store pointers to places where offsets must be updated. */
	for i := 0; i < len(tx.Pages); i++ {
		page := &tx.Pages[i]

		switch page.Type() {
		case PageTypeNode:
			node := page.Node()
			for j := -1; j < int(node.N); j++ {
				offset := node.GetChildAt(j)
				if offset < TreeTxMaxPages {
					node.SetChildAt(transformOffset(offset, startOffset), j)
				}
			}
		}
	}
	if tx.Root < TreeTxMaxPages {
		tx.Root = transformOffset(tx.Root, startOffset)
	}

	if err := tx.Tree.Pager.WritePagesAt(tx.Pages, startOffset); err != nil {
		return fmt.Errorf("failed to commit pages: %v", err)
	}

	meta.NextOffset = startOffset + int64(len(tx.Pages))*PageSize
	meta.Root = tx.Root

	if err := tx.Tree.Pager.WritePagesAt(Page2Slice(&tx.Tree.Meta), 0); err != nil {
		return fmt.Errorf("failed to commit meta: %v", err)
	}
	return nil
}

func (tx *TreeTx) Del(key []byte) error {
	return nil
}

func (tx *TreeTx) Get(key []byte) []byte {
	defer trace.End(trace.Begin(""))

	var page Page
	var v []byte

	offset := tx.Root
	for offset != 0 {
		if err := tx.readPage(&page, offset); err != nil {
			log.Panicf("Failed to read page: %v", err)
		}

		switch page.Type() {
		case PageTypeNode:
			node := page.Node()
			index := nodeFind(tx.Tree.Pager, node, key)
			offset = node.GetChildAt(index)
		case PageTypeLeaf:
			leaf := page.Leaf()
			index, ok := leafFind(tx.Tree.Pager, leaf, key)
			if ok {
				v = leaf.GetValueAt(index + 1)
			}
			offset = 0
		}
	}

	return v
}

func (tx *TreeTx) Has(key []byte) bool {
	defer trace.End(trace.Begin(""))

	var page Page

	offset := tx.Root
	for offset != 0 {
		if err := tx.readPage(&page, offset); err != nil {
			log.Panicf("Failed to read page: %v", err)
		}

		switch page.Type() {
		case PageTypeNode:
			node := page.Node()
			index := nodeFind(tx.Tree.Pager, node, key)
			offset = node.GetChildAt(index)
		case PageTypeLeaf:
			leaf := page.Leaf()
			_, ok := leafFind(tx.Tree.Pager, leaf, key)
			return ok
		}
	}

	return false
}

func (tx *TreeTx) Rollback() error {
	/* TODO(anton2920): move all new pages to FreeList, since they are going to be unused. */
	return nil
}

func (tx *TreeTx) Set(key []byte, value []byte) error {
	defer trace.End(trace.Begin(""))

	var page Page

	var index int
	var err error
	var ok bool

	tx.SearchPath = tx.SearchPath[:0]

	offset := tx.Root
forOffset:
	for offset != 0 {
		if err := tx.readPage(&page, offset); err != nil {
			return fmt.Errorf("failed to read page: %v", err)
		}

		switch page.Type() {
		case PageTypeNode:
			node := page.Node()
			index = nodeFind(tx.Tree.Pager, node, key)
			tx.SearchPath = append(tx.SearchPath, pathItem{page, offset, index})
			offset = node.GetChildAt(index)
		case PageTypeLeaf:
			leaf := page.Leaf()
			index, ok = leafFind(tx.Tree.Pager, leaf, key)
			break forOffset
		}
	}

	var overflow bool
	leaf := page.Leaf()

	if ok {
		/* Found key, check for overflow before updating value. */
		overflow = leaf.OverflowAfterInsertValue(value)
	} else {
		/* Check for overflow before inserting new key. */
		overflow = leaf.OverflowAfterInsertKeyValue(key, value) || (leaf.N >= TreeMaxOrder-1)
	}

	if !overflow {
		if ok {
			/* Updating value for existing key. */
			leaf.SetValueAt(value, index+1)
		} else {
			/* Insering new key-value. */
			leaf.InsertKeyValueAt(key, value, index+1)
		}

		offset, err = tx.writePage(&page, offset)
		if err != nil {
			return fmt.Errorf("failed to write updated leaf: %v", err)
		}

		/* Update indexing structure. */
		for p := len(tx.SearchPath) - 1; p >= 0; p-- {
			index := tx.SearchPath[p].Index
			page := tx.SearchPath[p].Page
			node := page.Node()

			node.SetChildAt(offset, index)
			offset, err = tx.writePage(&page, tx.SearchPath[p].Offset)
			if err != nil {
				return fmt.Errorf("failed to write updated node: %v", err)
			}
		}

		tx.Root = offset
	} else {
		/* Split leaf into two. */
		var newLeaf Page
		newLeaf.Init(PageTypeLeaf)

		newBuffer := make([]byte, PageSize)

		half := int(leaf.N) / 2
		if index < half-1 {
			leaf.MoveData(newLeaf.Leaf(), 0, half-1, -1)
			if ok {
				leaf.SetValueAt(value, index)
			} else {
				leaf.InsertKeyValueAt(key, value, index+1)
			}
		} else {
			leaf.MoveData(newLeaf.Leaf(), 0, half, -1)
			if ok {
				newLeaf.Leaf().SetValueAt(value, index-half)
			} else {
				newLeaf.Leaf().InsertKeyValueAt(key, value, index+1-half)
			}
		}

		newKey := duplicate(newBuffer, newLeaf.Leaf().GetKeyAt(0))
		newPage, err := tx.writePage(&newLeaf, TreeNewPageOffset)
		if err != nil {
			return fmt.Errorf("failed to write new leaf: %v", err)
		}

		offset, err = tx.writePage(&page, offset)
		if err != nil {
			return fmt.Errorf("failed to write updated leaf: %v", err)
		}

		/* Update indexing structure. */
		for p := len(tx.SearchPath) - 1; p >= 0; p-- {
			index := tx.SearchPath[p].Index
			page := tx.SearchPath[p].Page
			node := page.Node()

			node.SetChildAt(offset, index)

			overflow = node.OverflowAfterInsertKeyChild(key) || (node.N >= TreeMaxOrder-1)
			if !overflow {
				node.InsertKeyChildAt(newKey, newPage, index+1)

				offset, err = tx.writePage(&page, tx.SearchPath[p].Offset)
				if err != nil {
					return fmt.Errorf("failed to write updated node: %v", err)
				}

				/* Update indexing structure. */
				for p := p - 1; p >= 0; p-- {
					index := tx.SearchPath[p].Index
					page := tx.SearchPath[p].Page
					node := page.Node()

					node.SetChildAt(offset, index)
					offset, err = tx.writePage(&page, tx.SearchPath[p].Offset)
					if err != nil {
						return fmt.Errorf("failed to write updated node: %v", err)
					}
				}

				tx.Root = offset
				return nil
			}

			var insertKey []byte
			var newNode Page
			newNode.Init(PageTypeNode)

			insertBuffer := make([]byte, PageSize)

			half = int(node.N) / 2
			if index < half-1 {
				insertKey = duplicate(insertBuffer, newKey)
				newKey = duplicate(newBuffer, node.GetKeyAt(half-1))

				node.MoveData(newNode.Node(), -1, half-1, -1)
				node.InsertKeyChildAt(insertKey, newPage, index+1)
			} else if index == half-1 {
				insertKey = duplicate(insertBuffer, node.GetKeyAt(half))
				insertPage := node.GetChildAt(half)

				node.MoveData(newNode.Node(), -1, half, -1)
				newNode.Node().SetChildAt(newPage, -1)
				newNode.Node().InsertKeyChildAt(insertKey, insertPage, index+1-half)
			} else {
				insertKey = duplicate(insertBuffer, newKey)
				newKey = duplicate(newBuffer, node.GetKeyAt(half))

				node.MoveData(newNode.Node(), -1, half, -1)
				newNode.Node().InsertKeyChildAt(insertKey, newPage, index-half)
			}

			newPage, err = tx.writePage(&newNode, TreeNewPageOffset)
			if err != nil {
				return fmt.Errorf("failed to write new node: %v", err)
			}

			offset, err = tx.writePage(&page, tx.SearchPath[p].Offset)
			if err != nil {
				return fmt.Errorf("failed to write updated node: %v", err)
			}
		}

		var root Page
		root.Init(PageTypeNode)
		node := root.Node()
		node.Init(newKey, tx.Root, newPage)

		tx.Root, err = tx.writePage(&root, TreeNewPageOffset)
		if err != nil {
			return fmt.Errorf("failed to write new root: %v", err)
		}
	}

	return nil
}

func (tx *TreeTx) stringImpl(buf *bytes.Buffer, offset int64, level int) error {
	var page Page

	if offset != 0 {
		if err := tx.readPage(&page, offset); err != nil {
			return fmt.Errorf("failed to read page: %v", err)
		}

		for i := 0; i < level; i++ {
			buf.WriteRune('\t')
		}

		switch page.Type() {
		case PageTypeNode:
			node := page.Node()
			for i := 0; i < int(node.N); i++ {
				fmt.Fprintf(buf, "%4d", slice2Int(node.GetKeyAt(i)))
			}
			buf.WriteRune('\n')

			for i := -1; i < int(node.N); i++ {
				tx.stringImpl(buf, node.GetChildAt(i), level+1)
			}
		case PageTypeLeaf:
			leaf := page.Leaf()
			for i := 0; i < int(leaf.N); i++ {
				fmt.Fprintf(buf, "%4d", slice2Int(leaf.GetKeyAt(i)))
				// fmt.Fprintf(buf, "(%d: %d) ", slice2Int(leaf.GetKeyAt(i)), slice2Int(leaf.GetValueAt(i)))
			}
			buf.WriteRune('\n')
		}
	}

	return nil
}

func (tx *TreeTx) String() string {
	var buf bytes.Buffer

	if err := tx.stringImpl(&buf, tx.Root, 0); err != nil {
		return err.Error()
	}

	return buf.String()
}
