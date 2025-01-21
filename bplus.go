package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/anton2920/gofa/trace"
)

type PathItem struct {
	Page
	Offset int64
	Index  int
}

type Bplus struct {
	Meta Page
	sync.RWMutex

	Fd *os.File
}

type BplusTx struct {
	Tree *Bplus

	NextOffset   int64
	Root         int64
	EndSentinel  int64
	RendSentinel int64

	Pages      []Page
	Offsets    []*int64
	SearchPath []PathItem
}

type Page [PageSize]byte

/* NOTE(anton2920): must be in sync with C definition. */
const (
	BplusNotFound = -2
	// BplusOrder    = (1 << 8) - 1
	PageSize = 4096
)

const (
	BplusVersion = 0x1

	BplusOrder = 5

	BplusTxMaxPages    = PageSize - 1
	BplusNewPageOffset = BplusTxMaxPages
)

const (
	BplusPageTypeNone = uint8(iota)
	BplusPageTypeNode
	BplusPageTypeLeaf
)

func BplusPageGetType(*Page) uint8
func BplusPageInit(*Page, uint8, int)

func BplusMetaGetEndSentinel(*Page) int64
func BplusMetaGetNextOffset(*Page) int64
func BplusMetaGetRendSentinel(*Page) int64
func BplusMetaGetRoot(*Page) int64
func BplusMetaGetVersion(*Page) uint64
func BplusMetaSetEndSentinel(*Page, int64)
func BplusMetaSetNextOffset(*Page, int64)
func BplusMetaSetRendSentinel(*Page, int64)
func BplusMetaSetRoot(*Page, int64)
func BplusMetaSetVersion(*Page, uint64)

func BplusNodeCopyChildren(*Page, *Page, int, int)
func BplusNodeCopyKeys(*Page, *Page, int, int)
func BplusNodeFind(*Page, []byte) int
func BplusNodeGetChildAt(*Page, int) int64
func BplusNodeGetKeyAt(*Page, int) []byte
func BplusNodeGetNchildren(*Page) int
func BplusNodeInsertChildAt(*Page, int64, int)
func BplusNodeInsertKeyAt(*Page, []byte, int)
func BplusNodeSetChildAt(*Page, int64, int)
func BplusNodeSetKeyAt(*Page, []byte, int)
func BplusNodeSetNchildren(*Page, int)

func BplusLeafCopyKeys(*Page, *Page, int, int)
func BplusLeafCopyValues(*Page, *Page, int, int)
func BplusLeafFind(*Page, []byte) (int, bool)
func BplusLeafGetKeyAt(*Page, int) []byte
func BplusLeafGetNvalues(*Page) int
func BplusLeafGetPrev(*Page) int64
func BplusLeafGetValueAt(*Page, int) []byte
func BplusLeafInsertKeyAt(*Page, []byte, int)
func BplusLeafInsertValueAt(*Page, []byte, int)
func BplusLeafSetKeyAt(*Page, []byte, int)
func BplusLeafSetNvalues(*Page, int)
func BplusLeafSetPrev(*Page, int64)
func BplusLeafSetValueAt(*Page, []byte, int)

func Page2Slice(*Page) []Page
func Pages2Bytes([]Page) []byte

func ReadPagesAt(f *os.File, pages []Page, offset int64) error {
	if _, err := f.ReadAt(Pages2Bytes(pages), offset); (err != nil) && (err != io.EOF) {
		return fmt.Errorf("failed to read %d pages at %d: %v", len(pages), offset, err)
	}
	return nil
}

func WritePagesAt(f *os.File, pages []Page, offset int64, sync bool) error {
	if _, err := f.WriteAt(Pages2Bytes(pages), offset); err != nil {
		return fmt.Errorf("failed to write %d pages at %d: %v", len(pages), offset, err)
	}
	if sync {
		if err := f.Sync(); err != nil {
			return fmt.Errorf("failed to sync writes to disk: %v", err)
		}
	}
	return nil
}

func OpenBplus(path string) (*Bplus, error) {
	var t Bplus

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open/create Bplus tree file: %v", err)
	}

	t.Fd = f
	if err := ReadPagesAt(t.Fd, Page2Slice(&t.Meta), 0); err != nil {
		return nil, fmt.Errorf("failed to read meta page: %v", err)
	}

	if BplusMetaGetRoot(&t.Meta) == 0 {
		const (
			Meta = iota
			Root
			EndSentinel
			RendSentinel
			Next
		)

		var pages [4]Page

		BplusMetaSetVersion(&pages[Meta], BplusVersion)
		BplusMetaSetNextOffset(&pages[Meta], Next*PageSize)
		BplusMetaSetRoot(&pages[Meta], Root*PageSize)
		BplusMetaSetEndSentinel(&pages[Meta], EndSentinel*PageSize)
		BplusMetaSetRendSentinel(&pages[Meta], RendSentinel*PageSize)

		BplusPageInit(&pages[Root], BplusPageTypeLeaf, 0)
		//BplusLeafSetPrev(&pages[Root], RendSentinel*PageSize)
		//BplusLeafSetNext(&pages[Root], EndSentinel*PageSize)

		BplusPageInit(&pages[EndSentinel], BplusPageTypeLeaf, 0)
		//BplusLeafSetPrev(&pages[EndSentinel], Root*PageSize)

		BplusPageInit(&pages[RendSentinel], BplusPageTypeLeaf, 0)
		//BplusLeafSetNext(&pages[RendSentinel], Root*PageSize)

		if err := WritePagesAt(t.Fd, pages[:], 0, true); err != nil {
			return nil, fmt.Errorf("failed to write initial pages: %v", err)
		}
		t.Meta = pages[Meta]
	}

	return &t, nil
}

func (t *Bplus) Begin() int64 {
	return 0
}

func (t *Bplus) BeginTx() *BplusTx {
	var tx BplusTx

	tx.Tree = t

	tx.Tree.RLock()
	tx.Root = BplusMetaGetRoot(&t.Meta)
	tx.EndSentinel = BplusMetaGetEndSentinel(&t.Meta)
	tx.RendSentinel = BplusMetaGetRendSentinel(&t.Meta)
	tx.Tree.RUnlock()

	return &tx
}

func (t *Bplus) End() int64 {
	return BplusMetaGetEndSentinel(&t.Meta)
}

func (t *Bplus) Get(key []byte) []byte {
	var tx BplusTx

	tx.Tree = t
	tx.Root = BplusMetaGetRoot(&t.Meta)

	return tx.Get(key)
}

func (t *Bplus) Del(key []byte) {
	tx := t.BeginTx()
	defer tx.Rollback()

	tx.Del(key)

	if err := tx.Commit(); err != nil {
		log.Panicf("Failed to commit Tx in Set: %v", err)
	}
}

func (t *Bplus) Has(key []byte) bool {
	var tx BplusTx

	tx.Tree = t
	tx.Root = BplusMetaGetRoot(&t.Meta)

	return tx.Has(key)
}

func (t *Bplus) Set(key []byte, value []byte) {
	defer trace.End(trace.Begin(""))

	tx := t.BeginTx()
	defer tx.Rollback()

	tx.Set(key, value)

	if err := tx.Commit(); err != nil {
		log.Panicf("Failed to commit Tx in Set: %v", err)
	}
}

func (t *Bplus) String() string {
	var tx BplusTx

	tx.Tree = t
	tx.Root = BplusMetaGetRoot(&t.Meta)

	return tx.String()
}

func (tx *BplusTx) readPage(page *Page, offset int64) error {
	if offset < BplusTxMaxPages {
		*page = tx.Pages[offset]
		return nil
	}
	return ReadPagesAt(tx.Tree.Fd, Page2Slice(page), offset)
}

func (tx *BplusTx) writePage(page *Page, offset int64) int64 {
	n := int64(len(tx.Pages))

	switch offset {
	case tx.Root:
		tx.Root = n
	case tx.EndSentinel:
		tx.EndSentinel = n
	case tx.RendSentinel:
		tx.RendSentinel = n
	}

	if offset < BplusTxMaxPages {
		tx.Pages[offset] = *page
		return offset
	}

	/* TODO(anton2920): if RefCount == 1 -> move to FreeList. */
	tx.Pages = append(tx.Pages, *page)
	return n
}

func transformOffset(offset int64, startOffset int64) int64 {
	offset *= PageSize
	offset += startOffset
	return offset
}

func (tx *BplusTx) Commit() error {
	tx.Tree.Lock()
	defer tx.Tree.Unlock()

	startOffset := BplusMetaGetNextOffset(&tx.Tree.Meta)

	/* TODO(anton2920): store pointers to places where offsets must be updated. */
	for i := 0; i < len(tx.Pages); i++ {
		page := &tx.Pages[i]
		switch BplusPageGetType(page) {
		case BplusPageTypeNode:
			for j := -1; j < BplusNodeGetNchildren(page); j++ {
				offset := BplusNodeGetChildAt(page, j)
				if offset < BplusTxMaxPages {
					BplusNodeSetChildAt(page, transformOffset(offset, startOffset), j)
				}
			}
		case BplusPageTypeLeaf:
			/*
				prev := BplusLeafGetPrev(page)
				if prev < BplusTxMaxPages {
					BplusLeafSetPrev(page, transformOffset(prev, startOffset))
				}
				next := BplusLeafGetNext(page)
				if next < BplusTxMaxPages {
					BplusLeafSetNext(page, transformOffset(next, startOffset))
				}
			*/
		}
	}
	if tx.Root < BplusTxMaxPages {
		tx.Root = transformOffset(tx.Root, startOffset)
	}
	if tx.EndSentinel < BplusTxMaxPages {
		tx.EndSentinel = transformOffset(tx.EndSentinel, startOffset)
	}
	if tx.RendSentinel < BplusTxMaxPages {
		tx.RendSentinel = transformOffset(tx.RendSentinel, startOffset)
	}

	if err := WritePagesAt(tx.Tree.Fd, tx.Pages, startOffset, false); err != nil {
		return fmt.Errorf("failed to commit pages: %v", err)
	}

	meta := &tx.Tree.Meta
	BplusMetaSetNextOffset(meta, startOffset+int64(len(tx.Pages))*PageSize)
	BplusMetaSetRoot(meta, tx.Root)
	BplusMetaSetEndSentinel(meta, tx.EndSentinel)
	BplusMetaSetRendSentinel(meta, tx.RendSentinel)

	if err := WritePagesAt(tx.Tree.Fd, Page2Slice(meta), 0, true); err != nil {
		return fmt.Errorf("failed to commit meta: %v", err)
	}
	return nil
}

func (tx *BplusTx) Del(key []byte) {
}

func (tx *BplusTx) Get(key []byte) []byte {
	var page Page
	var v []byte

	offset := tx.Root
	for offset != 0 {
		if err := tx.readPage(&page, offset); err != nil {
			log.Panicf("Failed to read page: %v", err)
		}

		switch BplusPageGetType(&page) {
		case BplusPageTypeNode:
			index := BplusNodeFind(&page, key)
			offset = BplusNodeGetChildAt(&page, index)
		case BplusPageTypeLeaf:
			index, ok := BplusLeafFind(&page, key)
			if ok {
				v = BplusLeafGetValueAt(&page, index+1)
			}
			offset = 0
		}
	}

	return v
}

func (tx *BplusTx) Has(key []byte) bool {
	return false
}

func (tx *BplusTx) Rollback() error {
	/* TODO(anton2920): move all new pages to FreeList, since they are going to be unused. */
	return nil
}

func (tx *BplusTx) Set(key []byte, value []byte) {
	defer trace.End(trace.Begin(""))

	var page Page

	var index int
	var ok bool

	tx.SearchPath = tx.SearchPath[:0]

	offset := tx.Root
forOffset:
	for offset != 0 {
		if err := tx.readPage(&page, offset); err != nil {
			log.Panicf("Failed to read page: %v", err)
		}

		switch BplusPageGetType(&page) {
		case BplusPageTypeNode:
			index = BplusNodeFind(&page, key)
			tx.SearchPath = append(tx.SearchPath, PathItem{page, offset, index})
			offset = BplusNodeGetChildAt(&page, index)
		case BplusPageTypeLeaf:
			index, ok = BplusLeafFind(&page, key)
			break forOffset
		}
	}

	if ok {
		/* Found key, update value. */
		BplusLeafSetValueAt(&page, value, index+1)
		offset = tx.writePage(&page, offset)

		/* Update indexing structure. */
		for p := len(tx.SearchPath) - 1; p >= 0; p-- {
			index := tx.SearchPath[p].Index
			page := tx.SearchPath[p].Page

			BplusNodeSetChildAt(&page, offset, index)
			offset = tx.writePage(&page, tx.SearchPath[p].Offset)
		}

		tx.Root = offset
	} else {
		/* Insert new key. */
		half := BplusOrder / 2
		BplusLeafInsertKeyAt(&page, key, index+1)
		BplusLeafInsertValueAt(&page, value, index+1)
		if BplusLeafGetNvalues(&page) < BplusOrder {
			offset = tx.writePage(&page, offset)

			/* Update indexing structure. */
			for p := len(tx.SearchPath) - 1; p >= 0; p-- {
				index := tx.SearchPath[p].Index
				page := tx.SearchPath[p].Page

				BplusNodeSetChildAt(&page, offset, index)
				offset = tx.writePage(&page, tx.SearchPath[p].Offset)
			}

			tx.Root = offset
		} else {
			/* Split leaf into two. */
			var newLeaf Page
			BplusPageInit(&newLeaf, BplusPageTypeLeaf, half+(BplusOrder%2))

			BplusLeafCopyKeys(&newLeaf, &page, half, -1)
			BplusLeafCopyValues(&newLeaf, &page, half, -1)
			BplusLeafSetNvalues(&page, half)

			//BplusLeafSetPrev(&newLeaf, offset)
			//BplusLeafSetNext(&newLeaf, BplusLeafGetNext(&page))

			newKey := BplusLeafGetKeyAt(&page, half)
			newPage := tx.writePage(&newLeaf, BplusNewPageOffset)

			//var leafNext Page
			//if err := tx.readPage(&leafNext, BplusLeafGetNext(&page)); err != nil {
			//	log.Panicf("Failed to read leaf.Next: %v", err)
			//}
			//BplusLeafSetPrev(&leafNext, newPage)
			//tx.writePage(&leafNext, BplusLeafGetNext(&page))

			//BplusLeafSetNext(&page, newPage)
			offset = tx.writePage(&page, offset)

			/* Update indexing structure. */
			for p := len(tx.SearchPath) - 1; p >= 0; p-- {
				index := tx.SearchPath[p].Index
				page := tx.SearchPath[p].Page

				BplusNodeSetChildAt(&page, offset, index)
				BplusNodeInsertKeyAt(&page, newKey, index+1)
				BplusNodeInsertChildAt(&page, newPage, index+1)
				if BplusNodeGetNchildren(&page) < BplusOrder {
					offset = tx.writePage(&page, tx.SearchPath[p].Offset)

					/* Update indexing structure. */
					for p := p - 1; p >= 0; p-- {
						index := tx.SearchPath[p].Index
						page := tx.SearchPath[p].Page

						BplusNodeSetChildAt(&page, offset, index)
						offset = tx.writePage(&page, tx.SearchPath[p].Offset)
					}

					tx.Root = offset
					return
				}

				var newNode Page
				BplusPageInit(&newNode, BplusPageTypeNode, half-(1-BplusOrder%2))

				BplusNodeCopyKeys(&newNode, &page, half+1, -1)
				BplusNodeCopyChildren(&newNode, &page, half+1, -1)
				BplusNodeSetChildAt(&newNode, BplusNodeGetChildAt(&page, half), -1)
				BplusNodeSetNchildren(&page, half)

				newKey = BplusNodeGetKeyAt(&page, half)
				newPage = tx.writePage(&newNode, BplusNewPageOffset)

				offset = tx.writePage(&page, tx.SearchPath[p].Offset)
			}

			tmp := tx.Root
			BplusPageInit(&page, BplusPageTypeNode, 1)
			BplusNodeSetKeyAt(&page, newKey, 0)
			BplusNodeSetChildAt(&page, tmp, -1)
			BplusNodeSetChildAt(&page, newPage, 0)
			tx.Root = tx.writePage(&page, BplusNewPageOffset)
		}
	}
}

func (tx *BplusTx) stringImpl(buf *bytes.Buffer, offset int64, level int) {
	var page Page

	if offset != 0 {
		if err := tx.readPage(&page, offset); err != nil {
			log.Panicf("Failed to read page: %v", err)
		}

		for i := 0; i < level; i++ {
			buf.WriteRune('\t')
		}

		switch BplusPageGetType(&page) {
		case BplusPageTypeNode:
			for i := 0; i < BplusNodeGetNchildren(&page); i++ {
				fmt.Fprintf(buf, "%4d", Slice2Int(BplusNodeGetKeyAt(&page, i)))
			}
			buf.WriteRune('\n')

			for i := -1; i < BplusNodeGetNchildren(&page); i++ {
				tx.stringImpl(buf, BplusNodeGetChildAt(&page, i), level+1)
			}
		case BplusPageTypeLeaf:
			for i := 0; i < BplusLeafGetNvalues(&page); i++ {
				fmt.Fprintf(buf, "%4d", Slice2Int(BplusLeafGetKeyAt(&page, i)))
			}
			buf.WriteRune('\n')
		}
	}
}

func (tx *BplusTx) String() string {
	var buf bytes.Buffer

	tx.stringImpl(&buf, tx.Root, 0)

	return buf.String()
}
