package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

type PathItem struct {
	Page  Page
	Index int
}

type Bplus struct {
	Page
	sync.RWMutex

	Fd         *os.File
	SearchPath []PathItem
}

type BplusTx struct {
	Page

	Tree *Bplus
}

type Page [PageSize]byte

/* NOTE(anton2920): must be in sync with C definition. */
const (
	BplusNotFound = -2
	BplusOrder    = (1 << 8) - 1
	PageSize      = 4096
)

const (
	BplusPageTypeNone = uint8(iota)
	BplusPageTypeNode
	BplusPageTypeLeaf
)

func BplusGetEndSentinel(*Page) int64
func BplusGetRendSentinel(*Page) int64
func BplusGetRootOffset(*Page) int64
func BplusSetEndSentinel(*Page, int64)
func BplusSetRendSentinel(*Page, int64)
func BplusSetRootOffset(*Page, int64)

func BplusPageGetType(*Page) uint8
func BplusPageInit(*Page, uint8, int)

func BplusNodeGetChildAt(*Page, int) int64
func BplusNodeGetKeyAt(*Page, int) []byte
func BplusNodeGetNchildren(*Page) int
func BplusNodeFind(*Page, []byte) int
func BplusNodeSetChildAt(*Page, int64, int)

func BplusLeafGetKeyAt(*Page, int) []byte
func BplusLeafGetValueAt(*Page, int) []byte
func BplusLeafGetNvalues(*Page) int
func BplusLeafFind(*Page, []byte) (int, bool)
func BplusLeafSetKeyAt(*Page, []byte, int)
func BplusLeafSetNext(*Page, int64)
func BplusLeafSetPrev(*Page, int64)
func BplusLeafSetValueAt(*Page, []byte, int)

func OpenBplus(path string) (*Bplus, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open/create Bplus tree file: %v", err)
	}

	var t Bplus

	t.Fd = f
	if err := t.ReadPage(&t.Page, 0); err != nil {
		return nil, fmt.Errorf("failed to read meta page: %v", err)
	}

	return &t, nil
}

func (t *Bplus) BeginTx() BplusTx {
	var tx BplusTx

	tx.Tree = t

	t.RLock()
	copy(tx.Page[:], t.Page[:])
	t.RUnlock()

	return tx
}

func (t *Bplus) Get(key []byte) []byte {
	var page Page
	var v []byte

	offset := BplusGetRootOffset(&t.Page)
	for offset != 0 {
		if err := t.ReadPage(&page, offset); err != nil {
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

func (t *Bplus) Del(key []byte) {

}

func (t *Bplus) Has(key []byte) bool {
	return false
}

func (t *Bplus) ReadPage(page *Page, offset int64) error {
	_, err := t.Fd.ReadAt(page[:], offset)
	if (err != nil) && (err != io.EOF) {
		return fmt.Errorf("failed to read page: %v", err)
	}
	return nil
}

func (t *Bplus) Set(key []byte, value []byte) {
	var page Page

	offset := BplusGetRootOffset(&t.Page)
	if offset == 0 {
		endSentinel := BplusGetRendSentinel(&t.Page)
		rendSentinel := BplusGetEndSentinel(&t.Page)

		BplusPageInit(&page, BplusPageTypeLeaf, 1)
		BplusLeafSetKeyAt(&page, key, 0)
		BplusLeafSetValueAt(&page, value, 0)
		BplusLeafSetPrev(&page, rendSentinel)
		BplusLeafSetNext(&page, endSentinel)

		tx := t.BeginTx()
		defer tx.Rollback()

		{
			offset, err := tx.WritePage(&page)
			if err != nil {
				log.Panicf("Failed to write page: %v", err)
			}
			BplusSetRootOffset(&tx.Page, offset)
		}

		{
			if err := t.ReadPage(&page, endSentinel); err != nil {
				log.Panicf("Failed to read page: %v", err)
			}
			BplusLeafSetPrev(&page, offset)
			endSentinel, err := tx.WritePage(&page)
			if err != nil {
				log.Panicf("Failed to write page: %v", err)
			}
			BplusSetEndSentinel(&tx.Page, endSentinel)
		}

		{
			if err := t.ReadPage(&page, rendSentinel); err != nil {
				log.Panicf("Failed to read page: %v", err)
			}
			BplusLeafSetNext(&page, offset)
			rendSentinel, err := tx.WritePage(&page)
			if err != nil {
				log.Panicf("Failed to write page: %v", err)
			}
			BplusSetRendSentinel(&tx.Page, rendSentinel)
		}

		if err := tx.Commit(); err != nil {
			log.Panicf("Failed to commit Tx: %v", err)
		}
		return
	}

	var index int
	var ok bool

	/* TODO(anton2920): not MT-safe. */
	t.SearchPath = t.SearchPath[:0]
	for offset != 0 {
		if err := t.ReadPage(&page, offset); err != nil {
			log.Panicf("Failed to read page: %v", err)
		}

		switch BplusPageGetType(&page) {
		case BplusPageTypeNode:
			index = BplusNodeFind(&page, key)
			offset = BplusNodeGetChildAt(&page, index)
			t.SearchPath = append(t.SearchPath, PathItem{page, index})
		case BplusPageTypeLeaf:
			index, ok = BplusLeafFind(&page, key)
			offset = 0
		}
	}

	if ok {
		tx := t.BeginTx()
		defer tx.Rollback()

		BplusLeafSetValueAt(&page, value, index+1)
		offset, err := tx.WritePage(&page)
		if err != nil {
			log.Panicf("Failed to write page: %v", err)
		}

		/* Update indexing structure. */
		for p := len(t.SearchPath) - 1; p >= 0; p-- {
			index := t.SearchPath[p].Index
			page := t.SearchPath[p].Page

			BplusNodeSetChildAt(&page, offset, index)
			offset, err = tx.WritePage(&page)
			if err != nil {
				log.Panicf("Failed to write page: %v", err)
			}
		}
		BplusSetRootOffset(&tx.Page, offset)

		if err := tx.Commit(); err != nil {
			log.Panicf("Failed to commit Tx: %v", err)
		}
	} else {
	}
}

func (t *Bplus) stringImpl(buf *bytes.Buffer, offset int64, level int) {
	var page Page

	if offset != 0 {
		if err := t.ReadPage(&page, offset); err != nil {
			log.Panicf("Failed to read page: %v", err)
		}

		for i := 0; i < level; i++ {
			buf.WriteRune('\t')
		}
		switch BplusPageGetType(&page) {
		case BplusPageTypeNode:
			for i := 0; i < BplusNodeGetNchildren(&page)-1; i++ {
				fmt.Fprintf(buf, "%4v", BplusNodeGetKeyAt(&page, i))
			}
			buf.WriteRune('\n')

			for i := -1; i < BplusNodeGetNchildren(&page); i++ {
				t.stringImpl(buf, BplusNodeGetChildAt(&page, i), level+1)
			}
		case BplusPageTypeLeaf:
			for i := 0; i < BplusLeafGetNvalues(&page); i++ {
				fmt.Fprintf(buf, "%4v", BplusLeafGetKeyAt(&page, i))
			}
			buf.WriteRune('\n')
		}
	}
}

func (t *Bplus) String() string {
	var buf bytes.Buffer

	t.stringImpl(&buf, BplusGetRootOffset(&t.Page), 0)

	return buf.String()
}

func (tx *BplusTx) Commit() error {
	tx.Tree.Lock()
	defer tx.Tree.Unlock()

	n, err := tx.Tree.Fd.WriteAt(tx.Tree.Page[:], 0)
	if err != nil {
		return fmt.Errorf("failed to write Bplus page: %v", err)
	}
	if n != PageSize {
		log.Panicf("Somehow write returned %d", n)
	}

	copy(tx.Tree.Page[:], tx.Page[:])
	return nil
}

func (tx *BplusTx) Rollback() error {
	/* TODO(anton2920): move all new pages to FreeList, since they are going to be unused. */
	return nil
}

func (tx *BplusTx) WritePage(page *Page) (int64, error) {
	return 0, nil
}
