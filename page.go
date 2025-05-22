package main

import (
	"log"
	"reflect"
	"unsafe"
)

type PageType uint8

type Page [PageSize]byte

type PageHeader struct {
	Type PageType
	N    uint8
	Head uint16
	Tail uint16
	_    [2]byte
}

const (
	PageSize       = 4096
	PageHeaderSize = unsafe.Sizeof(PageHeader{})
)

const (
	PageTypeNone = PageType(iota)
	PageTypeMeta
	PageTypeNode
	PageTypeLeaf
)

func (p *Page) Init(typ PageType) {
	var clr Page
	copy(p[:], clr[:])

	hdr := p.Header()
	hdr.Type = typ
}

func (p *Page) Header() *PageHeader {
	return (*PageHeader)(unsafe.Pointer(p))
}

func (p *Page) Type() PageType {
	return p.Header().Type
}

func (p *Page) Meta() *Meta {
	hdr := p.Header()
	if hdr.Type != PageTypeMeta {
		log.Panicf("Page has type %d, but tried to use it as '*Meta'", hdr.Type)
	}
	return (*Meta)(unsafe.Pointer(p))
}

func (p *Page) Node() *Node {
	hdr := p.Header()
	if hdr.Type != PageTypeNode {
		log.Panicf("Page has type %d, but tried to use it as '*Node'", hdr.Type)
	}
	return (*Node)(unsafe.Pointer(p))
}

func (p *Page) Leaf() *Leaf {
	hdr := p.Header()
	if hdr.Type != PageTypeLeaf {
		log.Panicf("Page has type %d, but tried to use it as '*Leaf'", hdr.Type)
	}
	return (*Leaf)(unsafe.Pointer(p))
}

func Page2Slice(p *Page) []Page {
	return *(*[]Page)(unsafe.Pointer(&reflect.SliceHeader{Data: uintptr(unsafe.Pointer(p)), Len: 1, Cap: 1}))
}

func Pages2Bytes(ps []Page) []byte {
	return *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&ps[0])), Len: len(ps) * PageSize, Cap: cap(ps) * PageSize}))
}
