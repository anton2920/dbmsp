package main

import (
	"log"
	"reflect"
	"unsafe"
)

type Page [PageSize]byte

type PageHeader struct {
	Type   uint8
	N      uint8
	Nbytes uint16
	_      [4]byte
}

const (
	PageSize       = 4096
	PageHeaderSize = unsafe.Sizeof(PageHeader{})
)

const (
	PageTypeNone = uint8(iota)
	PageTypeMeta
	PageTypeNode
	PageTypeLeaf
)

func (p *Page) Init(typ byte) {
	var clr Page
	copy(p[:], clr[:])

	hdr := p.Header()
	hdr.Type = typ
}

func (p *Page) Header() *PageHeader {
	return (*PageHeader)(unsafe.Pointer(p))
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
	return *(*[]Page)(unsafe.Pointer(&reflect.SliceHeader{uintptr(unsafe.Pointer(p)), 1, 1}))
}

func Pages2Bytes(ps []Page) []byte {
	return *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{uintptr(unsafe.Pointer(&ps[0])), len(ps) * PageSize, cap(ps) * PageSize}))
}
