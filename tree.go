package main

import (
	"log"
	"unsafe"
)

/* Tree is an implementation of a B+tree. */
type Tree struct {
}

const (
	TreeMaxOrder = 1 << 8
	//TreeMaxOrder     = 5
	TreeMaxKeyLength = (1 << 16) - 1
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
