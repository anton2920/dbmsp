package main

import "unsafe"

type Meta struct {
	PageHeader

	Version int64

	Root         int64
	EndSentinel  int64
	RendSentinel int64

	_ [PageSize - PageHeaderSize - 4*unsafe.Sizeof(int64(0))]byte
}

func (m *Meta) Page() *Page {
	return (*Page)(unsafe.Pointer(m))
}
