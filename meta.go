package main

import "unsafe"

type Meta struct {
	PageHeader

	Version int64

	/* TODO(anton2920): probably switch to more sophisticated allocator. */
	NextOffset int64

	Root int64

	_ [PageSize - PageHeaderSize - 3*unsafe.Sizeof(int64(0))]byte
}

func (m *Meta) Page() *Page {
	return (*Page)(unsafe.Pointer(m))
}
