package main

import (
	"fmt"
)

type Pager interface {
	ReadPagesAt(pages []Page, offset int64) error
	WritePagesAt(pages []Page, offset int64) error
}

type MemoryPager struct {
	Pages []Page
}

var _ Pager = new(MemoryPager)

func (p *MemoryPager) ReadPagesAt(pages []Page, offset int64) error {
	if offset%PageSize != 0 {
		return fmt.Errorf("offset must be a multiple of PageSize")
	}
	index := int(offset / PageSize)

	if (index == 0) && (len(p.Pages) == 0) {
		return nil
	}
	if (index < 0) || (index >= len(p.Pages)) {
		return fmt.Errorf("pages index out of bounds")
	}

	copy(pages, p.Pages[index:])
	return nil
}

func (p *MemoryPager) WritePagesAt(pages []Page, offset int64) error {
	if offset%PageSize != 0 {
		return fmt.Errorf("offset must be a multiple of PageSize")
	}
	index := int(offset / PageSize)

	if (index < 0) || (index >= len(p.Pages)+1) {
		return fmt.Errorf("pages index out of bounds")
	}

	if index == len(p.Pages) {
		p.Pages = append(p.Pages, pages...)
	} else {
		copy(p.Pages[index:], pages)
	}
	return nil
}
