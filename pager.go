package main

import (
	"fmt"
	"io"
	"os"

	"github.com/anton2920/gofa/trace"
)

/* TODO(anton2920): replace '*os.File' with 'int32'. */
func ReadPagesAt(f *os.File, pages []Page, offset int64) error {
	defer trace.End(trace.Begin(""))

	if _, err := f.ReadAt(Pages2Bytes(pages), offset); (err != nil) && (err != io.EOF) {
		return fmt.Errorf("failed to read %d pages at %d: %v", len(pages), offset, err)
	}
	return nil
}

func WritePagesAt(f *os.File, pages []Page, offset int64, sync bool) error {
	defer trace.End(trace.Begin(""))

	if _, err := f.WriteAt(Pages2Bytes(pages), offset); err != nil {
		return fmt.Errorf("failed to write %d pages at %d: %v", len(pages), offset, err)
	}
	if sync {
		defer trace.End(trace.Begin("main.WritePagesAt.Sync"))

		if err := f.Sync(); err != nil {
			return fmt.Errorf("failed to sync writes to disk: %v", err)
		}
	}
	return nil
}
