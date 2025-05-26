package main

import (
	"os"
	"testing"

	"github.com/anton2920/gofa/trace"
)

func TestMain(m *testing.M) {
	trace.BeginProfile()
	code := m.Run()
	trace.EndAndPrintProfile()

	os.Exit(code)
}
