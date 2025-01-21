package main

import (
	"fmt"
	"log"
	"unsafe"

	_ "github.com/anton2920/gofa/intel"
	"github.com/anton2920/gofa/trace"
)

func Main()

func CallC(func())

var (
	/* 20; 40 10 30 15; 35 7 26 18 22; 5; 42 13 46 27 8 32; 38 24 45 25; */
	InsertKeys = [...]int{20, 40, 10, 30, 15, 35, 7, 26, 18, 22, 5, 42, 13, 46, 27, 8, 32, 38, 24, 45, 25}

	/* 25 45 24; 38 32; 8 27 46 13 42; 5 22 18 26; 7 35 15; */
	DeleleKeys = [...]int{25, 45, 24, 38, 32, 8, 27, 46, 13, 42, 5, 22, 18, 26, 7, 35, 15}
)

func Slice2Int([]byte) int

func Int2Slice(x int) []byte {
	xs := make([]byte, unsafe.Sizeof(x))
	*(*int)(unsafe.Pointer(&xs[0])) = x
	return xs
}

func BplusPrintSeq(t *Bplus) {
}

func main() {
	CallC(Main)

	t, err := OpenBplus("dbmsp.tree")
	if err != nil {
		log.Fatalf("Failed to open Bplus tree: %v", err)
	}

	println("INSERT 1!!!")
	trace.BeginProfile()
	for _, key := range InsertKeys {
		//fmt.Println("I:", key)
		t.Set(Int2Slice(key), Int2Slice(0))
		//fmt.Println(t)
	}
	trace.EndAndPrintProfile()
	fmt.Println(t)
	//BplusPrintSeq(t)

	/* NOTE(anton2920): sanity check for 'missing stackmap' error. */
	//runtime.GC()
}
