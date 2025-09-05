package main

import (
	"fmt"
	"log"

	_ "github.com/anton2920/gofa/intel"
	"github.com/anton2920/gofa/trace"
)

var (
	/* 20; 40 10 30 15; 35 7 26 18 22; 5; 42 13 46 27 8 32; 38 24 45 25; */
	InsertKeys = [...]int{20, 40, 10, 30, 15, 35, 7, 26, 18, 22, 5, 42, 13, 46, 27, 8, 32, 38, 24, 45, 25}

	/* 25 45 24; 38 32; 8 27 46 13 42; 5 22 18 26; 7 35 15; */
	DeleleKeys = [...]int{25, 45, 24, 38, 32, 8, 27, 46, 13, 42, 5, 22, 18, 26, 7, 35, 15}

	ZeroValue = int2Slice(0)
)

const (
	Min  = 1
	Max  = 20
	Step = 1
)

func TreePrintSeq(t *Tree) {
	it, err := t.Begin()
	if err != nil {
		log.Fatalf("Failed to get iterator: %v", err)
	}
	for it.Next() {
		fmt.Printf("%d ", slice2Int(it.Key()))
	}
	fmt.Println()
	fmt.Println()
}

func main() {
	var pager MemoryPager

	t, err := GetTreeAt(&pager, -1)
	if err != nil {
		log.Fatalf("Failed to get first tree: %v", err)
	}

	trace.BeginProfile()

	println("INSERT 1!!!")
	for _, key := range InsertKeys {
		fmt.Println("I:", key)
		t.Set(int2Slice(key), ZeroValue)
		fmt.Println(t)
	}
	fmt.Println(t)
	TreePrintSeq(t)

	t, err = GetTreeAt(&pager, -1)
	if err != nil {
		log.Fatalf("Failed to get second tree: %v", err)
	}

	fmt.Println("INSERT 2!!!")
	for key := Min; key <= Max; key += Step {
		fmt.Println("I:", key)
		t.Set(int2Slice(key), ZeroValue)
		fmt.Println(t)
	}
	fmt.Println(t)
	TreePrintSeq(t)

	trace.EndAndPrintProfile()
}
