package main

import (
	"fmt"
	"os"

	_ "github.com/anton2920/gofa/intel"
	"github.com/anton2920/gofa/trace"
)

var (
	/* 20; 40 10 30 15; 35 7 26 18 22; 5; 42 13 46 27 8 32; 38 24 45 25; */
	InsertKeys = [...]int{20, 40, 10, 30, 15, 35, 7, 26, 18, 22, 5, 42, 13, 46, 27, 8, 32, 38, 24, 45, 25}

	/* 25 45 24; 38 32; 8 27 46 13 42; 5 22 18 26; 7 35 15; */
	DeleleKeys = [...]int{25, 45, 24, 38, 32, 8, 27, 46, 13, 42, 5, 22, 18, 26, 7, 35, 15}
)

const (
	Min  = 1
	Max  = 13
	Step = 1
)

func main() {
	var kv KV

	if err := kv.Init(new(MemoryPager)); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize KV: %v\n", err)
		os.Exit(1)
	}

	trace.BeginProfile()

	println("INSERT 1!!!")
	for _, key := range InsertKeys {
		//fmt.Println("I:", key)
		kv.Set(key, 0)
		//fmt.Println(kv.Tree)
	}
	fmt.Println(kv.Tree)

	if err := kv.Init(new(MemoryPager)); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize KV: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("INSERT 2!!!")
	for key := Min; key <= Max; key += Step {
		fmt.Println("I:", key)
		kv.Set(key, key)
		fmt.Println(kv.Tree)
	}
	fmt.Println(kv.Tree)
	for key := Min; key <= Max; key += Step {
		if got := kv.Get(key); key != deserialize(got) {
			fmt.Printf("ERROR! Expected %d, got %d\n", key, deserialize(got))
		}
	}

	trace.EndAndPrintProfile()
}
