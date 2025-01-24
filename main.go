package main

import (
	"fmt"

	_ "github.com/anton2920/gofa/intel"
	"github.com/anton2920/gofa/trace"
)

var (
	/* 20; 40 10 30 15; 35 7 26 18 22; 5; 42 13 46 27 8 32; 38 24 45 25; */
	InsertKeys = [...]int{20, 40, 10, 30, 15, 35, 7, 26, 18, 22, 5, 42, 13, 46, 27, 8, 32, 38, 24, 45, 25}

	/* 25 45 24; 38 32; 8 27 46 13 42; 5 22 18 26; 7 35 15; */
	DeleleKeys = [...]int{25, 45, 24, 38, 32, 8, 27, 46, 13, 42, 5, 22, 18, 26, 7, 35, 15}
)

func main() {
	var kv KV

	println("INSERT 1!!!")
	trace.BeginProfile()
	for _, key := range InsertKeys {
		//fmt.Println("I:", key)
		kv.Set(key, 0)
		//fmt.Println(kv)
	}
	trace.EndAndPrintProfile()
	fmt.Println(kv)
}
