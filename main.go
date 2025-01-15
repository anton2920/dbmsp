package main

import (
	"log"
)

func Main()

func CallC(func())

var (
	/* 20; 40 10 30 15; 35 7 26 18 22; 5; 42 13 46 27 8 32; 38 24 45 25; */
	InsertKeys = [...]int{20, 40, 10, 30, 15, 35, 7, 26, 18, 22, 5, 42, 13, 46, 27, 8, 32, 38, 24, 45, 25}

	/* 25 45 24; 38 32; 8 27 46 13 42; 5 22 18 26; 7 35 15; */
	DeleleKeys = [...]int{25, 45, 24, 38, 32, 8, 27, 46, 13, 42, 5, 22, 18, 26, 7, 35, 15}
)

func Int2Slice(int) []byte

func main() {
	CallC(Main)

	t, err := OpenBplus("dbmsp.tree")
	if err != nil {
		log.Fatalf("Failed to open Bplus tree: %v", err)
	}

	println("INSERT 1!!!")
	for _, key := range InsertKeys {
		// fmt.Println("I:", key)
		t.Set(Int2Slice(key), Int2Slice(0))
		//fmt.Println(t)
	}
	// fmt.Println(t)
}
