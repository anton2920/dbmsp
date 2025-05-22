package main

import (
	"encoding/binary"
	"unsafe"
)

type KV struct {
	Tree *Tree
}

const (
	Version = 0x1
)

func serialize(x interface{}) []byte {
	switch x := x.(type) {
	default:
		panic("only int is supported")
	case int:
		buffer := make([]byte, unsafe.Sizeof(x))
		binary.LittleEndian.PutUint64(buffer, uint64(x))
		return buffer
	}
}

func (kv *KV) Init(pager Pager) error {
	kv.Tree = new(Tree)
	return kv.Tree.Init(pager)
}

func (kv *KV) Get(key interface{}) interface{} {
	return nil
}

func (kv *KV) Del(key interface{}) {
}

func (kv *KV) Has(key interface{}) bool {
	return false
}

func (kv *KV) Set(_key interface{}, _value interface{}) error {
	key := serialize(_key)
	value := serialize(_value)

	return kv.Tree.Set(key, value)
}

func (kv *KV) String() string {
	return kv.Tree.String()
}
