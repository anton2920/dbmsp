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
		panic("type is not supported")
	case []byte:
		return x
	case int:
		buffer := make([]byte, unsafe.Sizeof(x))
		binary.LittleEndian.PutUint64(buffer, uint64(x))
		return buffer
	}
}

func deserialize(_x interface{}) int {
	x := _x.([]byte)
	if len(x) == 0 {
		return 0
	} else {
		return int(binary.LittleEndian.Uint64(x))
	}
}

func (kv *KV) Init(pager Pager) error {
	kv.Tree = new(Tree)
	return kv.Tree.Init(pager)
}

func (kv *KV) Get(_key interface{}) interface{} {
	return kv.Tree.Get(serialize(_key))
}

func (kv *KV) Del(_key interface{}) error {
	return kv.Tree.Del(serialize(_key))
}

func (kv *KV) Has(_key interface{}) bool {
	return kv.Tree.Has(serialize(_key))
}

func (kv *KV) Set(_key interface{}, _value interface{}) error {
	key := serialize(_key)
	value := serialize(_value)

	return kv.Tree.Set(key, value)
}

func (kv *KV) String() string {
	return kv.Tree.String()
}
