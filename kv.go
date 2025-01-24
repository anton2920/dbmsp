package main

type KV struct {
}

const (
	Version = 0x1
)

func (kv *KV) Get(key interface{}) interface{} {
	return nil
}

func (kv *KV) Del(key interface{}) {
}

func (kv *KV) Has(key interface{}) bool {
	return false
}

func (kv *KV) Set(key interface{}, value interface{}) {
}
