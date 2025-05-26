package main

import (
	"testing"
)

const N = 10000

func testKVGet(t *testing.T, g Generator, pager Pager) {
	t.Helper()

	var kv KV
	if err := kv.Init(pager); err != nil {
		t.Fatalf("Failed to initialize KV: %v", err)
	}

	m := make(map[int]int)
	for i := 0; i < N; i++ {
		k := g.Generate()
		v := g.Generate()

		m[k] = v
		kv.Set(k, v)
	}

	for k, v := range m {
		if got := kv.Get(k); deserialize(got) != v {
			t.Errorf("expected value %v, got %v", v, deserialize(got))
		}
	}
}

func testKVDel(t *testing.T, g Generator, pager Pager) {
	t.Helper()

	var kv KV
	if err := kv.Init(pager); err != nil {
		t.Fatalf("Failed to initialize KV: %v", err)
	}

	m := make(map[int]struct{})
	for i := 0; i < N; i++ {
		k := g.Generate()

		m[k] = struct{}{}
		kv.Set(k, 0)
	}

	for k := range m {
		kv.Del(k)
		if kv.Has(k) {
			t.Errorf("expected key %v to be removed, but it's still present", k)
		}
	}
}

func testKVHas(t *testing.T, g Generator, pager Pager) {
	t.Helper()

	var kv KV
	if err := kv.Init(pager); err != nil {
		t.Fatalf("Failed to initialize KV: %v", err)
	}

	m := make(map[int]struct{})
	for i := 0; i < N; i++ {
		k := g.Generate()

		m[k] = struct{}{}
		kv.Set(k, 0)
	}

	for k := range m {
		if !kv.Has(k) {
			t.Errorf("expected to find key %v, found nothing", k)
		}
	}
}

func testKVSet(t *testing.T, g Generator, pager Pager) {
	t.Helper()

	var kv KV
	if err := kv.Init(pager); err != nil {
		t.Fatalf("Failed to initialize KV: %v", err)
	}

	for i := 0; i < N; i++ {
		k := g.Generate()
		v := g.Generate()

		kv.Set(k, v)
		if !kv.Has(k) {
			t.Errorf("expected to find key %v, found nothing", k)
		}
		if got := kv.Get(k); deserialize(got) != v {
			t.Errorf("expected value %v, got %v", v, deserialize(got))
		}
	}
}

func TestKV(t *testing.T) {
	ops := [...]struct {
		Name string
		Func func(*testing.T, Generator, Pager)
	}{
		{"Get", testKVGet},
		// 	{"Del", testKVDel},
		{"Has", testKVHas},
		{"Set", testKVSet},
	}

	generators := [...]Generator{
		new(RandomGenerator),
		new(AscendingGenerator),
		new(DescendingGenerator),
		new(SawtoothGenerator),
	}

	for _, op := range ops {
		t.Run(op.Name, func(t *testing.T) {
			for _, generator := range generators {
				generator.Reset()
				t.Run(generator.String(), func(t *testing.T) {
					t.Parallel()
					t.Run("MemoryPager", func(t *testing.T) {
						op.Func(t, generator, new(MemoryPager))
					})
					t.Run("FilePager", func(t *testing.T) {
						filePager, err := FilePagerNew(generator.String() + "_test.kv")
						if err != nil {
							t.Fatalf("Failed to create new file pager: %v", err)
						}
						defer filePager.Close()
						op.Func(t, generator, filePager)
					})
				})
			}
		})
	}
}

func benchmarkKVGet(b *testing.B, g Generator, pager Pager) {
	b.Helper()

	var kv KV
	if err := kv.Init(pager); err != nil {
		b.Fatalf("Failed to initialize KV: %v", err)
	}

	for i := 0; i < b.N; i++ {
		kv.Set(g.Generate(), 0)
	}

	g.Reset()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = kv.Get(g.Generate())
	}
}

func benchmarkKVDel(b *testing.B, g Generator, pager Pager) {
	b.Helper()

	var kv KV
	if err := kv.Init(pager); err != nil {
		b.Fatalf("Failed to initialize KV: %v", err)
	}

	for i := 0; i < b.N; i++ {
		kv.Set(g.Generate(), 0)
	}

	g.Reset()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kv.Del(g.Generate())
	}
}

func benchmarkKVSet(b *testing.B, g Generator, pager Pager) {
	b.Helper()

	var kv KV
	if err := kv.Init(pager); err != nil {
		b.Fatalf("Failed to initialize KV: %v", err)
	}

	for i := 0; i < b.N; i++ {
		kv.Set(g.Generate(), 0)
	}
}

func BenchmarkKV(b *testing.B) {
	ops := [...]struct {
		Name string
		Func func(*testing.B, Generator, Pager)
	}{
		{"Get", benchmarkKVGet},
		//	{"Del", benchmarkKVDel},
		{"Set", benchmarkKVSet},
	}

	generators := [...]Generator{
		new(RandomGenerator),
		new(AscendingGenerator),
		new(DescendingGenerator),
		new(SawtoothGenerator),
	}

	for _, op := range ops {
		b.Run(op.Name, func(b *testing.B) {
			for _, generator := range generators {
				generator.Reset()
				b.Run(generator.String(), func(b *testing.B) {
					b.Run("MemoryPager", func(b *testing.B) {
						op.Func(b, generator, new(MemoryPager))
					})
					b.Run("FilePager", func(b *testing.B) {
						filePager, err := FilePagerNew(generator.String() + "_test.kv")
						if err != nil {
							b.Fatalf("Failed to create new file pager: %v", err)
						}
						defer filePager.Close()
						op.Func(b, generator, filePager)
					})
				})
			}
		})
	}
}
