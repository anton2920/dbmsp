package main

import (
	"fmt"
	"math/rand"

	"github.com/anton2920/gofa/util"
)

type Generator interface {
	fmt.Stringer

	Generate() int
	Reset()
}

type RandomGenerator struct {
	Rng *rand.Rand
}

func (g *RandomGenerator) Generate() int {
	return g.Rng.Int()
}

func (g *RandomGenerator) Reset() {
	g.Rng = rand.New(rand.NewSource(100500))
}

func (g *RandomGenerator) String() string {
	return "Random"
}

type AscendingGenerator struct {
	Current int
}

func (g *AscendingGenerator) Generate() int {
	ret := g.Current
	g.Current++
	return ret
}

func (g *AscendingGenerator) Reset() {
	g.Current = 0
}

func (g *AscendingGenerator) String() string {
	return "Ascending"
}

type DescendingGenerator struct {
	Current int
}

func (g *DescendingGenerator) Generate() int {
	ret := g.Current
	g.Current--
	return int(ret)
}

func (g *DescendingGenerator) Reset() {
	g.Current = 0
}

func (g *DescendingGenerator) String() string {
	return "Descending"
}

type SawtoothGenerator struct {
	Current int
}

func (g *SawtoothGenerator) Generate() int {
	ret := g.Current
	g.Current = -g.Current + (1 * -util.Bool2Int(g.Current >= 0))
	return ret
}

func (g *SawtoothGenerator) Reset() {
	g.Current = 0
}

func (g *SawtoothGenerator) String() string {
	return "Sawtooth"
}

var (
	_ Generator = &RandomGenerator{}
	_ Generator = &AscendingGenerator{}
	_ Generator = &DescendingGenerator{}
	_ Generator = &SawtoothGenerator{}
)
