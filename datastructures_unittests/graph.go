package DataStructures

import (
	"golang.org/x/exp/constraints"
)

type GraphInterface[T constraints.Ordered] interface {
	Insert(val Pair[T], neighbors []T) // helper function to build/update graph. inserts T into graph if not present, adds neighbors. O(neighbors)
	Remove(val T)                      // helper function to build/update graph. removes T from graph, removes T from other nodes' neighbors.  O(neighbors)
	Empty() bool                       // returns whether graph is empty. O(1)
	DepthFirstTraversal() []T          // returns values in graph ordered by DFS processing. O(n)
	BreadthFirstTraversal() []T        // returns values in graph ordered by BFS processing. O(n)
	Size() int                         // returns number of items in graph.
}

type Graph[T constraints.Ordered] struct {
	nodes     map[T]struct{}
	// Using second map as a set; ignore the value
	// Ensure on removals you use delete func
	neighbors map[T]map[T]struct{}
}

func NewGraph[T constraints.Ordered]() *Graph[T] {
	return &Graph[T]{
		nodes: make(map[T]struct{}),
		neighbors: make(map[T]map[T]struct{}),
	}
}

func (g *Graph[T]) Empty() bool {
	return len(g.nodes) == 0
}

func (g *Graph[T]) Size() int {
	return len(g.nodes)
}

func (g *Graph[T]) Insert(key T, val struct{}, neighbors[]T) {
	g.nodes[key] = val
	set := make(map[T]struct{})
	for _, neighbor := range neighbors {
		set[neighbor] = struct{}{}
	}
	g.neighbors[key] = set
	for _, neighbor := range neighbors {
		g.neighbors[neighbor][key] = struct{}{}
	}
}

func (g *Graph[T]) Remove(key T) {
  delete(g.nodes, key)
	neighbors := g.neighbors[key]
	for  neighbor, _ := range neighbors {
		delete(g.neighbors[neighbor], key)
	}
	delete(g.neighbors, key)
}