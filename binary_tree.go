package DataStructures

import (
	"golang.org/x/exp/constraints"
)

type BinaryTreeNode[T constraints.Ordered] struct {
	Key   T
	Value any
	Left  *BinaryTreeNode[T]
	Right *BinaryTreeNode[T]
}

type BinaryTreeInterface[T constraints.Ordered] interface {
	Root() *BinaryTreeNode[T]       // returns node at root of tree. O(1)
	Insert(key T, val any)          // inserts node with key, val (can be nil), increases size by 1. O(logn)
	Contains(key T) bool            // checks if tree contains key. O(logn)
	Lookup(key T) any               // returns value for key. O(logn)
	Remove(key T)                   // removes node with key, decreases size by 1. O(logn)
	Empty() bool                    // returns whether tree is empty. O(1)
	InOrderTraversal() []Pair[T]    // returns keys in tree ordered by processing left, current, right. O(n)
	PreOrderTraversal() []Pair[T]   // returns keys in tree ordered by processing current, left, right. O(n)
	PostOrderTraversal() []Pair[T]  // returns keys in tree ordered by processing left, right, current. O(n)
	LevelOrderTraversal() []Pair[T] // returns keys in tree ordered by processing each level left to right. O(n)
}

type BinarySearchTree[T constraints.Ordered] struct {
	root *BinaryTreeNode[T]
}

func NewBinarySearchTree[T constraints.Ordered]() *BinarySearchTree[T] {
	return &BinarySearchTree[T]{nil}
}

func (bst *BinarySearchTree[T]) Insert(key T, val any) {
	newNode := &BinaryTreeNode[T]{key, val, nil, nil}
	if bst.root == nil {
		bst.root = newNode
		return
	}
	curr := bst.root
	for curr != nil {
		if key < curr.Key {
			if curr.Left == nil {
				curr.Left = newNode
				return
			}
			curr = curr.Left
		} else {
			if curr.Right == nil {
				curr.Right = newNode
				return
			}
			curr = curr.Right
		}
	}
}

func (bst *BinarySearchTree[T]) ValueForKey(key T) any {
	curr := bst.root
	for curr != nil {
		if key == curr.Key {
			return curr.Value
		} else if key < curr.Key {
			curr = curr.Left
		} else {
			curr = curr.Right
		}
	}
	return nil
}

func (bst *BinarySearchTree[T]) UpdateValueForKey(key T, val any) {
	curr := bst.root
	for curr.Key != key {
		if key < curr.Key {
			curr = curr.Left
		} else {
			curr = curr.Right
		}
	}
	curr.Value = val
}

func (bst *BinarySearchTree[T]) Remove(key T) {
	parent := (*BinaryTreeNode[T])(nil)
	curr := bst.root
	// Find node & parent
	for curr.Key != key {
		parent = curr
		if key < curr.Key {
			curr = curr.Left
		} else {
			curr = curr.Right
		}
	}
	if curr.Left == nil && curr.Right == nil { // Node has no children
		if curr == bst.root { // Node is root
			bst.root = nil
		} else if parent.Left == curr { // Node is left child
			parent.Left = nil
		} else { // Node is right child
			parent.Right = nil
		}
	} else if curr.Left == nil { // Node has right child
		if curr == bst.root { // Node is root
			bst.root = curr.Right
		} else if parent.Left == curr { // Node is left child
			parent.Left = curr.Right
		} else { // Node is right child
			parent.Right = curr.Right
		}
	} else if curr.Right == nil { // Node has left child
		if curr == bst.root { // Node is root
			bst.root = curr.Left
		} else if parent.Left == curr { // Node is left child
			parent.Left = curr.Left
		} else { // Node is right child
			parent.Right = curr.Left
		}
	} else { // Node has both children
		next := curr.Right
		for next.Left != nil {
			next = next.Left
		}
		newKey := next.Key
		newVal := next.Value
		bst.Remove(newKey)
		curr.Key = newKey
		curr.Value = newVal
	}
}
