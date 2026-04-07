package DataStructures

type StackInterface[T any] interface {
	Top() T      // returns top item in stack. O(1)
	Push(val T)  // adds val to top of stack, increases size by 1. O(1)
	Pop()        // removes item from top of stack, decreases size by 1. O(1)
	Empty() bool // returns whether stack is empty. O(1)
	Size() int   // returns number of elements in stack. O(1) or O(n) depending on implementation
}

type Stack[T any] struct {
	list SinglyLinkedList[T]
}

// Alternate implementation of a stack using an array
type AlternateStack[T any] struct {
	arr []T
}

func (stack *Stack[T]) Top() T {
	if stack.list.Empty() {
		var zero T
		return zero
	}
	tail := stack.list.Tail()
	return tail.Data
}

func (stack *Stack[T]) Push(val T) {
	stack.list.InsertAtEnd(val)
}

func (stack *Stack[T]) Pop() {
	stack.list.RemoveAtEnd()
}

func (stack *Stack[T]) Empty() bool {
	return stack.list.Empty()
}

func (stack *Stack[T]) Size() int {
	return stack.list.Size()
}

func (stack *AlternateStack[T]) Top() T {
	last := len(stack.arr)
	if last == 0 {
		var zero T
		return zero
	}
	return stack.arr[last]
}

func (stack *AlternateStack[T]) Push(val T) {
	stack.arr = append(stack.arr, val)
}

func (stack *AlternateStack[T]) Pop() {
	size := len(stack.arr)
	if size == 0 {
		return
	}

	stack.arr = stack.arr[:size-1]
}

func (stack *AlternateStack[T]) Empty() bool {
	if len(stack.arr) == 0 {
		return true
	}
	return false
}
func (stack *AlternateStack[T]) Size() int {
	return len(stack.arr)
}
