package DataStructures

type DequeInterface[T any] interface {
	Front() T        // returns first item in deque. O(1)
	Back() T         // returns last item in deque. O(1)
	PushFront(val T) // adds val to front of queue, increases size by 1. O(1)
	PushBack(val T)  // adds val to back of queue, increases size by 1. O(1)
	PopFront()       // removes item from front of queue, decreases size by 1. O(1)
	PopBack()        // removes item from back of queue, decreases size by 1. O(1)
	Empty() bool     // returns whether queue is empty. O(1)
	Size() int       // returns number of elements in queue. O(1) or O(n) depending on implementation
}

type Deque[T any] struct {
	list DoublyLinkedListInterface[T]
}

func (q *Deque[T]) Front() T {
	head := q.list.Head()
	return head.Data
}

func (q *Deque[T]) Back() T {
	tail := q.list.Tail()
	return tail.Data
}

func (q *Deque[T]) PushFront(val T) {
	q.list.InsertAtFront(val)
}

func (q *Deque[T]) PushBack(val T) {
	q.list.InsertAtEnd(val)
}

func (q *Deque[T]) PopFront() {
	q.list.RemoveAtFront()
}

func (q *Deque[T]) PopBack() {
	q.list.RemoveAtFront()
}

func (q *Deque[T]) Empty() bool {
	return q.list.Empty()
}

func (q *Deque[T]) Size() int {
	return q.list.Size()
}
