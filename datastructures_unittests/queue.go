package DataStructures

type QueueInterface[T any] interface {
	Front() T      // returns first item in queue. O(1)
	Enqueue(val T) // adds val to end of queue, increases size by 1. O(1)
	Dequeue()      // removes item from front of queue, decreases size by 1. O(1)
	Empty() bool   // returns whether queue is empty. O(1)
	Size() int     // returns number of elements in queue. O(1) or O(n) depending on implementation
}

type Queue[T any] struct {
	list SinglyLinkedListInterface[T]
}

// Alternate implementation of a queue using a circular array
type AlternateQueue[T any] struct {
	arr   []T
	first int
	last  int
}

func (q *Queue[T]) Front() T {
	if q.list.Empty() {
		var zero T
		return zero
	}
	head := q.list.Head()
	return head.Data
}

func (q *Queue[T]) Enqueue(val T) {
	q.list.InsertAtEnd(val)
}

func (q *Queue[T]) Dequeue() {
	q.list.RemoveAtEnd()
}

func (q *Queue[T]) Empty() bool {
	return q.list.Empty()
}

func (q *Queue[T]) Size() int {
	return q.list.Size()
}

// Stack version

func (q *AlternateQueue[T]) Front() T {
	if q.Empty() {
		var zero T
		return zero
	}
	return q.arr[q.first]
}

func (q *AlternateQueue[T]) Size() int {
	return (q.last - q.first + len(q.arr)) % len(q.arr)
}

func (q *AlternateQueue[T]) Empty() bool {
	if q.Size() == q.first && q.Size() == q.last {
		return true
	}
	return false
}
func (q *AlternateQueue[T]) Enqueue(val T) {
	next := (q.last + 1) % q.Size()
	if next == q.first {
		end := q.arr[q.first:]
		q.arr = append(q.arr[:q.first], val)
		q.arr = append(q.arr, end...)
		if q.first > q.last {
			q.first++
		}
	} else {
		q.arr[next] = val
	}
	q.last = next
}

func (q *AlternateQueue[T]) Dequeue() {
	q.first = (q.first + 1) % q.Size()
}
