package DataStructures

type SingleLinkNode[T any] struct {
	Data T
	Next *SingleLinkNode[T]
}

type SinglyLinkedListInterface[T any] interface {
	InsertAfter(val T, prev *SingleLinkNode[T]) // create new node with data val after node prev, increase size by 1. O(1)
	RemoveAfter(prev *SingleLinkNode[T])        // remove node after node prev, decrease size by 1. O(1)
	InsertAtFront(val T)                        // create node with data val at front of list, increase size by 1. O(1)
	RemoveAtFront()                             // remove node at front of list, decrease size by 1. O(1)
	InsertAtEnd(val T)                          // create node with data val at end of list, increase size by 1. O(n)
	RemoveAtEnd()                               // remove node at end of list, decrease size by 1. O(n)
	Head() *SingleLinkNode[T]                   // return first node in list. O(1)
	Tail() *SingleLinkNode[T]                   // return last node in list. O(n)
	Empty() bool                                // returns whether list is empty. O(1)
	Size() int                                  // returns number of elements in list. O(1) or O(n) depending on implementation
}

type SinglyLinkedList[T any] struct {
	head *SingleLinkNode[T]
	tail *SingleLinkNode[T]
	size int
}

func NewSinglyLinkedList[T any]() *SinglyLinkedList[T] {
	return &SinglyLinkedList[T]{
		head: nil,
		tail: nil,
		size: 0,
	}
}
func (list *SinglyLinkedList[T]) InsertAfter(val T, prev *SingleLinkNode[T]) {
	newNode := &SingleLinkNode[T]{val, nil}
	list.size++
	if prev == nil {
		list.head = newNode
		list.tail = newNode
		return
	}
	if prev.Next == nil {
		list.tail = newNode
	}
	newNode.Next = prev.Next
	prev.Next = newNode
}

func (list *SinglyLinkedList[T]) RemoveAfter(prev *SingleLinkNode[T]) {
	if prev == nil || prev == list.tail {
		return
	}
	nextNode := prev.Next.Next
	if nextNode == nil {
		list.tail = prev
	}
	prev.Next = nextNode
	list.size--
}

func (list *SinglyLinkedList[T]) InsertAtFront(val T) {
	newNode := &SingleLinkNode[T]{val, list.head}
	if list.size == 0 {
		list.tail = newNode
	}
	list.head = newNode
	list.size++
}

func (list *SinglyLinkedList[T]) RemoveAtFront() {
	if list.Size() == 0 || list == nil {
		return
	}
	secondNode := list.head.Next
	if secondNode == nil {
		list.head = nil
		list.tail = nil
		list.size--
		return
	}
	list.head.Next = nil
	list.head = secondNode
	list.size--
}

func (list *SinglyLinkedList[T]) InsertAtEnd(val T) {
	newNode := &SingleLinkNode[T]{val, nil}
	if list.size == 0 {
		list.head = newNode
	} else {
		list.tail.Next = newNode
	}
	list.tail = newNode
	list.size++
}

func (list *SinglyLinkedList[T]) RemoveAtEnd() {
	if list.Size() == 0 || list == nil {
		return
	}

	if list.tail == list.head {
		list.head = nil
		list.tail = nil
		list.size--
		return
	}
	newTail := list.head
	for newTail != nil && newTail.Next != list.tail {
		newTail = newTail.Next
	}
	list.tail = newTail
	newTail.Next = nil
	list.size--
}

func (list *SinglyLinkedList[T]) Head() *SingleLinkNode[T] {
	return list.head
}

func (list *SinglyLinkedList[T]) Tail() *SingleLinkNode[T] {
	return list.tail
}

func (list *SinglyLinkedList[T]) Size() int {
	return list.size
}

func (list *SinglyLinkedList[T]) Empty() bool {
	if list.size == 0 {
		return true
	} else {
		return false
	}
}
