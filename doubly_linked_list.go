package DataStructures

type DoubleLinkNode[T any] struct {
	Data T
	Next *DoubleLinkNode[T]
	Prev *DoubleLinkNode[T]
}

type DoublyLinkedListInterface[T any] interface {
	InsertAfter(val T, prev *DoubleLinkNode[T])  // create new node with data val after node prev, increase size by 1 O(1)
	InsertBefore(val T, next *DoubleLinkNode[T]) // create new node with data val before node next, increase size by 1 O(1)
	Remove(node *DoubleLinkNode[T])              // remove node , decrease size by 1 O(1)
	InsertAtFront(val T)                         // create node with data val at front of list, increase size by 1. O(1)
	RemoveAtFront()                              // remove node at front of list, decrease size by 1. O(1)
	InsertAtEnd(val T)                           // create node with data val at end of list, increase size by 1. O(n)
	RemoveAtEnd()                                // remove node at end of list, decrease size by 1. O(n)
	Head() *DoubleLinkNode[T]                    // return first node in list. O(1)
	Tail() *DoubleLinkNode[T]                    // return last node in list. O(1)
	Empty() bool                                 // returns whether list is empty. O(1)
	Size() int                                   // returns number of elements in list. O(1) or O(n) depending on implementation
}

type DoublyLinkedList[T any] struct {
	head *DoubleLinkNode[T]
	tail *DoubleLinkNode[T]
	// you can choose whether to store the size by uncommenting the following line
	size int
}

func (list *DoublyLinkedList[T]) Head() *DoubleLinkNode[T] {
	return list.head
}

func (list *DoublyLinkedList[T]) Tail() *DoubleLinkNode[T] {
	return list.tail
}

func (list *DoublyLinkedList[T]) Size() int {
	return list.size
}

func (list *DoublyLinkedList[T]) Empty() bool {
	if list.head == nil && list.tail == nil {
		return true
	}
	return false
}

func (list *DoublyLinkedList[T]) InsertAtFront(val T) {
	newNode := &DoubleLinkNode[T]{val, nil, nil}
	list.size++
	if list.Empty() {
		list.tail = newNode
	} else {
		newNode.Next = list.head
		list.head.Prev = newNode
	}
	list.head = newNode
}

func (list *DoublyLinkedList[T]) InsertAtEnd(val T) {
	newNode := &DoubleLinkNode[T]{val, nil, nil}
	list.size++
	if list.Empty() {
		list.head = newNode
	} else {
		newNode.Prev = list.tail
		list.tail.Next = newNode
	}
	list.tail = newNode
}

func (list *DoublyLinkedList[T]) RemoveAtFront() {
	if list.Empty() {
		return
	}
	list.size--
	if list.head == list.tail {
		list.head = nil
		list.tail = nil
		return
	}
	next := list.head.Next
	list.head.Next = nil
	next.Prev = nil
	list.head = next
}

func (list *DoublyLinkedList[T]) RemoveAtEnd() {
	if list.Empty() {
		return
	}
	list.size--
	if list.head == list.tail {
		list.head = nil
		list.tail = nil
		return
	}
	next := list.tail.Prev
	list.tail.Prev = nil
	next.Next = nil
	list.tail = next
}

func (list *DoublyLinkedList[T]) InsertAfter(val T, prev *DoubleLinkNode[T]) {
	newNode := &DoubleLinkNode[T]{val, nil, nil}
	list.size++
	if prev == nil {
		list.head = newNode
		list.tail = newNode
		return
	}

	if prev == list.tail {
		list.tail = newNode
	} else {
		next := prev.Next
		next.Prev = newNode
		newNode.Next = next
	}
	prev.Next = newNode
	newNode.Prev = prev
}

func (list *DoublyLinkedList[T]) InsertBefore(val T, next *DoubleLinkNode[T]) {
	newNode := &DoubleLinkNode[T]{val, nil, nil}
	list.size++
	if next == nil {
		list.head = newNode
		list.tail = newNode
		return
	}

	if next == list.head {
		list.head = newNode
	} else {
		prev := next.Prev
		prev.Next = newNode
		newNode.Prev = prev
	}
	next.Prev = newNode
	newNode.Next = next
}

func (list *DoublyLinkedList[T]) Remove(node *DoubleLinkNode[T]) {
	if list.Empty() || node == nil {
		return
	}
	list.size--
	if list.head == list.tail {
		list.head = nil
		list.tail = nil
		return
	}

	if node == list.head {
		next := node.Next
		next.Prev = nil
		list.head = next
	} else if node == list.tail {
		prev := node.Prev
		prev.Next = nil
		list.tail = prev
	} else {
		next := node.Next
		prev := node.Prev
		prev.Next = next
		next.Prev = prev
	}
}
