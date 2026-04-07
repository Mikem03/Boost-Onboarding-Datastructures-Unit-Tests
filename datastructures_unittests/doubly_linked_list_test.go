package DataStructures

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func getValues(list *DoublyLinkedList[int]) []int {
	var vals []int
	for n := list.head; n != nil; {
		vals = append(vals, n.Data)
		n = n.Next
	}
	return vals
}

func TestInsertAtFront(t *testing.T) {
	tests := []struct {
		name           string
		nums           []int
		expectedOutput [][]int
	}{
		{
			name: "multi node list",
			nums: []int{1, 2, 3},
			expectedOutput: [][]int{
				{1},
				{2, 1},
				{3, 2, 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			list := &DoublyLinkedList[int]{nil, nil, 0}
			require.NotNil(t, list, "list not instantiated properly")
			for i, val := range tt.nums {
				list.InsertAtFront(val)
				assert.Equal(t, tt.expectedOutput[i], getValues(list), "wrong list state")
			}
		})
	}
}

func TestInsertAtEnd(t *testing.T) {
	tests := []struct {
		name           string
		nums           []int
		expectedOutput [][]int
	}{
		{
			name: "multi node list",
			nums: []int{1, 2, 3},
			expectedOutput: [][]int{
				{1},
				{1, 2},
				{1, 2, 3},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			list := &DoublyLinkedList[int]{nil, nil, 0}
			for i, val := range tt.nums {
				list.InsertAtEnd(val)
				assert.Equal(t, tt.expectedOutput[i], getValues(list), "wrong list state")
			}
		})
	}
}

func TestRemoveAtEnd(t *testing.T) {
	tests := []struct {
		name           string
		input          []int
		expectedOutput []int
	}{
		{
			name:           "multi node list",
			input:          []int{1, 2, 3, 4, 5},
			expectedOutput: []int{1, 2, 3, 4},
		},
		{
			name:           "single node list",
			input:          []int{10},
			expectedOutput: []int(nil),
		},
		{
			name:           "empty list",
			input:          []int(nil),
			expectedOutput: []int(nil),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			list := &DoublyLinkedList[int]{}
			for _, val := range tt.input {
				list.InsertAtEnd(val)
			}
			require.Equal(t, tt.input, getValues(list), "Input list is incorrect")
			list.RemoveAtEnd()
			assert.Equal(t, tt.expectedOutput, getValues(list), "Did not return expected list")
		})
	}
}

func TestRemoveAtFront(t *testing.T) {
	tests := []struct {
		name           string
		input          []int
		expectedOutput []int
	}{
		{
			name:           "multi node list",
			input:          []int{1, 2, 3, 4, 5},
			expectedOutput: []int{2, 3, 4, 5},
		},
		{
			name:           "single node list",
			input:          []int{10},
			expectedOutput: []int(nil),
		},
		{
			name:           "empty list",
			input:          []int(nil),
			expectedOutput: []int(nil),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			list := &DoublyLinkedList[int]{}
			for _, val := range tt.input {
				list.InsertAtEnd(val)
			}
			require.Equal(t, tt.input, getValues(list), "Input list is incorrect")
			list.RemoveAtFront()
			assert.Equal(t, tt.expectedOutput, getValues(list), "Did not return expected list")
		})
	}
}

func TestRemove(t *testing.T) {
	tests := []struct {
		name           string
		input          []int
		idx            int
		expectedOutput []int
	}{
		{
			name:           "multi node list",
			input:          []int{1, 2, 3, 4, 5},
			idx:            2,
			expectedOutput: []int{1, 2, 4, 5},
		},
		{
			name:           "single node list",
			input:          []int{10},
			idx:            0,
			expectedOutput: []int(nil),
		},
		{
			name:           "empty list",
			input:          []int(nil),
			idx:            0,
			expectedOutput: []int(nil),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			list := &DoublyLinkedList[int]{nil, nil, 0}
			for _, val := range tt.input {
				list.InsertAtEnd(val)
			}
			require.Equal(t, tt.input, getValues(list), "Starting list is incorrect")

			//set node
			n := list.head
			for i := 0; i < tt.idx; i++ {
				n = n.Next
			}

			list.Remove(n)
			require.Equal(t, tt.expectedOutput, getValues(list), "list state is incorrect")
		})
	}
}

func TestInsertAfter(t *testing.T) {
	tests := []struct {
		name           string
		nums           []int
		expectedOutput [][]int
	}{
		{
			name: "inserting at tail",
			nums: []int{1, 2, 3},
			expectedOutput: [][]int{
				{1},
				{1, 2},
				{1, 2, 3},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			list := &DoublyLinkedList[int]{nil, nil, 0}
			require.NotNil(t, list, "list not instantiated properly")
			for i, val := range tt.nums {
				list.InsertAfter(val, list.tail)
				assert.Equal(t, tt.expectedOutput[i], getValues(list), "wrong list state")
			}
		})
	}
}

func TestInsertBefore(t *testing.T) {
	tests := []struct {
		name           string
		nums           []int
		expectedOutput [][]int
	}{
		{
			name: "inserting at head",
			nums: []int{1, 2, 3},
			expectedOutput: [][]int{
				{1},
				{2, 1},
				{3, 2, 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			list := &DoublyLinkedList[int]{nil, nil, 0}
			require.NotNil(t, list, "list not instantiated properly")
			for i, val := range tt.nums {
				list.InsertBefore(val, list.head)
				assert.Equal(t, tt.expectedOutput[i], getValues(list), "wrong list state")
			}
		})
	}
}

func TestHead(t *testing.T) {
	tests := []struct {
		name    string
		nums    []int
		wantVal int
		wantErr bool
	}{
		{
			name:    "Multi node list",
			nums:    []int{1, 2, 3},
			wantVal: 1,
			wantErr: false,
		},
		{
			name:    "Single node list",
			nums:    []int{10},
			wantVal: 10,
			wantErr: false,
		},
		{
			name:    "Empty list",
			nums:    []int(nil),
			wantVal: 0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			list := &DoublyLinkedList[int]{nil, nil, 0}
			for _, val := range tt.nums {
				list.InsertAtEnd(val)
			}
			require.Equal(t, tt.nums, getValues(list), "starting list incorrect")

			head := list.Head()
			if tt.wantErr {
				require.Nil(t, head, "Not nil head")
				return
			}

			assert.Equal(t, tt.wantVal, head.Data, "Head not correct")
		})
	}
}

func TestTail(t *testing.T) {
	tests := []struct {
		name    string
		nums    []int
		wantVal int
		wantErr bool
	}{
		{
			name:    "Multi node list",
			nums:    []int{1, 2, 3},
			wantVal: 3,
			wantErr: false,
		},
		{
			name:    "Single node list",
			nums:    []int{10},
			wantVal: 10,
			wantErr: false,
		},
		{
			name:    "Empty list",
			nums:    []int(nil),
			wantVal: 0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			list := &DoublyLinkedList[int]{nil, nil, 0}
			for _, val := range tt.nums {
				list.InsertAtEnd(val)
			}
			require.Equal(t, tt.nums, getValues(list), "starting list incorrect")

			tail := list.Tail()
			if tt.wantErr {
				require.Nil(t, tail, "Not nil tail")
				return
			}

			assert.Equal(t, tt.wantVal, tail.Data, "Tail not correct")
		})
	}
}

func TestEmpty(t *testing.T) {
	tests := []struct {
		name     string
		nums     []int
		wantBool bool
	}{
		{
			name:     "Populated list",
			nums:     []int{1, 2, 3},
			wantBool: false,
		},
		{
			name:     "Empty list",
			nums:     []int(nil),
			wantBool: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			list := &DoublyLinkedList[int]{nil, nil, 0}
			for _, val := range tt.nums {
				list.InsertAtEnd(val)
			}
			require.Equal(t, tt.nums, getValues(list), "starting list incorrect")
			assert.Equal(t, tt.wantBool, list.Empty(), "Empty not correct")
		})
	}
}
