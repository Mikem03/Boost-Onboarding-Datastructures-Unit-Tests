package DataStructures

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRemoveAfter(t *testing.T) {
	tests := []struct {
		name           string
		input          []int
		prevIndex      int
		expectedOutput []int
	}{
		{
			name:           "multi node list",
			input:          []int{1, 2, 3, 4},
			prevIndex:      1,
			expectedOutput: []int{1, 2, 4},
		},
		{
			name:           "single node list",
			input:          []int{10},
			prevIndex:      0,
			expectedOutput: []int{10},
		},
		{
			name:           "empty list",
			input:          []int(nil),
			prevIndex:      -1,
			expectedOutput: []int(nil),
		},
		{
			name:           "no removal at tail",
			input:          []int{1, 2, 3},
			prevIndex:      2,
			expectedOutput: []int{1, 2, 3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			list := NewSinglyLinkedList[int]()
			require.NotNil(t, list, "Nil list")
			for _, val := range tt.input {
				list.InsertAtEnd(val)
			}
			require.Equal(t, tt.input, getValues(list), "Starting list state is incorrect")

			var prev *SingleLinkNode[int]
			if tt.prevIndex >= 0 {
				prev = list.head
				for i := 0; i < tt.prevIndex && prev != nil; i++ {
					prev = prev.Next
				}
			}

			list.RemoveAfter(prev)
			assert.Equal(t, tt.expectedOutput, getValues(list), "Expected list not given")
		})
	}
}
func TestInsertAfter(t *testing.T) {
	tests := []struct {
		name           string
		nums           []int
		input          int
		prevIndex      int
		expectedOutput []int
	}{
		{
			name:           "multi node list",
			nums:           []int{1, 2, 4},
			input:          3,
			prevIndex:      1,
			expectedOutput: []int{1, 2, 3, 4},
		},
		{
			name:           "empty list",
			nums:           []int(nil),
			input:          10,
			prevIndex:      0,
			expectedOutput: []int{10},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			list := NewSinglyLinkedList[int]()
			for _, val := range tt.nums {
				list.InsertAtEnd(val)
			}
			require.Equal(t, tt.nums, getValues(list), "Starting list state is incorrect")

			// set prev then call InsertAfter
			if tt.prevIndex == 0 {
				list.InsertAfter(tt.input, nil)
			} else {
				prev := list.head
				for i := 0; i < tt.prevIndex; i++ {
					prev = prev.Next
				}
				list.InsertAfter(tt.input, prev)
			}

			assert.Equal(t, tt.expectedOutput, getValues(list), "list does not match expected output")
		})
	}
}

func TestInsertAtFront(t *testing.T) {
	tests := []struct {
		name           string
		nums           []int
		input          int
		expectedOutput []int
	}{
		{
			name:           "multi node list",
			nums:           []int{1, 2, 3},
			input:          0,
			expectedOutput: []int{0, 1, 2, 3},
		},
		{
			name:           "empty list",
			nums:           []int(nil),
			input:          10,
			expectedOutput: []int{10},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			list := NewSinglyLinkedList[int]()
			for _, val := range tt.nums {
				list.InsertAtEnd(val)
			}
			require.Equal(t, tt.nums, getValues(list), "Starting list state is incorrect")
			list.InsertAtFront(tt.input)
			assert.Equal(t, tt.expectedOutput, getValues(list), "Did not return expected list")
		})
	}
}

func TestInsertAtEnd(t *testing.T) {
	tests := []struct {
		name           string
		nums           []int
		input          int
		expectedOutput []int
	}{
		{
			name:           "multi node list",
			nums:           []int{1, 2, 3},
			input:          4,
			expectedOutput: []int{1, 2, 3, 4},
		},
		{
			name:           "empty list",
			nums:           []int(nil),
			input:          10,
			expectedOutput: []int{10},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			list := NewSinglyLinkedList[int]()
			for _, val := range tt.nums {
				list.InsertAtEnd(val)
			}
			require.Equal(t, tt.nums, getValues(list), "Starting list state is incorrect")
			list.InsertAtEnd(tt.input)
			assert.Equal(t, tt.expectedOutput, getValues(list), "Did not return expected list")
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
			list := NewSinglyLinkedList[int]()
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
			list := NewSinglyLinkedList[int]()
			for _, val := range tt.input {
				list.InsertAtEnd(val)
			}
			require.Equal(t, tt.input, getValues(list), "Input list is incorrect")
			list.RemoveAtFront()
			assert.Equal(t, tt.expectedOutput, getValues(list), "Did not return expected list")
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
			list := &SinglyLinkedList[int]{nil, nil, 0}
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
			list := &SinglyLinkedList[int]{nil, nil, 0}
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
			list := &SinglyLinkedList[int]{nil, nil, 0}
			for _, val := range tt.nums {
				list.InsertAtEnd(val)
			}
			require.Equal(t, tt.nums, getValues(list), "starting list incorrect")
			assert.Equal(t, tt.wantBool, list.Empty(), "Empty not correct")
		})
	}
}

func getValues(list *SinglyLinkedList[int]) []int {
	var vals []int
	for n := list.head; n != nil; {
		vals = append(vals, n.Data)
		n = n.Next
	}
	return vals
}

/*

TestEmpty(t *testing.T)
TestSize(t *testing.T)
*/
