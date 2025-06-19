package promsketch

import (
	"fmt"
)

type Item struct {
	key   string
	count int64
}

type TopKHeap struct {
	heap     []Item
	k        int
	totalMem float64 // Bytes
}

func (topkheap *TopKHeap) Clean() {
	topkheap.heap = topkheap.heap[:0]
}

func (topkheap *TopKHeap) GetMemoryBytes() float64 {
	return topkheap.totalMem // Bytes
}

func NewTopKHeap(k int) (topkheap *TopKHeap) {
	topkheap = &TopKHeap{
		heap:     make([]Item, 0, k),
		k:        k,
		totalMem: 0,
	}
	return topkheap
}

func NewTopKFromHeap(from *TopKHeap) (topkheap *TopKHeap) {
	topkheap = &TopKHeap{
		k:    from.k,
		heap: make([]Item, len(from.heap)),
	}
	for i, item := range from.heap {
		topkheap.heap[i].key = item.key
		topkheap.heap[i].count = item.count
	}

	return topkheap
}

func (topkheap *TopKHeap) Print() {
	for _, item := range topkheap.heap {
		fmt.Println(item.key, ":", item.count)
	}
}

func (topkheap *TopKHeap) Find(key string) (int, bool) {
	for i, item := range topkheap.heap {
		if item.key == key {
			return i, true
		}
	}
	return -1, false
}

func (topkheap *TopKHeap) leftChild(i int) int {
	return 2*i + 1
}

func (topkheap *TopKHeap) rightChild(i int) int {
	return 2*i + 2
}

func (topkheap *TopKHeap) parent(i int) int {
	return (i - 1) / 2
}

func (topkheap *TopKHeap) swap(i, j int) {
	var key string
	var count int64
	key = topkheap.heap[i].key
	topkheap.heap[i].key = topkheap.heap[j].key
	topkheap.heap[j].key = key

	count = topkheap.heap[i].count
	topkheap.heap[i].count = topkheap.heap[j].count
	topkheap.heap[j].count = count
}

// update modifies the priority and value of an Item in the queue.
func (topkheap *TopKHeap) UpdateCS(key string, count int64) bool {

	index, find := topkheap.Find(key)

	if find {
		topkheap.heap[index].count = topkheap.heap[index].count + 1
		topkheap.updateOrder(index)
		return true
	} else {
		topkheap.Insert(key, count)
		return true
	}
}

// update modifies the priority and value of an Item in the queue.
func (topkheap *TopKHeap) Update(key string, count int64) bool {

	index, find := topkheap.Find(key)

	if find {
		topkheap.heap[index].count = count
		topkheap.updateOrder(index)
		return true
	} else {
		topkheap.Insert(key, count)
		return true
	}
}

func (topkheap *TopKHeap) Insert(key string, count int64) {
	if int(len(topkheap.heap)) < topkheap.k {
		topkheap.heap = append(topkheap.heap, Item{
			key:   key,
			count: count,
		})
		topkheap.totalMem += float64(len(key)) + 8
		topkheap.updateOrderUp(len(topkheap.heap) - 1)
	} else {
		if topkheap.heap[0].count < count {
			topkheap.heap[0].count = count
			topkheap.heap[0].key = key
			topkheap.updateOrderDown(0)
		}
	}
}

func (topkheap *TopKHeap) updateOrder(i int) {
	if !topkheap.updateOrderDown(i) {
		topkheap.updateOrderUp(i)
	}
}

func (topkheap *TopKHeap) updateOrderDown(i int) bool {
	n := len(topkheap.heap)
	i0 := i
	var (
		l, r, smallest int = 0, 0, 0
	)
	for i < n {
		l = topkheap.leftChild(i)
		r = topkheap.rightChild(i)
		smallest = i

		if l < n && topkheap.heap[smallest].count > topkheap.heap[l].count {
			smallest = l
		}
		if r < n && topkheap.heap[smallest].count > topkheap.heap[r].count {
			smallest = r
		}

		if smallest != i {
			topkheap.swap(smallest, i)
		} else {
			break
		}
		i = smallest
	}
	return i > i0
}

func (topkheap *TopKHeap) updateOrderUp(i int) {
	var par int = 0
	for i > 0 {
		par = topkheap.parent(i)
		if topkheap.heap[par].count > topkheap.heap[i].count {
			topkheap.swap(par, i)
			i = par
		} else {
			break
		}
	}
}
