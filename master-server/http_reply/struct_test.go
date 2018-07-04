package http_reply

import (
	"container/heap"
	"testing"
	"fmt"
)

func Test_RangeStatHeap(t *testing.T) {
	h := &RangeStatHeap{}
	heap.Push(h, RangeStatsInfo{RangeId: 1, LeaderId: 2, WriteOps: 1002})
	heap.Push(h, RangeStatsInfo{RangeId: 2, LeaderId: 3, WriteOps: 87})
	heap.Push(h, RangeStatsInfo {RangeId: 4, LeaderId: 5})
	heap.Push(h, RangeStatsInfo{RangeId: 6, LeaderId: 7, WriteOps: 1021})
	for h.Len() > 0 {
		fmt.Printf("%v", heap.Pop(h))
	}

}

func Test_RangeStateQuery(t *testing.T)  {
	topN := 2
	h := &RangeStatHeap{}
	heap.Push(h, RangeStatsInfo{RangeId: 1, LeaderId: 2, WriteOps: 1002})
	heap.Push(h, RangeStatsInfo{RangeId: 2, LeaderId: 3, WriteOps: 87})
	heap.Push(h, RangeStatsInfo {RangeId: 4, LeaderId: 5})
	heap.Push(h, RangeStatsInfo{RangeId: 6, LeaderId: 7, WriteOps: 1021})
	if topN > h.Len() {
		topN = h.Len()
	}

	var result []RangeStatsInfo
	for i := 0; i< topN; i++ {
		result = append(result,  heap.Pop(h).(RangeStatsInfo))
	}

	fmt.Printf("%v", result)
}