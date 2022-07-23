package raft

import (
	"sync"
)

type RaftLog struct {
	mu         sync.RWMutex
	firstIdx   uint64
	lastIdx    uint64
	appliedIdx int64
	items      []*Entry
	dbEng      KvStore
}

//
// Mem
//

func MakeMemRaftLog() *RaftLog {
	empEnt := &Entry{}
	newItems := []*Entry{}
	newItems = append(newItems, empEnt)
	return &RaftLog{items: newItems, firstIdx: 0, lastIdx: 1}
}

func (raftlog *RaftLog) GetMemFirst() *Entry {
	return raftlog.items[0]
}

func (raftlog *RaftLog) MemLogItemCount() int {
	return len(raftlog.items)
}

func (raftlog *RaftLog) EraseMemBefore(idx int64) []*Entry {
	raftlog.items = raftlog.items[idx:]
	return raftlog.items
}

func (raftlog *RaftLog) EraseMemAfter(idx int64) []*Entry {
	raftlog.items = raftlog.items[:idx]
	return raftlog.items
}

func (raftlog *RaftLog) GetMemBefore(idx int64) []*Entry {
	return raftlog.items[:idx+1]
}

func (raftlog *RaftLog) GetMemAfter(idx int64) []*Entry {
	return raftlog.items[idx:]
}

func (raftlog *RaftLog) GetMemRange(lo, hi int64) []*Entry {
	return raftlog.items[lo:hi]
}

func (raftlog *RaftLog) MemAppend(newEnt *Entry) {
	raftlog.items = append(raftlog.items, newEnt)
}

func (raftlog *RaftLog) GetMemEntry(idx int64) *Entry {
	return raftlog.items[idx]
}

func (raftlog *RaftLog) GetMemLast() *Entry {
	return raftlog.items[len(raftlog.items)-1]
}
