package raft

// Applier() Write the commited message to the applyCh channel
// and update lastApplied
func (raft *Raft) Applier() {
	for !raft.killed() {
		raft.mu.Lock()
		for raft.lastApplied >= raft.commitIdx {
			DPrintf("applier ... ")
			raft.applyCond.Wait()
		}
		firstIndex, commitIndex, lastApplied := raft.logs.GetFirst().Index, raft.commitIdx, raft.lastApplied
		entries := make([]*Entry, commitIndex-lastApplied)
		copy(entries, raft.logs.GetRange(int64(lastApplied+1-firstIndex), int64(commitIndex+1-firstIndex)))
		DPrintf("%d, applies entries %d-%d in term %d", raft.me, raft.lastApplied, commitIndex, raft.curTerm)
		raft.mu.Unlock()
		for _, entry := range entries {
			raft.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Cmd,
				CommandTerm:  entry.Term,
				CommandIndex: entry.Index,
			}
		}
		raft.mu.Lock()
		raft.lastApplied = uint64(Max(int(raft.lastApplied), int(commitIndex)))
		raft.mu.Unlock()
	}
}
