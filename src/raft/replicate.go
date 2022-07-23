package raft

import "easyspark/labrpc"

// Replicator manager duplicate run
func (raft *Raft) Replicator(id int, peer *labrpc.ClientEnd) {
	raft.replicatorCond[id].L.Lock()
	defer raft.replicatorCond[id].L.Unlock()
	for !raft.killed() {
		DPrintf("peer id:%d wait for replicating...", id)
		for !(raft.role == Leader && raft.matchIdx[id] < raft.logs.GetLast().Index) {
			raft.replicatorCond[id].Wait()
		}
		raft.replicatorOneRound(id, peer)
	}
}

// replicateOneRound Leader replicates log entries to followers
func (raft *Raft) replicatorOneRound(id int, peer *labrpc.ClientEnd) {
	raft.mu.RLock()
	if raft.role != Leader {
		raft.mu.RUnlock()
		return
	}
	prevLogIndex := raft.nextIdx[id] - 1
	DPrintf("I:%v leader prevLogIndex %d", raft.me, prevLogIndex)
	// snapshot
	if prevLogIndex < raft.logs.GetFirst().Index {
		firstLog := raft.logs.GetFirst()
		snapShotReq := &InstallSnapshotRequest{
			Term:              raft.curTerm,
			LeaderId:          raft.me,
			LastIncludedIndex: firstLog.Index,
			LastIncludedTerm:  firstLog.Term,
			Data:              raft.persister.ReadSnapshot(),
		}
		raft.mu.RUnlock()
		DPrintf("send snapshot to %v with %v\n", id, snapShotReq)
		snapShotResp := &InstallSnapshotResponse{}
		ok := raft.sendInstallSnapshot(id, snapShotReq, snapShotResp)
		if !ok {
			DPrintf("send snapshot to %v failed %v\n", id, snapShotReq)
		}

		raft.mu.Lock()
		DPrintf("send snapshot to %v with resp %v\n", id, snapShotResp)

		if snapShotResp != nil {
			if raft.role == Leader && raft.curTerm == snapShotReq.Term {
				if snapShotResp.Term < raft.curTerm {
					raft.ChangeRole(Follower)
					raft.curTerm = snapShotReq.Term
					raft.votedFor = -1
					raft.PersistRaftState()
				} else {
					DPrintf("set peer %d matchIdx %d\n", id, snapShotReq.LastIncludedIndex)
					raft.matchIdx[id] = snapShotReq.LastIncludedIndex
					raft.nextIdx[id] = snapShotReq.LastIncludedIndex + 1
				}
			}
		}
		raft.mu.Unlock()
	} else {
		firstIndex := raft.logs.GetFirst().Index
		DPrintf("I:%v leader first log index %d", raft.me, firstIndex)
		entries := make([]*Entry, len(raft.logs.EraseBefore(int64(prevLogIndex-firstIndex+1))))
		copy(entries, raft.logs.EraseBefore(int64(prevLogIndex+1-firstIndex)))
		appendEntReq := &AppendEntriesRequest{
			Term:         raft.curTerm,
			LeaderId:     raft.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  raft.logs.GetEntry(int64(prevLogIndex - firstIndex)).Term,
			Entries:      entries,
			LeaderCommit: raft.commitIdx,
		}
		raft.mu.RUnlock()
		resp := &AppendEntriesResponse{}
		ok := raft.sendAppendEntries(id, appendEntReq, resp)
		if !ok {
			DPrintf("send append entries to %v failed %v\n", id, appendEntReq)
		}
		if raft.role == Leader {
			if resp != nil {
				if resp.Success {
					DPrintf("I:%v send heart beat to %v success", raft.me, id)
					raft.matchIdx[id] = appendEntReq.PrevLogIndex + uint64(len(appendEntReq.Entries))
					raft.nextIdx[id] = raft.matchIdx[id] + 1
					raft.advanceCommitIndexForLeader()
				} else {
					if resp.Term > raft.curTerm {
						raft.ChangeRole(Follower)
						raft.curTerm = resp.Term
						raft.votedFor = None
						// TO DO persist
						raft.PersistRaftState()
					} else if resp.Term == raft.curTerm {
						raft.nextIdx[id] = resp.ConflictIndex
						if resp.ConflictTerm != -1 {
							for i := appendEntReq.PrevLogIndex; i >= firstIndex; i-- {
								if raft.logs.GetEntry(int64(i-firstIndex)).Term == resp.ConflictTerm {
									raft.nextIdx[id] = i + 1
									break
								}
							}
						}
					}
				}
			}
		}
	}
}
