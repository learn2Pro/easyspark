package raft

import "easyspark/labrpc"

func (rf *Raft) Heartbeat() {

}

// BroadcastHeartbeat broadcast heartbeat to peers
func (raft *Raft) BroadcastHeartbeat() {
	for id, peer := range raft.peers {
		if id == raft.me {
			continue
		}
		DPrintf("I:%v send heart beat to %v", raft.me, id)
		go func(id int, peer *labrpc.ClientEnd) {
			raft.replicatorOneRound(id, peer)
		}(id, peer)
	}
}
