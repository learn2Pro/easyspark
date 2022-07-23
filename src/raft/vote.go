package raft

import (
	"easyspark/labrpc"
	"time"
)

// change raft node's role to new role
func (raft *Raft) ChangeRole(newrole NodeRole) {
	if raft.role == newrole {
		return
	}
	raft.role = newrole
	DPrintf("node:%v role change to -> %s\n", raft.me, RoleToString(newrole))
	switch newrole {
	case Follower:
		raft.heartbeatTimer.Stop()
		raft.electionTimer.Reset(time.Duration(MakeAnRandomElectionTimeout(int(raft.baseElecTimeout))) * time.Millisecond)
	case Candidate:

	case Leader:
		lastLog := raft.logs.GetLast()
		raft.leaderId = raft.me
		for i := 0; i < len(raft.peers); i++ {
			raft.matchIdx[i], raft.nextIdx[i] = 0, lastLog.Index+1
		}
		raft.electionTimer.Stop()
		raft.heartbeatTimer.Reset(time.Duration(raft.heartBeatTimeout) * time.Millisecond)
	}
}
func (raft *Raft) IncrGrantedVotes() {
	raft.grantedVotes += 1
}

// Election  make a new election
//
func (raft *Raft) StartNewElection() {
	DPrintf("node:%d start a new election with term:%v\n", raft.me, raft.curTerm)
	raft.grantedVotes = 1
	raft.votedFor = raft.me
	voteReq := &RequestVoteArgs{
		Term:         raft.curTerm,
		CandidateId:  raft.me,
		LastLogIndex: raft.logs.GetLast().Index,
		LastLogTerm:  raft.logs.GetLast().Term,
	}
	// TO DO PERSIST RAFT STATE
	raft.PersistRaftState()

	for id, peer := range raft.peers {
		if id == raft.me || raft.role == Leader {
			continue
		}
		go func(id int, peer *labrpc.ClientEnd) {
			DPrintf("node:%v send request vote to %v %v\n", raft.me, id, voteReq)
			requestVoteResp := &RequestVoteReply{}
			ok := raft.sendRequestVote(id, voteReq, requestVoteResp)

			if !ok {
				DPrintf("I:%v send request vote to %v failed %v \n", raft.me, id, voteReq)
			}

			if requestVoteResp != nil {
				raft.mu.Lock()
				defer raft.mu.Unlock()
				DPrintf("I:%v send request vote to %v recive -> %v, curterm %d, req term %d", raft.me, id, requestVoteResp, raft.curTerm, voteReq.Term)
				if raft.curTerm == voteReq.Term && raft.role == Candidate {
					if requestVoteResp.VoteGranted {
						DPrintf("I:%v got a vote from:%v\n", raft.me, id)
						raft.IncrGrantedVotes()
						if raft.grantedVotes > len(raft.peers)/2 {
							DPrintf("node %d get majority votes int term %d ", raft.me, raft.curTerm)
							raft.ChangeRole(Leader)
							raft.BroadcastHeartbeat()
							raft.grantedVotes = 0
						}
					} else if requestVoteResp.Term > raft.curTerm {
						raft.ChangeRole(Follower)
						raft.curTerm, raft.votedFor = requestVoteResp.Term, None
						// TO DO PERSISTRAFTESTATE
						raft.PersistRaftState()
					}
				}
			}
		}(id, peer)
	}
}

func (raft *Raft) isUpToDate(lastIdx uint64, term int64) bool {
	return term > raft.logs.GetLast().Term || (term == raft.logs.GetLast().Term && lastIdx >= raft.logs.GetLast().Index)
}

//
// example RequestVote RPC handler.
//
func (raft *Raft) RequestVote(args *RequestVoteArgs, resp *RequestVoteReply) {
	// Your code here (2A, 2B).
	raft.mu.Lock()
	defer raft.mu.Unlock()
	// TO DO persistraft
	defer raft.PersistRaftState()

	canVote := raft.votedFor == args.CandidateId ||
		(raft.votedFor == None && raft.leaderId == None) ||
		args.Term > raft.curTerm
	DPrintf("I:%v Handle vote request: %v canVote:%v my term:%v", raft.me, args, canVote, raft.curTerm)

	if canVote && raft.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
		resp.Term, resp.VoteGranted = raft.curTerm, true
	} else {
		resp.Term, resp.VoteGranted = raft.curTerm, false
		return
	}

	raft.votedFor = args.CandidateId
	raft.curTerm = args.Term
	raft.ChangeRole(Follower)
	raft.electionTimer.Reset(time.Millisecond * time.Duration(MakeAnRandomElectionTimeout(int(raft.baseElecTimeout))))
}
