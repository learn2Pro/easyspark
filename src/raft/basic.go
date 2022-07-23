package raft

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int64  //current term to vote
	CandidateId  int    //your id for vote
	LastLogIndex uint64 //last log index
	LastLogTerm  int64  //last log term
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int64 //term for candidate to update itself
	VoteGranted bool  //true=means candidate received vote
}
type EntryType uint8

const (
	NORMAL EntryType = iota
	CONF_CHANGE
)

//type LogEntry struct {
//	Index int         //log index start from 0
//	Term  int32       //term for index
//	Cmd   interface{} //the command
//}

type Entry struct {
	EntryType EntryType
	Term      int64
	Index     uint64
	Cmd       interface{}
}

//type AppendEntryReq struct {
//	Term         int32      //current leader term
//	LeaderId     int        //current leader index for rf.peers[]
//	PrevLogIndex int        //previous log index
//	PrevLogTerm  int32      //term for the PrevLogIndex
//	Entries      []LogEntry //empty for heartbeat,multi for efficiency
//	LeaderCommit int        //leader commit index
//	Msg          string     //message for append
//}
//type AppendEntryReply struct {
//	Term               int32 //current term,for leader to update itself
//	Success            bool  //true if follower contained entry matching AppendEntryReq.PrevLogIndex and AppendEntryReq.PrevLogTerm
//	PreIndexOfLastTerm int
//}
type AppendEntriesRequest struct {
	Term         int64
	LeaderId     int
	PrevLogIndex uint64
	PrevLogTerm  int64
	LeaderCommit uint64
	Entries      []*Entry
}

type AppendEntriesResponse struct {
	Term          int64
	Success       bool
	ConflictIndex uint64
	ConflictTerm  int64
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandTerm  int64
	CommandIndex uint64
	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int64
	SnapshotIndex int
}

type InstallSnapshotRequest struct {
	Term              int64
	LeaderId          int
	LastIncludedIndex uint64
	LastIncludedTerm  int64
	Data              []byte
}

type InstallSnapshotResponse struct {
	Term int64
}
