package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"easyspark/labgob"
	"math/rand"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"easyspark/labgob"
	"easyspark/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
var LEADER int32 = 1
var CANDIDATE int32 = 2
var FOLLOWER int32 = 3

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role                   int32 //1=leader,2=candidate,3=follower
	voteTimeout            int64 // election timeout  in ns
	callRequestVoteTimeout int64 // election timeout  in ns
	heartbeatDelay         int64 // election timeout  in ns
	heartbeatTimeout       int64 // election timeout  in ns
	lastHeartbeat          int64 // election timeout  in ns
	tickDelay              int64 // duration in ns
	tryAppendEntryTimeout  int64 // duration in ns
	callAppendEntryTimeout int64 // duration in ns
	callInstallSnapTimeout int64 // duration in ns
	leaderApplyTimeout     int64 // duration in ns
	maxSendEntrySize       int   // default 100
	//Persistent state on all servers:
	currentTerm int32      // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term (or -1 if none)
	logs        []LogEntry //log entries
	//Volatile state on all servers:
	commitIndex int //committed
	lastApplied int //applied
	//Volatile state on leaders:
	nextIndex  []int //index of log entry send to other server(initialized to leader last log index+1)
	matchIndex []int //index of highest index of other server known to replicated on server
	//apply msg
	applyMsg chan ApplyMsg
	//snapshot
	snapshotStartIndex int    //snapshot index
	snapshotState      []byte //snapshot bytes
	lastIncludedIndex  int
	lastIncludedTerm   int32
}
type LogEntry struct {
	Index int         //log index start from 0
	Term  int32       //term for index
	Cmd   interface{} //the command
}

func (e *LogEntry) OuterIndex() int {
	return e.Index + 1
}

type AppendEntryReq struct {
	Term         int32      //current leader term
	LeaderId     int        //current leader index for rf.peers[]
	PrevLogIndex int        //previous log index
	PrevLogTerm  int32      //term for the PrevLogIndex
	Entries      []LogEntry //empty for heartbeat,multi for efficiency
	LeaderCommit int        //leader commit index
	Msg          string     //message for append
}
type AppendEntryReply struct {
	Term               int32 //current term,for leader to update itself
	Success            bool  //true if follower contained entry matching AppendEntryReq.PrevLogIndex and AppendEntryReq.PrevLogTerm
	PreIndexOfLastTerm int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int32 //current term to vote
	CandidateId  int   //your id for vote
	LastLogIndex int   //last log index
	LastLogTerm  int32 //last log term
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int32 //term for candidate to update itself
	VoteGranted bool  //true=means candidate received vote
}

type InstallSnapshotReq struct {
	Term              int32  //leader's term
	LeaderId          int    //leader id
	LastIncludedIndex int    //the last included index is the index of the last entry in the log that the snapshot replaces (the last entry the state machine had applied)
	LastIncludedTerm  int32  //the last included term is the term of this entry.
	Data              []byte //the snapshot data
}

type InstallSnapshotReply struct {
	Success bool  //install succeed
	Term    int32 //currentTerm, for leader to update itself
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}
func maxOfSlice(a []int) int {
	if len(a) <= 0 {
		return 0
	} else {
		m := -2147483648
		for _, item := range a {
			m = max(m, item)
		}
		return m
	}
}
func (rf *Raft) IsFollower() bool {
	return rf.role == FOLLOWER
}
func (rf *Raft) IsLeader() bool {
	return rf.role == LEADER
}
func (rf *Raft) IsCandidate() bool {
	return rf.role == CANDIDATE
}
func (rf *Raft) ConvertToFollower() bool {
	return rf.convertToExpect(rf.role, FOLLOWER)
}
func (rf *Raft) ConvertToCandidate() bool {
	return rf.convertToExpect(rf.role, CANDIDATE)
}
func (rf *Raft) ConvertToLeader() bool {
	return rf.convertToExpect(rf.role, LEADER)
}
func (rf *Raft) convertToExpect(expect int32, role int32) bool {
	return atomic.CompareAndSwapInt32(&rf.role, expect, role)
}

func (rf *Raft) IncrTermOneStep(expect int32) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm == expect {
		rf.currentTerm = expect + 1
		return true
	} else {
		return false
	}
}
func (rf *Raft) ResetHeartbeatTimer() {
	rf.lastHeartbeat = time.Now().UnixNano()
}
func (rf *Raft) GetRoleTermAndLastTick() (int32, int32, int64) {
	rf.mu.Lock()
	role, t, term := rf.role, rf.lastHeartbeat, rf.currentTerm
	rf.mu.Unlock()
	return role, term, t
}
func (rf *Raft) IsOutOfCurrRange(i int) bool {
	return i-rf.snapshotStartIndex < 0
}
func (rf *Raft) GetLogEntry(i int) *LogEntry {
	//if i-rf.snapshotStartIndex == 0 {
	//DPrintf("server:%v,logs:%v,i:%v,startIndex:%v\n", rf.me, rf.logs, i, rf.snapshotStartIndex)
	//}
	if rf.IsOutOfCurrRange(i) {
		_, _, previous := rf.decodeSnapshot("GetRangeLogEntry")
		return &previous[i]
	} else {
		return &rf.logs[i-rf.snapshotStartIndex]
	}
}
func (rf *Raft) SetLogEntry(i int, entry LogEntry) {
	if i-rf.snapshotStartIndex >= 0 {
		rf.logs[i-rf.snapshotStartIndex] = entry
	}
}
func (rf *Raft) GetRangeLogEntry(from, to int) []LogEntry {
	if rf.IsOutOfCurrRange(from) {
		_, _, previous := rf.decodeSnapshot("GetRangeLogEntry")
		left := previous[from:min(to, len(previous))]
		right := rf.logs[0:max(0, to-rf.snapshotStartIndex)]
		return append(left, right...)
	} else {
		return rf.logs[from-rf.snapshotStartIndex : to-rf.snapshotStartIndex]
	}
}
func (rf *Raft) GetCurrFirstLogEntry() *LogEntry {
	if len(rf.logs) > 0 {
		return &rf.logs[0]
	} else {
		return nil
	}
}
func (rf *Raft) GetCurrLastLogEntry() *LogEntry {
	if len(rf.logs) > 0 {
		return &rf.logs[rf.GetLastLogIndex()]
	} else {
		return nil
	}
}
func (rf *Raft) GetLogEntryLen() int {
	return rf.snapshotStartIndex + len(rf.logs)
}
func (rf *Raft) GetLastLogEntry() *LogEntry {
	if rf.GetLogEntryLen() <= 0 {
		return nil
	} else {
		if len(rf.logs) == 0 {
			_, _, previous := rf.decodeSnapshot("GetLastLogEntry")
			return &previous[len(previous)-1]
		} else {
			return &rf.logs[len(rf.logs)-1]
		}
	}
}
func (rf *Raft) GetLastLogIndex() int {
	entry := rf.GetLastLogEntry()
	if entry == nil {
		return -1
	} else {
		return entry.Index
	}
}
func (rf *Raft) GetLastLogTerm() int32 {
	entry := rf.GetLastLogEntry()
	if entry == nil {
		return -1
	} else {
		return entry.Term
	}
}
func (rf *Raft) GetLastTermIndex(term int32) int {
	for i := rf.GetLastLogIndex(); i >= rf.snapshotStartIndex; i-- {
		if rf.GetLogEntry(i).Term != term {
			return i
		}
	}
	return -1
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	//var term int
	//var isleader bool
	// Your code here (2A).
	//DPrintf("server:%v term:%v role:%v\n", rf.me, rf.currentTerm, rf.role)
	role, term, _ := rf.GetRoleTermAndLastTick()
	return int(term), role == 1
}
func (rf *Raft) ApplyCmd(entry *LogEntry, snapshot bool) bool {
	res := make(chan bool)
	go func() {
		if snapshot {
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(entry.Cmd)
			rf.applyMsg <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      w.Bytes(),
				SnapshotTerm:  int(entry.Term),
				SnapshotIndex: entry.OuterIndex(),
			}
			DPrintf("server:%v snapshot cmd:%v completed!\n", rf.me, entry)
			res <- true
		} else {
			rf.applyMsg <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Cmd,
				CommandIndex: entry.OuterIndex(),
			}
			DPrintf("server:%v apply cmd:%v completed\n", rf.me, entry)
			res <- true
		}
	}()
	select {
	case <-res:
		return true
	case <-time.After(time.Duration(rf.leaderApplyTimeout)):
		return false
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	// persist state
	go func() {
		w := new(bytes.Buffer)
		enc := labgob.NewEncoder(w)
		if err := enc.Encode(rf.currentTerm); err != nil {
			DPrintf("encode current term failed:%v", err)
		}
		if err := enc.Encode(rf.votedFor); err != nil {
			DPrintf("encode rf votedFor failed:%v", err)
		}
		if err := enc.Encode(rf.logs); err != nil {
			DPrintf("encode rf logs failed:%v", err)
		}
		data := w.Bytes()
		// persist snapshot
		ws := new(bytes.Buffer)
		encs := labgob.NewEncoder(ws)
		encs.Encode(rf.snapshotStartIndex)
		encs.Encode(rf.lastIncludedIndex)
		encs.Encode(rf.lastIncludedTerm)
		encs.Encode(rf.snapshotState)
		encs.Encode(rf.commitIndex)
		encs.Encode(rf.lastApplied)
		sdata := ws.Bytes()
		rf.persister.SaveStateAndSnapshot(data, sdata)
	}()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	//read state
	reader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(reader)
	var Term int32
	var VotedFor int
	var Logs []LogEntry
	if decoder.Decode(&Term) != nil ||
		decoder.Decode(&VotedFor) != nil ||
		decoder.Decode(&Logs) != nil {
		DPrintf("%v (%v) read persist error", rf.me, rf.role)
	} else {
		rf.currentTerm = Term
		rf.votedFor = VotedFor
		rf.logs = Logs
	}
}
func (rf *Raft) readSnapshot(data []byte) {
	//read snapshot
	reader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(reader)
	var StartIndex int
	var li int
	var lt int32
	var state []byte
	var commitIndex int
	var lastApplied int
	if decoder.Decode(&StartIndex) != nil ||
		decoder.Decode(&li) != nil ||
		decoder.Decode(&lt) != nil ||
		decoder.Decode(&state) != nil ||
		decoder.Decode(&commitIndex) != nil ||
		decoder.Decode(&lastApplied) != nil {
		DPrintf("%v (%v) read snapshot error", rf.me, rf.role)
	} else {
		rf.snapshotStartIndex = StartIndex
		rf.lastIncludedIndex = li
		rf.lastIncludedTerm = lt
		rf.snapshotState = state
		rf.commitIndex = commitIndex
		rf.lastApplied = lastApplied
		DPrintf("readSnapshot => server:%v,startIndex:%v,lastIncludedIndex:%v,lastIncludedTerm:%v,commitIndex:%v,lastApply:%v\n",
			rf.me, rf.snapshotStartIndex, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.commitIndex, rf.lastApplied)
		rf.commitIndex = max(rf.commitIndex, li)
		rf.tryApplyEntries("readSnapshot", li)
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.GetCurrFirstLogEntry() == nil { //not necessary snapshot
		return
	}
	if index < rf.snapshotStartIndex { //already snapshot
		return
	}
	DPrintf("server:%v,index:%v,snapIndex:%v,logs:%v!\n", rf.me, index, rf.snapshotStartIndex, rf.logs)
	snapshots := rf.GetRangeLogEntry(rf.GetCurrFirstLogEntry().Index, index)
	remain := rf.GetRangeLogEntry(index, rf.GetLogEntryLen())
	_, _, previous := rf.decodeSnapshot("Snapshot")
	li, lt, state := rf.encodeSnapshot(previous, snapshots)
	rf.logs = remain
	rf.snapshotState = state
	rf.snapshotStartIndex = index
	rf.lastIncludedIndex = li
	rf.lastIncludedTerm = lt
	DPrintf("server:%v snapshot completed!\n", rf.me)
}

func (rf *Raft) decodeSnapshot(msg string) (int, int, []LogEntry) {
	if rf.snapshotState == nil {
		return -1, -1, nil
	} else {
		r := bytes.NewBuffer(rf.snapshotState)
		dec := labgob.NewDecoder(r)
		var lastIncludedIndex int
		var lastIncludedTerm int
		var entries []LogEntry
		if dec.Decode(&lastIncludedIndex) != nil ||
			dec.Decode(&lastIncludedTerm) != nil ||
			dec.Decode(&entries) != nil {
			DPrintf("%v => %v (%v) read snapshot error", msg, rf.me, rf.role)
			return -1, -1, nil
		} else {
			return lastIncludedIndex, lastIncludedTerm, entries
		}
	}
}
func (rf *Raft) encodeSnapshot(previous []LogEntry, entries []LogEntry) (int, int32, []byte) {
	merged := make([]LogEntry, 0, len(previous)+len(entries))
	merged = append(merged, previous...)
	merged = append(merged, entries...)
	if len(merged) > 0 {
		lastIncludedIndex, lastIncludedTerm := merged[len(merged)-1].Index, merged[len(merged)-1].Term
		w := new(bytes.Buffer)
		enc := labgob.NewEncoder(w)
		enc.Encode(lastIncludedIndex)
		enc.Encode(lastIncludedTerm)
		enc.Encode(merged)
		return lastIncludedIndex, lastIncludedTerm, w.Bytes()
	} else {
		return -1, -1, nil
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//DPrintf("receive arg term:%v current term:%v cid:%v\n", args.Term, rf.currentTerm, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("server:%v,receive vote:%v,current term:%v,args term:%v,beforeVote:%v,role:%v\n",
		rf.me, args.CandidateId, rf.currentTerm, args.Term, rf.votedFor, rf.role)
	if args.Term >= atomic.LoadInt32(&rf.currentTerm) {
		//Raft uses the voting process to prevent a candidate from
		//winning an election unless its log contains all committed
		//entries. A candidate must contact a majority of the cluster
		//in order to be elected, which means that every committed
		//entry must be present in at least one of those servers. If the
		//candidate’s log is at least as up-to-date as any other log
		//in that majority (where “up-to-date” is defined precisely
		//below), then it will hold all the committed entries. The
		//RequestVote RPC implements this restriction: the RPC
		//includes information about the candidate’s log, and the
		//voter denies its vote if its own log is more up-to-date than
		//that of the candidate.
		//Raft determines which of two logs is more up-to-date
		//by comparing the index and term of the last entries in the
		//logs. If the logs have last entries with different terms, then
		//the log with the later term is more up-to-date. If the logs
		//end with the same term, then whichever log is longer is
		//more up-to-date.
		// not voted || term<args.Term || term==args.Term&&index<=args.Index
		if rf.votedFor == -1 || rf.GetLastLogTerm() < args.LastLogTerm || (rf.GetLastLogTerm() == args.LastLogTerm && rf.GetLastLogIndex() <= args.LastLogIndex) {
			beforeVote := rf.votedFor
			reply.VoteGranted = true
			reply.Term = args.Term
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			rf.ConvertToFollower() //converted to follower=3
			rf.ResetHeartbeatTimer()
			rf.persist()
			DPrintf("server:%v current term:%v args term:%v self logs:%v beforeVote:%v voteFor:%v role:%v\n",
				rf.me, rf.currentTerm, args.Term, rf.logs, beforeVote, rf.votedFor, rf.role)
		} else {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
		}
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotReq, reply *InstallSnapshotReply) {
	DPrintf("receive call install from server:%v lastIncludedIndex:%v!\n", args.LeaderId, args.LastIncludedIndex)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		rf.mu.Lock()
		found := -1
		for i, entry := range rf.logs {
			if entry.Index == args.LastIncludedIndex && entry.Term == args.LastIncludedTerm {
				found = i
				break
			}
		}
		if found >= 0 { //found the index
			//If existing log entry has same index and term as snapshot’s
			//last included entry, retain log entries following it and reply
			rf.logs = rf.logs[found:]
		} else {
			rf.logs = nil
		}
		rf.ResetHeartbeatTimer()
		//reset state in snapshot
		rf.snapshotStartIndex = args.LastIncludedIndex + 1
		rf.snapshotState = args.Data
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
		commit, apply := min(rf.commitIndex, args.LastIncludedIndex), rf.lastApplied
		rf.mu.Unlock()
		//apply snapshot start
		_, _, entries := rf.decodeSnapshot("InstallSnapshot")
		DPrintf("server:%v install snapshot with commit:%v lastInclude:%v apply:%v logs:%v\n", rf.me, commit, args.LastIncludedIndex, apply, rf.logs)
		//rf.mu.Lock()
		if commit > apply {
			cur, first := max(-1, apply)+1, true
			for ; cur <= commit; cur++ {
				item := &entries[cur]
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(item.Cmd)
				if first {
					if !rf.ApplyCmd(item, true) {
						break
					}
					first = false
				} else {
					if !rf.ApplyCmd(item, false) {
						break
					}
				}
			}
			rf.lastApplied = cur - 1
			DPrintf("server:%v lastApplied:%v install snapshot completed!", rf.me, rf.lastApplied)
		}
		//rf.mu.Unlock()
		//reply term
		reply.Term = rf.currentTerm
		reply.Success = true
		//rf.tryApplyEntries("InstallSnapshot", args.LastIncludedIndex)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntryReq, reply *AppendEntryReply) {
	//DPrintf("server:%v,receive append args:%v,rf.currentTerm:%v\n", rf.me, args, rf.currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm { //not leader
		//DPrintf("server:%v,receive lower term:%v,current:%v\n", rf.me, args.Term, rf.currentTerm)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} else { //leader
		//DPrintf("server:%v,receive append args:%v,rf.currentTerm:%v get lock!\n", rf.me, args, rf.currentTerm)
		if !rf.IsFollower() { //convert to follower
			rf.ConvertToFollower()
		}
		rf.ResetHeartbeatTimer()

		if args.PrevLogIndex < rf.GetLogEntryLen() && len(args.Entries) == 0 { //heartbeat
			//Check 2 for the AppendEntries RPC handler should be executed even if the leader didn’t send any entries.
			//DPrintf("server:%v,receive heartbeat:%v,leadercommit:%v,commitIndex:%v,rf.logs[args.PrevLogIndex].Term:%v,argsPreTerm:%v\n")
			if args.PrevLogIndex >= 0 && rf.GetLogEntry(args.PrevLogIndex).Term != args.PrevLogTerm { //not accept start from here
				reply.Success = false
				reply.PreIndexOfLastTerm = rf.GetLastTermIndex(rf.GetLogEntry(args.PrevLogIndex).Term)
			} else {
				rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex)
				rf.tryApplyEntries(args.Msg, args.LeaderCommit)
				reply.Success = true
			}
			reply.Term = rf.currentTerm
			return
		} else if args.PrevLogIndex < rf.GetLogEntryLen() { // check valid
			//Reply false if log doesn’t contain an entry at prevLogIndex
			//whose term matches prevLogTerm (§5.3)
			if args.PrevLogIndex >= 0 && rf.GetLogEntry(args.PrevLogIndex).Term != args.PrevLogTerm { //not accept start from here
				reply.Success = false
				reply.Term = rf.currentTerm
				reply.PreIndexOfLastTerm = rf.GetLastTermIndex(rf.GetLogEntry(args.PrevLogIndex).Term)
				return
			} else {
				DPrintf("%v => follower:%v start to append entries size:%v\n", args.Msg, rf.me, len(args.Entries))
				//clear all after first
				last := args.Entries[len(args.Entries)-1]
				if last.Index >= rf.GetLogEntryLen() {
					for j := rf.GetLogEntryLen(); j <= last.Index; j++ { //append dummy node
						rf.logs = append(rf.logs, LogEntry{Index: -1, Term: last.Term})
					}
				}
				rf.logs = rf.GetRangeLogEntry(rf.snapshotStartIndex, min(last.Index+1, rf.GetLogEntryLen()))
				for _, entry := range args.Entries { //start overwrite
					rf.SetLogEntry(entry.Index, entry)
				}
				//If leaderCommit > commitIndex, set commitIndex =
				//min(leaderCommit, index of last new entry)
				rf.commitIndex = min(args.LeaderCommit, args.Entries[len(args.Entries)-1].Index)
				rf.tryApplyEntries(args.Msg, args.LeaderCommit)
				reply.Success = true
				reply.Term = rf.currentTerm
				DPrintf("%v => follower:%v complete append,term:%v,leader commit:%v,commit:%v\n", args.Msg, rf.me, reply.Term, args.LeaderCommit, rf.commitIndex)
			}
		} else { //if leader previous>follower then ask for decrement
			reply.Success = false
			reply.Term = rf.currentTerm
			reply.PreIndexOfLastTerm = rf.GetLastLogIndex()
			return
		}
		rf.persist()
		//DPrintf("leader:%v to %v send heartbeat term:%v lastHeartbeat:%v\n", args.LeaderId, rf.me, args.Term, rf.lastHeartbeat)
	}
}

func (rf *Raft) tryApplyEntries(msg string, leaderCommit int) {
	if rf.commitIndex > rf.lastApplied {
		cur := max(-1, rf.lastApplied) + 1
		for ; cur <= rf.commitIndex; cur++ {
			entry := rf.GetLogEntry(cur)
			//DPrintf("%v => server:%v to apply cmd:%v,logs:%v,leaderCommit:%v", msg, rf.me, entry, rf.logs, leaderCommit)
			if !rf.ApplyCmd(entry, false) {
				break
			}
		}
		DPrintf("%v => follower:%v apply from:%v to:%v,current len:%v,logs:%v!\n", msg, rf.me, rf.lastApplied, cur-1, rf.GetLogEntryLen(), rf.logs)
		rf.lastApplied = cur - 1
	}
}

func (rf *Raft) tryElection(expectTerm int32) {
	//start := time.Now().Unix()
	//if candidate
	for i := 1; atomic.LoadInt32(&rf.role) == 2; i++ {
		//do send election
		if ok := rf.IncrTermOneStep(expectTerm); !ok {
			return
		}
		rf.ResetHeartbeatTimer()
		beforeTerm := expectTerm + 1
		DPrintf("server:%v term:%v lastIndexTerm:%v lastLogIndex:%v to try election...\n", rf.me, beforeTerm, rf.GetLastLogTerm(), rf.GetLastLogIndex())
		voted := rf.voteForGranted(beforeTerm)
		rf.mu.Lock()
		rf.votedFor = rf.me
		afterTerm, afterRole := rf.currentTerm, rf.role
		if beforeTerm == afterTerm && afterRole == 2 && voted >= len(rf.peers)/2+1 {
			if ok := rf.ConvertToLeader(); !ok {
				return
			} else {
				for j := 0; j < len(rf.nextIndex); j++ { //initialize of leader
					rf.nextIndex[j] = rf.GetLastLogIndex() + 1
					rf.matchIndex[j] = -1
				}
				DPrintf("found leader server:%v term:%v role:%v\n", rf.me, afterTerm, rf.role)
				DPrintf("leader:%v set nextIndex:%v matchIndex:%v\n", rf.me, rf.nextIndex, rf.matchIndex)
			}
		}
		rf.persist()
		rf.mu.Unlock()
	}
}

func (rf *Raft) voteForGranted(term int32) int {
	voted := 0
	if atomic.LoadInt32(&rf.role) == 2 {
		chs := make(chan bool, len(rf.peers))
		for i, _ := range rf.peers {
			if i == rf.me { //no need to self
				chs <- true
			} else {
				go rf.trySendVote(i, term, rf.me, chs)
			}
		}
		for range rf.peers {
			select {
			case ans := <-chs:
				if ans {
					voted += 1
				}
			case <-time.After(time.Duration(rf.callRequestVoteTimeout)):
				//fmt.Printf("invoke election rpc call timeout:%vms skipped!\n", rf.electionTimeout/1000_000)
			}
		}
	}
	return voted
}

func (rf *Raft) trySendVote(server int, term int32, cid int, ch chan bool) {
	args := &RequestVoteArgs{
		Term:         term,
		CandidateId:  cid,
		LastLogIndex: rf.GetLastLogIndex(),
		LastLogTerm:  rf.GetLastLogTerm(),
	}
	reply := &RequestVoteReply{}
	DPrintf("server:%v,request vote to:%v,term:%v,lastLogTerm:%v,lastLogIndex:%v\n", rf.me, server, term, rf.GetLastLogTerm(), rf.GetLastLogIndex())
	rpc := make(chan bool)
	go func(res chan bool) {
		res <- rf.sendRequestVote(server, args, reply)
	}(rpc)
	select {
	case ok := <-rpc:
		DPrintf("server:%v,request vote to:%v,term:%v,lastLogTerm:%v,lastLogIndex:%v,reply:%v\n", rf.me, server, term, rf.GetLastLogTerm(), rf.GetLastLogIndex(), reply)
		if ok {
			if reply.VoteGranted {
				ch <- true
			} else {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.ConvertToFollower()
					rf.ResetHeartbeatTimer()
					rf.currentTerm = reply.Term
					ch <- false
				}
				rf.mu.Unlock()
			}
		} else {
			ch <- false
		}
	case <-time.After(time.Duration(rf.callRequestVoteTimeout)):
		DPrintf("server:%v,request vote to:%v,term:%v,timeout\n", rf.me, server, term)
		ch <- false
	}
}

func (rf *Raft) heartbeat() {
	for {
		if rf.killed() {
			return
		}
		if rf.IsLeader() {
			for server, _ := range rf.peers {
				if server == rf.me {
					//DPrintf("--------------->skip append entry to self!--------------->\n")
					continue
				}
				//DPrintf("------>leader:%v to:%v hearbeat------>", rf.me, i)
				go rf.tryHeartbeatToOne(server)
			}
			time.Sleep(time.Duration(rf.heartbeatDelay)) //heart beat in 200ms
		} else {
			time.Sleep(time.Duration(rf.heartbeatDelay)) //heart beat in 200ms
		}
	}
}
func (rf *Raft) tryHeartbeatToOne(server int) {
	needInstall, prevLogIndex, req, reply := rf.buildAppendReq(server)
	if needInstall && rf.IsLeader() {
		rf.tryInstallToOne(server)
	} else if rf.IsLeader() {
		if !rf.sendAppendEntries(server, req, reply) {
			return
		}
		rf.mu.Lock()
		DPrintf("leader:%v server:%v heartbeat reply:%v\n", rf.me, server, reply)
		if reply.Success { //update next/commit index
			rf.matchIndex[server] = prevLogIndex + len(req.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		} else {
			rf.nextIndex[server] = max(0, reply.PreIndexOfLastTerm)
		}
		rf.mu.Unlock()
		if rf.IsLeader() {
			rf.tryLeaderApply(true)
		}
	}

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotReq, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntryReq, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.killed() {
		DPrintf("%v return to Start() is index: %v, term: %v, it is killed", rf.me, -1, -1)
		return -1, -1, false
	} else {
		_, isLeader := rf.GetState()
		// Your code here (2B).
		if isLeader {
			DPrintf("------------------------>server:%v,term:%v,isleader:%v,receive command------------------------>\n", rf.me, rf.currentTerm, isLeader)
			entry := rf.tryAppendSelf(command)
			DPrintf("leader:%v,append to self index:%v,cmd:%v\n", rf.me, entry.Index, entry.Cmd)
			rf.tryNotifyOthers(entry.Index)
			//majority commitIndex
			//if rf.IsLeader() {
			rf.tryLeaderApply(false)
			//}
			DPrintf("************************server:%v,term:%v,complete: try commitIndex:%v logs:%v************************\n", rf.me, rf.currentTerm, rf.commitIndex, rf.GetLogEntryLen())
			rf.persist()
			return entry.OuterIndex(), int(rf.currentTerm), isLeader
		} else {
			return -1, -1, isLeader
		}
	}

}

func (rf *Raft) tryAppendSelf(command interface{}) LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := rf.GetLastLogIndex() + 1
	entry := LogEntry{
		Index: index,
		Term:  rf.currentTerm,
		Cmd:   command,
	}
	rf.logs = append(rf.logs, entry)
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	return entry
}

//tryNotifyOthers
func (rf *Raft) tryNotifyOthers(index int) {
	res := make(chan bool, len(rf.peers)-1) //channel for followers
	for i, _ := range rf.peers {
		if i == rf.me {
			//DPrintf("--------------->skip append entry to self!--------------->\n")
			continue
		}
		go rf.tryAppendToOne(i, res)
	}
	//wait for remote append
	//tryWaitAndRecv
	succeed, checkNum := 0, len(rf.peers)-1
	for i := 0; i < checkNum; i++ {
		select {
		case ans := <-res:
			if ans {
				succeed++
			}
		}
	}
	DPrintf(">>append remote append index:%v all:%v succeed:%v!>>\n", index, len(rf.peers)-1, succeed)
}

//tryAppendToOne
func (rf *Raft) tryAppendToOne(server int, res chan bool) {
	t0 := time.Now()
	for time.Since(t0) < time.Duration(rf.tryAppendEntryTimeout) {
		if !rf.IsLeader() {
			DPrintf("server:%v not leader skip append to others!", rf.me)
			break
		}
		needInstall, prevLogIndex, req, reply := rf.buildAppendReq(server)
		if needInstall { //install of snapshot
			rf.tryInstallToOne(server)
			break
		} else { //normal append
			DPrintf("leader:%v,send to:%v,req term:%v prevIndex:%v prevTerm:%v size:%v\n", rf.me, server, req.Term, req.PrevLogIndex, req.PrevLogTerm, len(req.Entries))
			rpcCall := make(chan bool)
			go func(res chan bool) {
				res <- rf.sendAppendEntries(server, req, reply)
			}(rpcCall)
			select {
			case _ = <-rpcCall:
				DPrintf("leader:%v,send to:%v,append reply:%v\n", rf.me, server, reply)
				rf.mu.Lock()
				if reply.Success { //update next/commit index
					rf.matchIndex[server] = prevLogIndex + len(req.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					rf.mu.Unlock()
					res <- true
					return
				} else if reply.Term > rf.currentTerm { //convert to follower
					rf.ConvertToFollower()
					rf.currentTerm = reply.Term
					rf.mu.Unlock()
					DPrintf("leader:%v convert to follower because of higher term:%v\n", rf.me, reply.Term)
					res <- false
					return
				} else {
					rf.nextIndex[server] = max(0, reply.PreIndexOfLastTerm)
					//rf.nextIndex[server] = max(0, rf.nextIndex[server]-1) //minimal 0
					t0 = time.Now()
					rf.mu.Unlock()
				}
			case <-time.After(time.Duration(rf.callAppendEntryTimeout * int64(time.Nanosecond))):
				continue
			}
		}
	}
	res <- false
}

func (rf *Raft) buildAppendReq(server int) (bool, int, *AppendEntryReq, *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	idx := rf.nextIndex[server]
	var logTerm int32 = -1
	if idx >= 1 && idx-1 < rf.GetLogEntryLen() {
		logTerm = rf.GetLogEntry(idx - 1).Term
	}
	prevLogIndex := idx - 1
	start := min(rf.GetLogEntryLen(), max(idx, 0))
	end := min(start+rf.maxSendEntrySize, rf.GetLogEntryLen())
	if rf.IsOutOfCurrRange(start) {
		return true, prevLogIndex, nil, nil
	} else {
		req := &AppendEntryReq{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  logTerm,
			Entries:      rf.GetRangeLogEntry(start, end),
			LeaderCommit: rf.commitIndex,
			Msg:          "heartbeat",
		}
		reply := &AppendEntryReply{}
		return false, prevLogIndex, req, reply
	}
}
func (rf *Raft) tryInstallToOne(server int) {
	rf.mu.Lock()
	//build request
	req := &InstallSnapshotReq{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.snapshotState,
	}
	reply := &InstallSnapshotReply{}
	rf.mu.Unlock()
	DPrintf("leader:%v,server:%v call install of LastIncludedIndex:%v term:%v\n", rf.me, server, req.LastIncludedIndex, req.Term)
	res := make(chan bool)
	go func(ch chan bool) {
		ch <- rf.sendInstallSnapshot(server, req, reply)
	}(res)
	select {
	case ok := <-res:
		if ok {
			rf.mu.Lock()
			DPrintf("leader:%v,server:%v call install reply:%v\n", rf.me, server, reply)
			if reply.Success {
				rf.matchIndex[server] = rf.lastIncludedIndex
				rf.nextIndex[server] = rf.matchIndex[server] + 1
			} else if reply.Term > rf.currentTerm {
				DPrintf("InstallSnapshot => leader:%v,to:%v,convert to follower by higher term:%v,origin term:%v\n",
					rf.me, server, reply.Term, rf.currentTerm)
				rf.currentTerm = reply.Term
				rf.ResetHeartbeatTimer()
				rf.ConvertToFollower()
			}
			rf.mu.Unlock()
		}
	case <-time.After(time.Duration(rf.callInstallSnapTimeout)):
	}
}

//tryLeaderApply
func (rf *Raft) tryLeaderApply(heartbeat bool) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//If there exists an N such that N > commitIndex, a majority
	//of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	//set commitIndex = N (§5.3, §5.4).
	if !heartbeat {
		DPrintf("before apply => followers:%v,commitIndex:%v,applied:%v\n", rf.matchIndex, rf.commitIndex, rf.lastApplied)
	}
	for i := rf.commitIndex + 1; i <= maxOfSlice(rf.matchIndex); i++ {
		var majority int
		for _, followerCommit := range rf.matchIndex {
			if followerCommit >= i {
				majority += 1
			}
		}
		if majority > len(rf.peers)/2 && rf.GetLogEntry(i).Term == rf.currentTerm {
			rf.commitIndex = i
		}
	}
	for rf.lastApplied < rf.commitIndex {
		entry := rf.GetLogEntry(rf.lastApplied + 1)
		DPrintf("leader:%v to apply cmd:%v,logs:%v,lastApplied:%v!", rf.me, entry, rf.logs, rf.lastApplied)
		if rf.ApplyCmd(entry, false) {
			rf.lastApplied += 1
		} else {
			break
		}
	}
	if !heartbeat {
		DPrintf("after apply => followers:%v,commitIndex:%v,applied:%v\n", rf.matchIndex, rf.commitIndex, rf.lastApplied)
	}
	return rf.commitIndex
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		sleep := rf.tickDelay + rand.Int63n(rf.tickDelay/2)
		//DPrintf("server:%v sleep:%vms role:%v lastHeartbeat:%v for ticker!\n", rf.me, sleep/1000, rf.role, rf.lastHeartbeat)
		time.Sleep(time.Duration(sleep)) //rand in [tickDelay,tickDelay+10)ms
		role, term, t := rf.GetRoleTermAndLastTick()
		if role != 1 && time.Now().UnixNano()-t > rf.heartbeatTimeout {
			//convert to candidate=2
			//DPrintf("server:%v,receive leader message timeout:%vms,try election...\n", rf.me, rf.heartbeatTimeout/1000)
			//DPrintf("follower:%v convert to candidate with timeout:%v\n", rf.me, time.Now().UnixNano()-t)
			if ok := rf.ConvertToCandidate(); !ok { // convert failed continue
				continue
			}
			//do election
			rf.tryElection(term)
		}

	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.heartbeatTimeout = 500_000_000       //heartbeatDelay*3=400ms
	rf.heartbeatDelay = 150_000_000         //heartbeatDelay=200ms
	rf.tickDelay = 200_000_000              //heartbeatDelay=200ms
	rf.voteTimeout = 200_000_000            //call vote all timeout 200ms
	rf.tryAppendEntryTimeout = 30_000_000   //30ms
	rf.callRequestVoteTimeout = 100_000_000 //100ms
	rf.callAppendEntryTimeout = 20_000_000  //20ms
	rf.callInstallSnapTimeout = 50_000_000  //100ms
	rf.leaderApplyTimeout = 100_000_000     //100ms
	rf.maxSendEntrySize = 100               //max send 100 entry one rpc
	rf.votedFor = -1
	rf.commitIndex = -1       //set to -1
	rf.lastApplied = -1       //set to -1
	rf.snapshotStartIndex = 0 //set to 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyMsg = applyCh

	DPrintf("server:%v,set callRequestVoteTimeout:%vms,heartbeatTimeout:%vms,heartbeatDelay:%vms,tickDelay:%vms\n", rf.me, rf.callRequestVoteTimeout/1000_000, rf.heartbeatTimeout/1000_000, rf.heartbeatDelay/1000_000, rf.tickDelay/1000_000)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeat()

	return rf
}
