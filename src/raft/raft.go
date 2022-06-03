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

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	//var term int
	//var isleader bool
	// Your code here (2A).
	//DPrintf("server:%v term:%v role:%v\n", rf.me, rf.currentTerm, rf.role)
	role, term, _ := rf.getRoleTermAndLastTick()
	return int(term), role == 1
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
	rf.persister.SaveRaftState(data)
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

}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//DPrintf("receive arg term:%v current term:%v cid:%v\n", args.Term, rf.currentTerm, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("server:%v,receive vote:%v,current term:%v,args term:%v,self logs:%v,beforeVote:%v,role:%v\n",
		rf.me, args.CandidateId, rf.currentTerm, args.Term, rf.logs, rf.votedFor, rf.role)
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
		if rf.votedFor == -1 || rf.getLastLogTerm() < args.LastLogTerm || (rf.getLastLogTerm() == args.LastLogTerm && rf.getLastLogIndex() <= args.LastLogIndex) {
			beforeVote := rf.votedFor
			reply.VoteGranted = true
			reply.Term = args.Term
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			rf.convertToRole(FOLLOWER) //converted to follower=3
			rf.lastHeartbeat = time.Now().UnixNano()
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
func (rf *Raft) getLastTermIndex(term int32) int {
	for i := rf.getLastLogIndex(); i >= 0; i-- {
		if rf.logs[i].Term != term {
			return i
		}
	}
	return -1
}
func (rf *Raft) AppendEntries(args *AppendEntryReq, reply *AppendEntryReply) {
	//heartbeat
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm { //not leader
		DPrintf("server:%v,receive lower term:%v,current:%v\n", rf.me, args.Term, rf.currentTerm)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} else { //leader
		rf.lastHeartbeat = time.Now().UnixNano()
		//DPrintf("follower:%v update heartbeat:%v\n", rf.me, rf.lastHeartbeat)
		if rf.role != FOLLOWER { //convert to follower
			rf.convertToRole(FOLLOWER)
		}

		if args.PrevLogIndex < len(rf.logs) && len(args.Entries) == 0 { //heartbeat
			//Check 2 for the AppendEntries RPC handler should be executed even if the leader didn’t send any entries.
			//DPrintf("server:%v,receive heartbeat:%v,leadercommit:%v,commitIndex:%v,rf.logs[args.PrevLogIndex].Term:%v,argsPreTerm:%v\n")
			if args.PrevLogIndex >= 0 && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm { //not accept start from here
				reply.Success = false
				reply.PreIndexOfLastTerm = rf.getLastTermIndex(rf.logs[args.PrevLogIndex].Term)
			} else {
				rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex)
				rf.tryApplyEntries(args.Msg, args.LeaderCommit)
				reply.Success = true
			}
			reply.Term = rf.currentTerm
			return
		} else if args.PrevLogIndex < len(rf.logs) { // check valid
			//Reply false if log doesn’t contain an entry at prevLogIndex
			//whose term matches prevLogTerm (§5.3)
			if args.PrevLogIndex >= 0 && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm { //not accept start from here
				reply.Success = false
				reply.Term = rf.currentTerm
				reply.PreIndexOfLastTerm = rf.getLastTermIndex(rf.logs[args.PrevLogIndex].Term)
				return
			} else {
				DPrintf("%v => follower:%v start to append entries size:%v\n", args.Msg, rf.me, len(args.Entries))
				//clear all after first
				last := args.Entries[len(args.Entries)-1]
				if last.Index >= len(rf.logs) {
					for j := len(rf.logs); j <= last.Index; j++ { //append dummy node
						rf.logs = append(rf.logs, LogEntry{Index: -1, Term: last.Term})
					}
				}
				rf.logs = rf.logs[:min(last.Index+1, len(rf.logs))]
				for _, entry := range args.Entries { //start overwrite
					rf.logs[entry.Index] = entry
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
			reply.PreIndexOfLastTerm = rf.getLastLogIndex()
			return
		}
		rf.persist()
		//DPrintf("leader:%v to %v send heartbeat term:%v lastHeartbeat:%v\n", args.LeaderId, rf.me, args.Term, rf.lastHeartbeat)
	}
}

func (rf *Raft) tryApplyEntries(msg string, leaderCommit int) {
	if rf.commitIndex > rf.lastApplied {
		cur := max(0, rf.lastApplied)
		for ; cur <= rf.commitIndex; cur++ {
			DPrintf("server:%v apply cmd:%v,logs:%v,leaderCommit:%v", rf.me, rf.logs[cur], rf.logs, leaderCommit)
			rf.applyMsg <- ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[cur].Cmd,
				CommandIndex: rf.logs[cur].OuterIndex(),
			}
		}
		DPrintf("server:%v => %v to apply from:%v to:%v,current len:%v!\n", rf.me, msg, rf.lastApplied, cur-1, len(rf.logs))
		rf.lastApplied = cur - 1
	}
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

func (rf *Raft) convertToRole(role int32) bool {
	return rf.convertToRoleWithExpect(rf.role, role)
}

func (rf *Raft) convertToRoleWithExpect(expect int32, role int32) bool {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	//if rf.role == expect {
	//	rf.role = role
	//	return true
	//} else {
	//	return false
	//}
	return atomic.CompareAndSwapInt32(&rf.role, expect, role)
}

func (rf *Raft) increaseTermWithExpect(expect int32, increment int32) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm == expect {
		rf.currentTerm = expect + increment
		return true
	} else {
		return false
	}
	//return atomic.CompareAndSwapInt32(&rf.currentTerm, expect, expect+increment)
}

func (rf *Raft) tryElection(expectTerm int32) {
	//start := time.Now().Unix()
	//if candidate
	for i := 1; atomic.LoadInt32(&rf.role) == 2; i++ {
		//do send election
		if ok := rf.increaseTermWithExpect(expectTerm, 1); !ok {
			return
		}
		rf.lastHeartbeat = time.Now().UnixNano()
		beforeTerm := expectTerm + 1
		DPrintf("server:%v term:%v lastIndexTerm:%v lastLogIndex:%v logs:%v to try election...\n", rf.me, beforeTerm, rf.getLastLogTerm(), rf.getLastLogIndex(), rf.logs)
		voted := rf.voteForGranted(beforeTerm)
		rf.mu.Lock()
		rf.votedFor = rf.me
		afterTerm, afterRole := rf.currentTerm, rf.role
		if beforeTerm == afterTerm && afterRole == 2 && voted >= len(rf.peers)/2+1 {
			if ok := rf.convertToRoleWithExpect(CANDIDATE, LEADER); !ok {
				return
			} else {
				for j := 0; j < len(rf.nextIndex); j++ { //initialize of leader
					rf.nextIndex[j] = rf.getLastLogIndex() + 1
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
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	reply := &RequestVoteReply{}
	DPrintf("server:%v,request vote to:%v,term:%v,lastLogTerm:%v,lastLogIndex:%v\n", rf.me, server, term, rf.getLastLogTerm(), rf.getLastLogIndex())
	rpc := make(chan bool)
	go func(res chan bool) {
		res <- rf.sendRequestVote(server, args, reply)
	}(rpc)
	select {
	case ok := <-rpc:
		DPrintf("server:%v,request vote to:%v,term:%v,lastLogTerm:%v,lastLogIndex:%v,reply:%v\n", rf.me, server, term, rf.getLastLogTerm(), rf.getLastLogIndex(), reply)
		if ok {
			if reply.VoteGranted {
				ch <- true
			} else {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.role = FOLLOWER
					rf.lastHeartbeat = time.Now().UnixNano()
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

func (rf *Raft) getLastLogIndex() int {
	if len(rf.logs) <= 0 {
		return -1
	} else {
		return rf.logs[len(rf.logs)-1].Index
	}
}
func (rf *Raft) getLastLogTerm() int32 {
	if len(rf.logs) <= 0 {
		return -1
	} else {
		return rf.logs[len(rf.logs)-1].Term
	}
}
func (rf *Raft) heartbeat() {
	for {
		if rf.killed() {
			return
		}
		if role, _, _ := rf.getRoleTermAndLastTick(); role == LEADER {
			res := make(chan bool, len(rf.peers)-1) //channel for followers
			for i, _ := range rf.peers {
				if i == rf.me {
					//DPrintf("--------------->skip append entry to self!--------------->\n")
					continue
				}
				//DPrintf("------>leader:%v to:%v hearbeat------>", rf.me, i)
				go rf.tryHeartbeatToOne(i, res)
			}
			time.Sleep(time.Duration(rf.heartbeatDelay)) //heart beat in 200ms
		} else {
			time.Sleep(time.Duration(rf.heartbeatDelay)) //heart beat in 200ms
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
			DPrintf("--------------->server:%v,term:%v,isleader:%v,receive command--------------->\n", rf.me, rf.currentTerm, isLeader)
			entry := rf.tryAppendSelf(command)
			DPrintf("leader:%v,append to self index:%v,cmd:%v\n", rf.me, entry.Index, entry.Cmd)
			rf.tryNotifyOthers()
			DPrintf("************************server:%v,term:%v,complete: try commitIndex:%v logs:%v************************\n", rf.me, rf.currentTerm, rf.commitIndex, len(rf.logs))
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
	index := rf.getLastLogIndex() + 1
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
func (rf *Raft) tryNotifyOthers() {
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
	DPrintf(">>append remote append all:%v succeed:%v!>>\n", len(rf.peers)-1, succeed)
	//majority commitIndex
	if rf.role == LEADER {
		rf.tryLeaderApply(false)
	}
}
func (rf *Raft) tryHeartbeatToOne(server int, res chan bool) {
	rf.mu.Lock()
	idx := rf.nextIndex[server]
	var logTerm int32 = -1
	if idx >= 1 && idx-1 < len(rf.logs) {
		logTerm = rf.logs[idx-1].Term
	}
	prevLogIndex := idx - 1
	start := min(len(rf.logs), max(idx, 0))
	end := min(start+rf.maxSendEntrySize, len(rf.logs))
	req := AppendEntryReq{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  logTerm,
		Entries:      rf.logs[start:end],
		LeaderCommit: rf.commitIndex,
		Msg:          "heartbeat",
	}
	reply := AppendEntryReply{}
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", &req, &reply)
	rf.mu.Lock()
	if reply.Success { //update next/commit index
		rf.matchIndex[server] = prevLogIndex + len(req.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else {
		rf.nextIndex[server] = max(0, reply.PreIndexOfLastTerm)
	}
	rf.mu.Unlock()
	if rf.role == LEADER {
		rf.tryLeaderApply(true)
	}
	res <- ok && reply.Success
}

//tryAppendToOne
func (rf *Raft) tryAppendToOne(server int, res chan bool) {
	t0 := time.Now()
	for time.Since(t0) < time.Duration(rf.tryAppendEntryTimeout) {
		if rf.role != LEADER {
			DPrintf("server:%v not leader skip append to others!", rf.me)
			break
		}
		rf.mu.Lock()
		idx := rf.nextIndex[server]
		var logTerm int32 = -1
		if idx >= 1 && idx-1 < len(rf.logs) {
			logTerm = rf.logs[idx-1].Term
		}
		prevLogIndex := idx - 1
		start := min(len(rf.logs), max(idx, 0))
		end := min(start+rf.maxSendEntrySize, len(rf.logs))
		req := AppendEntryReq{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  logTerm,
			Entries:      rf.logs[start:end],
			LeaderCommit: rf.commitIndex,
			Msg:          "append",
		}
		reply := AppendEntryReply{}
		rf.mu.Unlock()
		DPrintf("leader:%v,send to:%v,req term:%v prevIndex:%v prevTerm:%v size:%v\n", rf.me, server, req.Term, req.PrevLogIndex, req.PrevLogTerm, len(req.Entries))
		rpcCall := make(chan bool)
		go func(res chan bool) {
			res <- rf.peers[server].Call("Raft.AppendEntries", &req, &reply)
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
				rf.convertToRole(FOLLOWER)
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
		//time.Sleep(100 * time.Millisecond) //sleep 200ms
	}
	res <- false
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
		if majority > len(rf.peers)/2 && rf.logs[i].Term == rf.currentTerm {
			rf.commitIndex = i
		}
	}
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied += 1
		entry := rf.logs[rf.lastApplied]
		DPrintf("leader:%v apply cmd:%v,logs:%v,lastApplied:%v!", rf.me, entry, rf.logs, rf.lastApplied)
		rf.applyMsg <- ApplyMsg{
			CommandValid: true,
			Command:      entry.Cmd,
			CommandIndex: entry.OuterIndex(),
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
		role, term, t := rf.getRoleTermAndLastTick()
		if role != 1 && time.Now().UnixNano()-t > rf.heartbeatTimeout {
			//convert to candidate=2
			//DPrintf("server:%v,receive leader message timeout:%vms,try election...\n", rf.me, rf.heartbeatTimeout/1000)
			//DPrintf("follower:%v convert to candidate with timeout:%v\n", rf.me, time.Now().UnixNano()-t)
			if ok := rf.convertToRoleWithExpect(role, 2); !ok { // convert failed continue
				continue
			}
			//do election
			rf.tryElection(term)
		}

	}
}

func (rf *Raft) getRoleTermAndLastTick() (int32, int32, int64) {
	rf.mu.Lock()
	role, t, term := rf.role, rf.lastHeartbeat, rf.currentTerm
	rf.mu.Unlock()
	return role, term, t
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
	rf.callRequestVoteTimeout = 100_000_000 //call request vote timeout 200ms
	rf.tryAppendEntryTimeout = 30_000_000   //1000ms
	rf.callAppendEntryTimeout = 20_000_000  //1000ms
	rf.maxSendEntrySize = 100               //max send 100 entry one rpc
	rf.votedFor = -1
	rf.commitIndex = -1 //set to -1
	rf.lastApplied = -1 //set to -1
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyMsg = applyCh

	DPrintf("server:%v,set callRequestVoteTimeout:%vms,heartbeatTimeout:%vms,heartbeatDelay:%vms,tickDelay:%vms\n", rf.me, rf.callRequestVoteTimeout/1000_000, rf.heartbeatTimeout/1000_000, rf.heartbeatDelay/1000_000, rf.tickDelay/1000_000)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeat()

	return rf
}
