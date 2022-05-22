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
	"fmt"
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
var FOLLOWER int32 = 3
var CANDIDATE int32 = 2
var LEADER int32 = 1

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role                  int32 //1=leader,2=candidate,3=follower
	electionTimeout       int64 // election timeout  in ns
	heartbeatDelay        int64 // election timeout  in ns
	heartbeatTimeout      int64 // election timeout  in ns
	lastHeartbeat         int64 // election timeout  in ns
	tickDelay             int64 // duration in ns
	tryAppendEntryTimeout int64 // duration in ns
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
}
type AppendEntryReply struct {
	Term    int32 //current term,for leader to update itself
	Success bool  //true if follower contained entry matching AppendEntryReq.PrevLogIndex and AppendEntryReq.PrevLogTerm
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
	//fmt.Printf("server:%v term:%v role:%v\n", rf.me, rf.currentTerm, rf.role)
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
	//fmt.Printf("receive arg term:%v current term:%v cid:%v\n", args.Term, rf.currentTerm, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > atomic.LoadInt32(&rf.currentTerm) {
		//Raft determines which of two logs is more up-to-date
		//by comparing the index and term of the last entries in the
		//logs. If the logs have last entries with different terms, then
		//the log with the later term is more up-to-date. If the logs
		//end with the same term, then whichever log is longer is
		//more up-to-date.
		// not voted || term<args.Term || term==args.Term&&index<=args.Index
		if rf.votedFor == -1 || rf.getLastLogTerm() < args.LastLogTerm || (rf.getLastLogTerm() == args.LastLogTerm && rf.getLastLogIndex() <= args.LastLogIndex) {
			reply.VoteGranted = true
			reply.Term = args.Term
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			rf.convertToRole(3) //converted to follower=3
			rf.lastHeartbeat = time.Now().UnixNano()
			fmt.Printf("server:%v current term:%v args term:%v vote:%v role:%v\n", rf.me, rf.currentTerm, args.Term, rf.votedFor, rf.role)
		} else {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
		}
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}
}

func (rf *Raft) AppendEntries(args *AppendEntryReq, reply *AppendEntryReply) {
	//heartbeat
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm { //not leader
		reply.Success = false
		reply.Term = args.Term
		return
	} else { //leader
		rf.lastHeartbeat = time.Now().UnixNano()
		if rf.role != 3 { //convert to follower
			rf.role = 3
		}

		if len(args.Entries) == 0 { //heartbeat
			return
		} else if args.PrevLogIndex < len(rf.logs) { // check valid
			//Reply false if log doesn’t contain an entry at prevLogIndex
			//whose term matches prevLogTerm (§5.3)
			if args.PrevLogIndex >= 0 && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm { //not accept start from here
				reply.Success = false
				reply.Term = args.Term
				return
			} else {
				fmt.Printf("follower:%v start to append entries:%v\n", rf.me, args.Entries)
				for _, entry := range args.Entries { //start append
					if entry.Index >= len(rf.logs) { //just append
						rf.logs = append(rf.logs, entry)
					} else { //delete origin and replace with current
						rf.logs[entry.Index] = entry
					}
					rf.applyMsg <- ApplyMsg{
						CommandValid: true,
						Command:      entry.Cmd,
						CommandIndex: entry.OuterIndex(),
					}
				}
				//If leaderCommit > commitIndex, set commitIndex =
				//min(leaderCommit, index of last new entry)
				rf.commitIndex = min(args.LeaderCommit, args.Entries[len(args.Entries)-1].Index)
				reply.Success = true
				reply.Term = rf.currentTerm
				fmt.Printf("follower:%v complete append,term:%v,commit:%v\n", rf.me, reply.Term, rf.commitIndex)
			}
		} else { //if leader previous>follower then ask for decrement
			reply.Success = false
			reply.Term = args.Term
			return
		}
		//fmt.Printf("leader:%v to %v send heartbeat term:%v lastHeartbeat:%v\n", args.LeaderId, rf.me, args.Term, rf.lastHeartbeat)
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
		beforeTerm := expectTerm + 1
		fmt.Printf("server:%v term:%v to try election...\n", rf.me, beforeTerm)
		voted := rf.voteForGranted(beforeTerm)
		rf.mu.Lock()
		rf.votedFor = rf.me
		afterTerm, afterRole := rf.currentTerm, rf.role
		if beforeTerm == afterTerm && afterRole == 2 && voted >= len(rf.peers)/2+1 {
			if ok := rf.convertToRoleWithExpect(2, 1); !ok {
				return
			} else {
				fmt.Printf("found leader server:%v term:%v role:%v\n", rf.me, afterTerm, rf.role)

			}
		}
		rf.mu.Unlock()

	}
}
func (rf *Raft) initializeOfLeader() {
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) voteForGranted(term int32) int {
	voted := 1
	if atomic.LoadInt32(&rf.role) == 2 {
		chs := make(chan bool, len(rf.peers))
		for i, _ := range rf.peers {
			if i == rf.me { //no need to self
				continue
			}
			go rf.trySendVote(i, term, rf.me, chs)
		}
		for range rf.peers {
			select {
			case ans := <-chs:
				if ans {
					voted += 1
				}
			case <-time.After(time.Duration(rf.electionTimeout)):
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
	//fmt.Printf("server:%v,rpc before send to:%v,term:%v,cid:%v\n", rf.me, server, term, cid)
	ok := rf.sendRequestVote(server, args, reply)
	//if reply.VoteGranted {
	//	fmt.Printf("server:%v,rpc after send to:%v,term:%v,cid:%v,reply term:%v,VoteGranted:%v\n", rf.me, server, term, cid, reply.Term, reply.VoteGranted)
	//}
	if ok {
		if reply.VoteGranted {
			ch <- true
		} else {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.role = 3
				rf.currentTerm = reply.Term
				ch <- false
			}
			rf.mu.Unlock()
		}
	}
	ch <- false
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
		if role, _, _ := rf.getRoleTermAndLastTick(); role == 1 {
			for i, _ := range rf.peers {
				if i == rf.me { //do not send to self
					continue
				}
				args := &AppendEntryReq{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					LeaderCommit: rf.commitIndex,
				}
				reply := &AppendEntryReply{}
				go rf.peers[i].Call("Raft.AppendEntries", args, reply)
			}
		}
		time.Sleep(time.Duration(rf.heartbeatDelay)) //heart beat in 200ms
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
	_, isLeader := rf.GetState()
	//fmt.Printf("server:%v,isleader:%v\n", rf.me, isLeader)
	// Your code here (2B).
	if isLeader {
		rf.mu.Lock()
		fmt.Printf("--------------->server:%v,isleader:%v,receive command:%v--------------->\n", rf.me, isLeader, command)
		index := rf.getLastLogIndex() + 1
		entry := LogEntry{
			Index: index,
			Term:  rf.currentTerm,
			Cmd:   command,
		}
		rf.logs = append(rf.logs, entry)
		rf.mu.Unlock()
		fmt.Printf("before: try commit index:%v entry:%v\n", index, entry)
		res := make(chan bool, len(rf.peers)-1) //channel for followers
		for i, _ := range rf.peers {
			if i == rf.me {
				fmt.Printf("--------------->skip append entry to self!--------------->\n")
				continue
			}
			go rf.tryAppend(i, res)
		}
		//wait for remote append
		rf.tryReceive(res)
		//update commitIndex
		rf.tryUpdateCommit(index)
		rf.applyMsg <- ApplyMsg{
			CommandValid: true,
			Command:      entry.Cmd,
			CommandIndex: entry.OuterIndex(),
		}
		fmt.Printf("complete: try commit index:%v entry:%v logs:%v\n", index, entry, rf.logs)
		return entry.OuterIndex(), int(entry.Term), isLeader
	} else {
		return -1, -1, isLeader
	}
}

func (rf *Raft) tryReceive(res chan bool) {
	succeed, checkNum := 0, len(rf.peers)-1
	for i := 0; i < checkNum; i++ {
		select {
		case ans := <-res:
			if ans {
				succeed++
			}
		}
	}
	fmt.Printf("---------------->remote append all:%v succeed:%v!---------------->\n", len(rf.peers)-1, succeed)
}

func (rf *Raft) tryUpdateCommit(index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//If there exists an N such that N > commitIndex, a majority
	//of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	//set commitIndex = N (§5.3, §5.4).
	if index > rf.commitIndex {
		for i := index; i > rf.commitIndex; i-- {
			majority := 1
			for _, followerCommit := range rf.matchIndex {
				if followerCommit >= i {
					majority += 1
				}
			}
			if majority > len(rf.peers)/2 && rf.logs[i].Term == rf.currentTerm { //
				rf.commitIndex = i
				return
			}
		}
	}
}
func (rf *Raft) tryAppend(server int, res chan bool) {
	t0 := time.Now()
	for time.Since(t0) < time.Duration(rf.tryAppendEntryTimeout*int64(time.Second)) {
		rf.mu.Lock()
		if rf.getLastLogIndex() < rf.nextIndex[server] {
			fmt.Printf("current last index:%v greater than follower:%v index:%v\n", rf.getLastLogIndex(), server, rf.nextIndex[server])
			rf.mu.Unlock()
			res <- false
			return
		}
		idx := rf.nextIndex[server]
		var logTerm int32 = -1
		if idx >= 1 && idx-1 < len(rf.logs) {
			logTerm = rf.logs[idx-1].Term
		}
		req := AppendEntryReq{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: idx - 1,
			PrevLogTerm:  logTerm,
			Entries:      rf.logs[max(idx, 0):],
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntryReply{}
		rf.mu.Unlock()
		fmt.Printf("server:%v,send to:%v,append req:%v\n", rf.me, server, req)
		ok := rf.peers[server].Call("Raft.AppendEntries", &req, &reply)
		if !ok {
			fmt.Printf("invoke remote AppendEntries failed!\n")
		}
		fmt.Printf("server:%v,send to:%v,append reply:%v\n", rf.me, server, reply)
		rf.mu.Lock()
		if reply.Success { //update next/commit index
			rf.nextIndex[server] = req.Entries[len(req.Entries)-1].Index + 1
			rf.matchIndex[server] = req.Entries[len(req.Entries)-1].Index
			rf.mu.Unlock()
			res <- true
			return
		} else if reply.Term > rf.currentTerm { //convert to follower
			rf.convertToRole(FOLLOWER)
			rf.mu.Unlock()
			res <- false
			return
		} else {
			rf.nextIndex[server]--
		}
		rf.mu.Unlock()
		time.Sleep(200 * time.Millisecond) //sleep 200ms
	}
	res <- false
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
		sleep := rf.tickDelay + int64(rand.Int31n(100)*1000*1000)
		//fmt.Printf("server:%v sleep:%vms role:%v lastHeartbeat:%v for ticker!\n", rf.me, sleep/1000, rf.role, rf.lastHeartbeat)
		time.Sleep(time.Duration(sleep)) //rand in [tickDelay,tickDelay+10)ms
		role, term, t := rf.getRoleTermAndLastTick()
		if role != 1 && time.Now().UnixNano()-t > rf.heartbeatTimeout {
			//convert to candidate=2
			//fmt.Printf("server:%v,receive leader message timeout:%vms,try election...\n", rf.me, rf.heartbeatTimeout/1000)
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
	rf.electionTimeout = 450_000_000  //>heartbeat timeout
	rf.heartbeatTimeout = 400_000_000 //heartbeatDelay*2=400ms
	rf.heartbeatDelay = 200_000_000   //heartbeatDelay=200ms
	rf.tickDelay = 200_000_000        //heartbeatDelay=200ms
	rf.tryAppendEntryTimeout = 3      //3s
	rf.votedFor = -1
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyMsg = applyCh

	fmt.Printf("server:%v,set electionTimeout:%vms,heartbeatTimeout:%vms,heartbeatDelay:%vms,tickDelay:%vms\n", rf.me, rf.electionTimeout/1000_000, rf.heartbeatTimeout/1000_000, rf.heartbeatDelay/1000_000, rf.tickDelay/1000_000)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeat()

	return rf
}
