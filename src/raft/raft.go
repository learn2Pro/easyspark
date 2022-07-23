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
	"fmt"
	"math/rand"
	"sort"

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

//
// A Go object implementing a single Raft peer.
//

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
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotRequest, reply *InstallSnapshotResponse) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesRequest, reply *AppendEntriesResponse) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

const None int = -1

type NodeRole uint8

const (
	Follower NodeRole = iota
	Candidate
	Leader
)

func RoleToString(role NodeRole) string {
	switch role {
	case Candidate:
		return "Candidate"
	case Follower:
		return "Follower"
	case Leader:
		return "Leader"
	}
	return "unknown"
}

type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh        chan ApplyMsg // apply 协程通道，协程模型中会讲到
	applyCond      *sync.Cond    // apply 流程控制的信号量
	replicatorCond []*sync.Cond  // 复制操作控制的信号量
	role           NodeRole      // 节点当前的状态
	curTerm        int64         // 当前的任期
	votedFor       int           // 为谁投票
	grantedVotes   int           // 已经获得的票数
	logs           *RaftLog      // 日志信息
	commitIdx      uint64        // 已经提交的最大的日志 id
	lastApplied    uint64        // 已经 apply 的最大日志的 id
	nextIdx        []uint64      // 到其他节点下一个匹配的日志 id 信息
	matchIdx       []uint64      // 到其他节点当前匹配的日志 id 信息
	isSnapshoting  bool          //是否快照

	leaderId         int         // 集群中当前 Leader 节点的 id
	electionTimer    *time.Timer // 选举超时定时器
	heartbeatTimer   *time.Timer // 心跳超时定时器
	heartBeatTimeout uint64      // 心跳超时时间
	baseElecTimeout  uint64      // 选举超时时间
}

func (raft *Raft) advanceCommitIndexForLeader() {
	cloned := make([]uint64, len(raft.matchIdx))
	copy(raft.matchIdx, cloned)
	sort.Slice(cloned, func(i, j int) bool { return cloned[i] < cloned[j] })
	n := len(cloned)
	newCommitIndex := cloned[n/2]
	if newCommitIndex > raft.commitIdx {
		if raft.MatchLog(raft.curTerm, newCommitIndex) {
			DPrintf("peer %d advance commit index %d at term %d", raft.me, raft.commitIdx, raft.curTerm)
			raft.commitIdx = newCommitIndex
			raft.applyCond.Signal()
		}
	}
}

func (raft *Raft) MatchLog(term int64, index uint64) bool {
	return index <= raft.logs.GetLast().Index &&
		raft.logs.GetEntry(int64(index-raft.logs.GetFirst().Index)).Term == term
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//

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
}

func (rf *Raft) readSnapshot(data []byte) {
	//read snapshot
}

func (rf *Raft) ReadRaftState() (int64, int) {
	data := rf.persister.ReadRaftState()
	reader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(reader)
	var Term int64
	var VotedFor int
	if decoder.Decode(&Term) != nil ||
		decoder.Decode(&VotedFor) != nil {
		DPrintf("%v (%v) read persist error", rf.me, rf.role)
		return Term, VotedFor
	} else {
		return Term, VotedFor
	}
}
func (rf *Raft) PersistRaftState() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.curTerm)
	e.Encode(rf.votedFor)
	rf.persister.SaveRaftState(w.Bytes())
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int64, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index uint64, snapshot []byte) {
	// Your code here (2D).
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotRequest, reply *InstallSnapshotResponse) {
}

func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesResponse) {
	//DPrintf("server:%v,receive append args:%v,rf.currentTerm:%v\n", rf.me, args, rf.currentTerm)
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
	return 0, 0, false
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

// The Ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (raft *Raft) Ticker() {
	for !raft.killed() {
		select {
		case <-raft.electionTimer.C:
			{
				raft.mu.Lock()
				raft.ChangeRole(Candidate)
				raft.curTerm += 1
				raft.StartNewElection()
				raft.electionTimer.Reset(time.Millisecond * time.Duration(MakeAnRandomElectionTimeout(int(raft.baseElecTimeout))))
				raft.mu.Unlock()
			}
		case <-raft.heartbeatTimer.C:
			{
				if raft.role == Leader {
					raft.BroadcastHeartbeat()
					raft.heartbeatTimer.Reset(time.Millisecond * time.Duration(raft.heartBeatTimeout))
				}
			}
		}
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (raft *Raft) GetState() (int, bool) {
	//var term int
	//var isleader bool
	// Your code here (2A).
	//DPrintf("server:%v term:%v role:%v\n", rf.me, rf.currentTerm, rf.role)
	raft.mu.RLock()
	defer raft.mu.RUnlock()
	return int(raft.curTerm), raft.role == Leader
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
func (raft *Raft) ReInitLog() {
	raft.logs.ReInitLogs()
}
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {

	var hearttime uint64 = 100    //100ms
	var electiontime uint64 = 200 //200ms

	//var dataDir = flag.String("data_path", "./raft/data", "input meta server data path")
	suffix := rand.Intn(10000)
	logDbEng := KvStoreFactory("leveldb", fmt.Sprintf("%v_%v/log_%v", "./leveldb/data", suffix, me))

	newraft := &Raft{
		peers:            peers,
		me:               me,
		dead:             0,
		applyCh:          applyCh,
		replicatorCond:   make([]*sync.Cond, len(peers)),
		role:             Follower,
		curTerm:          0,
		votedFor:         None,
		grantedVotes:     0,
		isSnapshoting:    false,
		logs:             MakePersistRaftLog(logDbEng),
		persister:        persister,
		commitIdx:        0,
		lastApplied:      0,
		nextIdx:          make([]uint64, len(peers)),
		matchIdx:         make([]uint64, len(peers)),
		electionTimer:    time.NewTimer(time.Millisecond * time.Duration(MakeAnRandomElectionTimeout(int(electiontime)))),
		heartbeatTimer:   time.NewTimer(time.Millisecond * time.Duration(hearttime)),
		baseElecTimeout:  electiontime,
		heartBeatTimeout: hearttime,
	}

	newraft.curTerm, newraft.votedFor = newraft.ReadRaftState()
	newraft.ReInitLog()
	newraft.applyCond = sync.NewCond(&newraft.mu)
	lastLog := newraft.logs.GetLast()
	for id, peer := range peers {
		DPrintf("peer addr:%v   id:%v ", peer, id)
		newraft.matchIdx[id], newraft.nextIdx[id] = 0, lastLog.Index+1
		if id != me {
			newraft.replicatorCond[id] = sync.NewCond(&sync.Mutex{})
			go newraft.Replicator(id, peer)
		}
	}

	go newraft.Ticker()
	go newraft.Applier()

	return newraft
}
