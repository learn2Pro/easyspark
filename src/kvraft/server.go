package kvraft

import (
	"easyspark/labgob"
	"easyspark/labrpc"
	"easyspark/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OpType int

const (
	GET                   OpType = 1
	PUT                   OpType = 2
	APPEND                OpType = 3
	NOT_LEADER_ERR               = "not leader!"
	GET_TIMEOUT                  = "get timeout!"
	PUT_OR_APPEND_TIMEOUT        = "put/append timeout!"
	EMPTY                        = ""
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpId  int64 //unique id of operation
	Typo  OpType
	Key   string
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvData           map[string]string
	onComplete       map[int64]string
	waitApplyTimeout int64 //wait apply of command
	waitChanTimeout  int64 //wait channel of command
}

func (kv *KVServer) GetById(uuid int64) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	v, ok := kv.onComplete[uuid]
	return v, ok
}
func (kv *KVServer) ApplyByRaft() {
	for {
		select {
		case m, ok := <-kv.applyCh:
			DPrintf("server:%v receive cmd:%v ok:%v!", kv.me, m, ok)
			if m.CommandValid {
				op := m.Command.(Op)
				if _, ok := kv.GetById(op.OpId); ok { //already executed
					return
				}
				switch op.Typo {
				case GET:
					//A Get for a non-existent key should return an empty string.
					kv.mu.Lock()
					v, ok := kv.kvData[op.Key]
					if ok {
						kv.onComplete[op.OpId] = v
					} else {
						kv.onComplete[op.OpId] = EMPTY
					}
					kv.mu.Unlock()
				case PUT:
					kv.mu.Lock()
					kv.kvData[op.Key] = op.Value
					kv.onComplete[op.OpId] = EMPTY
					kv.mu.Unlock()
				case APPEND:
					kv.mu.Lock()
					v, ok := kv.kvData[op.Key]
					if ok {
						kv.kvData[op.Key] = v + op.Value
					} else {
						kv.kvData[op.Key] = op.Value
					}
					kv.onComplete[op.OpId] = EMPTY
					kv.mu.Unlock()
				}
			} else {
				DPrintf("server:%v skip cmd:%v\n", kv.me, m)
			}
		case <-time.After(time.Duration(kv.waitChanTimeout)):
			DPrintf("server:%v receive cmd timeout:%vms\n", kv.me, kv.waitChanTimeout)
			if kv.killed() {
				return
			}
		}
	}
}
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if _, _, isLeader := kv.rf.Start(args.Operation); !isLeader {
		reply.Success = false
		reply.Err = NOT_LEADER_ERR
		return
	} else {
		t0 := time.Now()
		for time.Since(t0) < time.Duration(kv.waitApplyTimeout) {
			if _, ok := kv.GetById(args.Operation.OpId); ok {
				reply.Value = kv.onComplete[args.Operation.OpId]
				reply.Success = true
				return
			}
		}
		reply.Success = false
		reply.Err = GET_TIMEOUT
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if _, _, isLeader := kv.rf.Start(args.Operation); !isLeader {
		reply.Success = false
		reply.Err = NOT_LEADER_ERR
		return
	} else {
		t0 := time.Now()
		for time.Since(t0) < time.Duration(kv.waitApplyTimeout) {
			if _, ok := kv.GetById(args.Operation.OpId); ok {
				reply.Success = true
				return
			}
		}
		reply.Success = false
		reply.Err = PUT_OR_APPEND_TIMEOUT
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	close(kv.applyCh)
	// Your code here, if desired.
	DPrintf("server:%v killed!", kv.me)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/Value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvData = make(map[string]string)
	kv.onComplete = make(map[int64]string)
	//parameter in timeout
	kv.waitApplyTimeout = 300_000_000 //10ms
	kv.waitChanTimeout = 100_000_000  //100ms

	go kv.ApplyByRaft() //do apply

	return kv
}
