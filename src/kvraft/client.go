package kvraft

import "easyspark/labrpc"
import "crypto/rand"
import "math/big"

//You will probably have to modify your Clerk to remember which server turned out to be the leader for the last RPC,
//and send the next RPC to that server first. This will avoid wasting time searching for the leader on every RPC,
//which may help you pass some of the tests quickly enough.
type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastLeader = -1
	return ck
}

//
// fetch the current Value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	uuid := nrand()
	return ck.GetWithId(key, uuid, ck.lastLeader)
}

func (ck *Clerk) GetWithId(key string, uuid int64, server int) string {
	which := Max(0, server)
	op := Op{OpId: uuid, Typo: GET, Key: key}
	args := &GetArgs{Operation: op}
	reply := &GetReply{}
	ok := ck.CallRemoteGet(which, args, reply)
	if ok {
		if reply.Success {
			ck.lastLeader = which
			DPrintf("request:%v uuid:%v key:%v value:%v completed!\n", which, uuid, key, reply.Value)
			return reply.Value
		} else {
			DPrintf("request:%v uuid:%v key:%v err:%v\n", which, uuid, key, reply.Err)
			if reply.Err == NOT_LEADER_ERR {
				return ck.GetWithId(key, uuid, ck.NextToTry(which))
			} else {
				return ck.GetWithId(key, uuid, which)
			}
		}
	} else {
		return ck.GetWithId(key, uuid, which)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.PutAppendWithId(key, value, op, nrand(), ck.lastLeader)
}
func (ck *Clerk) PutAppendWithId(key string, value string, op string, uuid int64, server int) {
	// You will have to modify this function.
	which := Max(0, server)
	var operation Op
	switch op {
	case "Put":
		operation = Op{OpId: uuid, Typo: PUT, Key: key, Value: value}
	case "Append":
		operation = Op{OpId: uuid, Typo: APPEND, Key: key, Value: value}
	}
	args := &PutAppendArgs{Operation: operation}
	reply := &PutAppendReply{}
	ok := ck.CallRemotePutOrAppend(which, args, reply)
	if ok {
		if reply.Success {
			ck.lastLeader = which
			DPrintf("request:%v uuid:%v key:%v value:%v completed!\n", which, uuid, key, value)
		} else {
			DPrintf("request:%v uuid:%v key:%v value:%v err:%v\n", which, uuid, key, value, reply.Err)
			if reply.Err == NOT_LEADER_ERR {
				ck.PutAppendWithId(key, value, op, uuid, ck.NextToTry(which))
			} else {
				ck.PutAppendWithId(key, value, op, uuid, which)
			}
		}
	} else {
		ck.PutAppendWithId(key, value, op, uuid, which)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) CallRemoteGet(server int, args *GetArgs, reply *GetReply) bool {
	ok := ck.servers[server].Call("KVServer.Get", args, reply)
	return ok
}
func (ck *Clerk) CallRemotePutOrAppend(server int, args *PutAppendArgs, reply *PutAppendReply) bool {
	ok := ck.servers[server].Call("KVServer.PutAppend", args, reply)
	return ok
}
func Max(a, b int) int {
	if a < b {
		return b
	}
	return a
}
func (ck *Clerk) NextToTry(server int) int {
	return (server + 1) % len(ck.servers)
}
