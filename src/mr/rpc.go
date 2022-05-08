package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	//sequence id
	X int
	//1=request a ask
	//2=complete signal of mapper
	//3=complete signal of reducer
	RequestType int
	TaskNo      int
}

type ExampleReply struct {
	Y int
	//1=map task
	//2=reduce task
	TaskType int
	FilePath string
	NReducer int
	NMapper  int
	TaskNo   int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
