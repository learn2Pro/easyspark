package mr

import (
	"errors"
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	nMapper   int
	nReducer  int
	filePaths []string
	//0=create 1=running 2=completed
	nMapperTask []int
	//0=create 1=running 2=completed
	nReducerTask []int
	nReducerDone int
	nMapperDone  int
	m            sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	fmt.Printf("before nMapperTask:%v,NReducerTask:%v\n", c.nMapperTask, c.nReducerTask)
	switch args.RequestType {
	case 1:
		if c.nMapperDone != c.nMapper {
			taskNo, err := c.chooseTask(c.nMapperTask)
			fmt.Printf("choose mapper task of no:%v \n", taskNo)
			if err != nil {
				reply.Y = args.X
			} else {
				reply.TaskType = 1
				reply.FilePath = c.filePaths[taskNo]
				reply.NReducer = c.nReducer
				reply.NMapper = c.nMapper
				reply.TaskNo = taskNo
			}
		} else if c.nReducerDone != c.nReducer {
			taskNo, err := c.chooseTask(c.nReducerTask)
			fmt.Printf("choose reducer task of no:%v \n", taskNo)
			if err != nil {
				reply.Y = args.X
			} else {
				reply.TaskType = 2
				reply.NReducer = c.nReducer
				reply.NMapper = c.nMapper
				reply.TaskNo = taskNo
			}
		} else {
			reply.Y = args.X
		}
	case 2:
		c.markMapperDone(args.TaskNo)
	case 3:
		c.markReducerDone(args.TaskNo)
	}
	fmt.Printf("after nMapperTask:%v,NReducerTask:%v\n", c.nMapperTask, c.nReducerTask)
	fmt.Printf("after input args:%v reply:%v\n\n", args, reply)
	return nil
}

func (c *Coordinator) markMapperDone(taskNo int) {
	c.m.Lock()
	defer c.m.Unlock()
	c.nMapperDone += 1
	c.nMapperTask[taskNo] = 2
}

func (c *Coordinator) markReducerDone(taskNo int) {
	c.m.Lock()
	defer c.m.Unlock()
	c.nReducerDone += 1
	c.nReducerTask[taskNo] = 2
}

func (c *Coordinator) chooseTask(arr []int) (int, error) {
	c.m.Lock()
	defer c.m.Unlock()
	//pop creat task
	for i, item := range arr {
		if item == 0 {
			arr[i] = 1
			return i, nil
		}
	}
	//pop long-time unfinished task
	for i, item := range arr {
		if item == 1 {
			arr[i] = 1
			return i, nil
		}
	}
	return -1, errors.New("not available slot!")
}

type ExampleHandler struct {
	c *Coordinator
}

//auto ser/deser no need handle
//func (e *ExampleHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
//	var args ExampleArgs
//	fmt.Printf("request body:%v\n", r.Body)
//	err := json.NewDecoder(r.Body).Decode(&args)
//	if err != nil {
//		http.Error(w, err.Error(), http.StatusBadRequest)
//		return
//	}
//	var reply ExampleReply
//	err = e.c.Example(&args, &reply)
//	if err != nil {
//		http.Error(w, err.Error(), http.StatusInternalServerError)
//		return
//	}
//	err = json.NewEncoder(w).Encode(reply)
//	if err != nil {
//		http.Error(w, err.Error(), http.StatusInternalServerError)
//		return
//	}
//}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	c.m.Lock()
	defer c.m.Unlock()
	ret := (c.nReducer == c.nReducerDone)
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.filePaths = files
	c.nMapper = len(files)
	c.nReducer = nReduce
	c.nMapperTask = make([]int, c.nMapper, c.nMapper)
	c.nReducerTask = make([]int, c.nReducer, c.nReducer)
	// Your code here.

	c.server()
	return &c
}
