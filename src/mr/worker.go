package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type RpcStatus bool
type SortKeyValue []KeyValue

// for sorting by key.
func (a SortKeyValue) Len() int           { return len(a) }
func (a SortKeyValue) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortKeyValue) Less(i, j int) bool { return a[i].Key < a[j].Key }

var WorkDir, _ = os.Getwd()
var TempFilePath = WorkDir + "/tmp"

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	sequenceNo := 1
	var status, reply = CallExample(&ExampleArgs{X: sequenceNo, RequestType: 1})
	for status {
		switch reply.TaskType {
		case 1:
			fmt.Printf("do mapper TaskNo:%v NMapper:%v NReducer:%v file:%v\n", reply.TaskNo, reply.NMapper, reply.NReducer, reply.FilePath)
			DoMap(mapf, reducef, reply.TaskNo, reply.NReducer, reply.FilePath)
			cycleSetStatus(&ExampleArgs{X: sequenceNo, RequestType: 2, TaskNo: reply.TaskNo}, 10)
		case 2:
			fmt.Printf("do reduce TaskNo:%v NMapper:%v NMapper:%v\n", reply.TaskNo, reply.NMapper, reply.NReducer)
			DoReduce(reducef, reply.NMapper, reply.TaskNo)
			cycleSetStatus(&ExampleArgs{X: sequenceNo, RequestType: 3, TaskNo: reply.TaskNo}, 10)
		}
		time.Sleep(1000 * 1000) //1ms
		sequenceNo++
		status, reply = CallExample(&ExampleArgs{X: sequenceNo, RequestType: 1})
	}
	fmt.Printf("Worker Done!\n")
}

func cycleSetStatus(arg *ExampleArgs, repeat int) {
	var status, _ = CallExample(arg)
	for i := 0; i < repeat && !status; {
		status, _ = CallExample(arg)
	}
	fmt.Printf("set status of task:%v status:%v\n", arg.TaskNo, status)
}

func DoMap(mapf func(string, string) []KeyValue, reducef func(string, []string) string, mapperNo int, nReducer int, filePath string) {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("cannot open %v", filePath)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("read file:%v failed", filePath)
	}
	kvs := mapf(filePath, string(content))
	DoSinkFile(mapperNo, nReducer, kvs, reducef)
}

func DoReduce(reducef func(string, []string) string, nMapper, reducerNo int) {
	ofile, _ := os.Create(fmt.Sprintf("mr-out-%v", reducerNo))
	defer ofile.Close()
	kvs := []KeyValue{}
	for i := 0; i < nMapper; i++ {
		name := fmt.Sprintf("%v/mr-%v-%v", TempFilePath, i, reducerNo)
		partial, _ := os.Open(name)
		contents, _ := ioutil.ReadAll(partial)
		fmt.Printf("open file:%v\n", name)
		ff := func(r rune) bool { return r == '\n' }

		// split contents into an array of words.
		lines := strings.FieldsFunc(string(contents), ff)
		for _, line := range lines {
			words := strings.Split(line, " ")
			kvs = append(kvs, KeyValue{Key: words[0], Value: words[1]})
		}
	}
	sort.Sort(SortKeyValue(kvs))
	DoCombine(kvs, ofile, reducef)
}

func DoSinkFile(mapperNo int, nReducer int, kvs []KeyValue, reducef func(string, []string) string) {
	indexOfKvs := make(map[int][]KeyValue)
	for _, item := range kvs {
		reducerNo := ihash(item.Key) % nReducer
		partial, ok := indexOfKvs[reducerNo]
		if !ok {
			partial = []KeyValue{item}
		} else {
			partial = append(partial, item)
		}
		indexOfKvs[reducerNo] = partial
	}
	for reducerNo, arr := range indexOfKvs {
		name := fmt.Sprintf("%v/mr-%v-%v", TempFilePath, mapperNo, reducerNo)
		if _, err := os.Stat(TempFilePath); os.IsNotExist(err) {
			os.MkdirAll(TempFilePath, 0755)
			fmt.Println("Directory created")
		}
		ofile, err := os.Create(name)
		if err != nil {
			log.Fatalf("Create Intermidiate File:%v failed!\nerr:%v", name, err)
		}
		sort.Sort(SortKeyValue(arr))
		//combine
		DoCombine(arr, ofile, reducef)
		//rename after complete
		//err = os.Rename(ofile.Name(), name)
		if err != nil {
			log.Fatalf("Rename File:%v To New:%v failed!", ofile.Name(), name)
		}
		err = ofile.Close()
		if err != nil {
			log.Fatalf("Close File:%v failed!", ofile.Name())
		}
	}
}
func DoCombine(arr []KeyValue, ofile *os.File, reducef func(string, []string) string) {
	for i := 0; i < len(arr); {
		j := i + 1
		for ; j < len(arr) && arr[j].Key == arr[i].Key; j++ {
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, arr[k].Value)
		}
		output := reducef(arr[i].Key, values)
		//write to file
		fmt.Fprintf(ofile, "%v %v\n", arr[i].Key, output)
		i = j
	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample(args *ExampleArgs) (RpcStatus, ExampleReply) {

	// declare an argument structure.
	// fill in the argument(s).
	// declare a reply structure.
	reply := ExampleReply{}
	// send the RPC request, wait for the reply.
	status := call("Coordinator.Example", args, &reply)
	// reply.Y should be 100.
	fmt.Printf("status:%v,reply.Y %v\n", status, reply.Y)
	return RpcStatus(status) && reply.Y == args.X+1, reply
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	err = c.Call(rpcname, args, reply)
	fmt.Printf("call rpc:%v args:%v reply:%v", rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
