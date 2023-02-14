package mr

import (
	"6.5840/raft"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type KV []KeyValue

// for sorting by key.
func (a KV) Len() int           { return len(a) }
func (a KV) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a KV) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type ByKey []KeyValue

func MapTask(mapf func(string, string) []KeyValue, taskID int, inputFiles []string, nReduce int) []string {
	// 1、对每个输入文件进行读取, 通过map处理得到中间的[]KeyValue
	var intermediate []KeyValue
	for _, filename := range inputFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		_ = file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}
	midFileContent := make([][]KeyValue, nReduce)
	for _, kv := range intermediate {
		index := ihash(kv.Key) % nReduce
		midFileContent[index] = append(midFileContent[index], kv)
	}
	midFiles := make([]string, nReduce)
	for i := 0; i < nReduce; i++ {
		filename := fmt.Sprintf("mr-%v-%v", taskID, i)
		file, _ := os.CreateTemp("", filename)
		enc := json.NewEncoder(file)

		for _, kv := range midFileContent[i] {
			_ = enc.Encode(&kv)
		}
		_ = os.Rename(file.Name(), filename)
		_ = file.Close()
		midFiles[i] = filename
	}
	return midFiles
}

func ReduceTask(reducef func(string, []string) string, taskID int, midFiles []string) []string {
	var kva []KeyValue
	for _, filename := range midFiles {
		file, _ := os.Open(filename)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		_ = file.Close()
	}
	sort.Sort(KV(kva))

	filename := fmt.Sprintf("mr-out-%v", taskID)
	file, _ := os.CreateTemp("", filename)
	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		_, _ = fmt.Fprintf(file, "%v %v\n", kva[i].Key, output)
		i = j
	}
	_ = os.Rename(file.Name(), filename)
	_ = file.Close()
	return []string{filename}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		askTaskArgs := &AskTaskArgs{}
		askTaskReply := &AskTaskReply{}
		raft.LOG("[Worker] 开始请求一个任务....")
		_ = call("Coordinator.AskTask", askTaskArgs, askTaskReply)
		if askTaskReply.TaskID == -1 {
			time.Sleep(time.Second / 2)
			continue
		}
		switch askTaskReply.Task {
		case MAPTASK:
			raft.LOG("[Worker] 收到一个map任务, %+v\n", askTaskReply)
			midFiles := MapTask(mapf, askTaskReply.TaskID, askTaskReply.Files, askTaskReply.NReduce)
			finTaskArgs := &FinTaskArgs{
				MAPTASK, askTaskReply.TaskID, midFiles,
			}
			finTaskReply := &FinTaskReply{}
			raft.LOG("[Worker] 通知完成一个map任务 %+v\n", finTaskArgs)
			_ = call("Coordinator.FinTask", finTaskArgs, finTaskReply)
		case REDUCETASK:
			raft.LOG("[Worker] 收到一个reduce任务, %+v\n", askTaskReply)
			outputFiles := ReduceTask(reducef, askTaskReply.TaskID, askTaskReply.Files)
			finTaskArgs := &FinTaskArgs{
				REDUCETASK, askTaskReply.TaskID, outputFiles,
			}
			finTaskReply := &FinTaskReply{}
			raft.LOG("[Worker] 通知完成一个reduce任务, %+v\n", askTaskReply)
			_ = call("Coordinator.FinTask", finTaskArgs, finTaskReply)
		case EXITTASK:
			raft.LOG("[Worker] 收到一个exit任务, 正在退出...\n")
			return
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 990

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
