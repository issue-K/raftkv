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
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type WorkerTask int

const (
	MAPTASK WorkerTask = iota
	REDUCETASK
	EXITTASK
)

type AskTaskArgs struct {
}

// AskTaskReply files表示任务的输入文件
type AskTaskReply struct {
	Task   WorkerTask
	TaskID int
	// 如果是map任务, 需要分为nReduce个中间文件
	NReduce int
	Files   []string
}

// FinTaskArgs files表示任务的输出文件
type FinTaskArgs struct {
	Task   WorkerTask
	TaskID int
	Files  []string
}

type FinTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
