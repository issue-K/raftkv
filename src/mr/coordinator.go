package mr

import (
	"6.5840/raft"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// 输入数据
	inputFiles []string
	// 经map处理后的中间数据. midFiles[i][j]表示第i个map任务产生的第j个中间文件
	midFiles [][]string
	// 经reduce处理后的输出数据
	outputFiles []string
	// 全局锁
	mu sync.Mutex
	// map任务数量.(也即inputFiles的长度)
	nMap int
	// reduce任务数量.(也即map任务应该生成的中间文件个数)
	nReduce int
	// 每个任务的状态. 0未分配, 1正在执行, 2已完成
	taskMap map[int]int
	// 已完成的map数量
	mapNum int
	// 已完成的reduce数量
	reduceNum int
	// 所有任务是否都已完成
	done bool
}

// Your code here -- RPC handlers for the worker to call.

// Example an example RPC handler.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 9
	return nil
}

func assert(x bool) {
	if !x {
		panic("assert失败")
	}
}

// GetTaskID 分配一个没有分配的任务
func (c *Coordinator) AllocTaskID(task WorkerTask) int {
	if task == MAPTASK {
		for i := 0; i < c.nMap; i++ {
			if c.taskMap[i] == 0 {
				c.taskMap[i] = 1
				return i
			}
		}
		return -1
	}
	if task == REDUCETASK {
		end := c.nMap + c.nReduce
		for i := c.nMap; i < end; i++ {
			if c.taskMap[i] == 0 {
				c.taskMap[i] = 1
				return i - c.nMap
			}
		}
		return -1
	}
	panic("GetTaskID: 不存在的分支")
}

// CheckTaskFin
// 检查任务是否在规定的时间内完成. 如果没有, 把该任务标记为未分配, 这样后续可以交付给其余worker执行
func (c *Coordinator) CheckTaskFin(task WorkerTask, taskID int) {
	raft.LOG("开始睡觉... task = %v, taskID = %v\n", task, taskID)
	time.Sleep(time.Second * 10)
	raft.LOG("睡醒了... task = %v, taskID = %v\n", task, taskID)
	c.mu.Lock()
	switch task {
	case MAPTASK:
		if c.taskMap[taskID] != 2 {
			c.taskMap[taskID] = 0
		}
	case REDUCETASK:
		if c.taskMap[c.nMap+taskID] != 2 {
			c.taskMap[c.nMap+taskID] = 0
		}
	case EXITTASK:
		// 忽略...
	}
	c.mu.Unlock()
}

// AskTask
// 分配一个任务给worker. 任务如果在10s内没有得到相应, 应把该任务交给另一个worker执行
func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mapNum < len(c.inputFiles) {
		// 分配一个map任务
		reply.TaskID = c.AllocTaskID(MAPTASK)
		if reply.TaskID == -1 {
			return nil
		}
		reply.Task = MAPTASK
		reply.NReduce = c.nReduce
		reply.Files = []string{c.inputFiles[reply.TaskID]}
		go c.CheckTaskFin(MAPTASK, reply.TaskID)
	} else if c.reduceNum < c.nReduce {
		// 分配一个reduce任务, 并修改c.taskMp的状态
		reply.TaskID = c.AllocTaskID(REDUCETASK)
		if reply.TaskID == -1 {
			return nil
		}
		reply.Task = REDUCETASK
		reply.NReduce = c.nReduce
		reply.Files = make([]string, c.nMap)
		for i := 0; i < c.nMap; i++ {
			reply.Files = append(reply.Files, c.midFiles[i][reply.TaskID])
		}
		go c.CheckTaskFin(REDUCETASK, reply.TaskID)
	} else {
		// 分配一个exit任务
		reply.Task = EXITTASK
	}
	return nil
}

func (c *Coordinator) FinTask(args *FinTaskArgs, reply *FinTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.taskMap[args.TaskID] == 2 {
		return nil
	}
	switch args.Task {
	case MAPTASK:
		raft.LOG("[FinTask] MAPTASK完成, args = %+v, len = %+v\n", args, len(args.Files))
		assert(len(args.Files) == c.nReduce)
		c.mapNum++
		c.midFiles[args.TaskID] = args.Files
		c.taskMap[args.TaskID] = 2
	case REDUCETASK:
		assert(len(args.Files) == 1)
		c.reduceNum++
		c.outputFiles = append(c.outputFiles, args.Files...)
		c.taskMap[args.TaskID+c.nMap] = 2
		// 完成了所有的任务
		if c.reduceNum == c.nReduce {
			c.done = true
		}
	case EXITTASK:
		// 忽略掉...
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	_ = rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	_ = os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		inputFiles:  files,
		midFiles:    make([][]string, len(files)),
		outputFiles: make([]string, nReduce),
		taskMap:     make(map[int]int),
		nMap:        len(files),
		nReduce:     nReduce,
		mapNum:      0,
		reduceNum:   0,
		done:        false,
	}
	raft.LOG("[MakeCoordinator] %+v\n", c)

	// Your code here.

	c.server()
	return &c
}
