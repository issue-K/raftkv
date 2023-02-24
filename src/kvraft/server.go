package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

const ExecuteTimeout = 500 * time.Millisecond

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Operator int

const (
	APPEND Operator = iota
	PUT
	GET
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  Operator
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
	kv         KVStore
	opChans    map[int]chan *OpReply
	lasReply   map[int64]*OpIdentity
	lasApplied int
}

// Get 成功与否, 应该告知clerk.
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

// GetChan
// 获取索引为index的日志对应的chan, 看是否已经被状态机应用
func (kv *KVServer) GetChan(index int) chan *OpReply {
	if c, ok := kv.opChans[index]; ok {
		return c
	}
	kv.opChans[index] = make(chan *OpReply, 1)
	return kv.opChans[index]
}

func (kv *KVServer) ReleaseChan(index int) {
	if c, ok := kv.opChans[index]; ok {
		close(c)
	}
	delete(kv.opChans, index)
}

// OpHandler
// 处理get, put, append操作的统一rpc
func (kv *KVServer) OpHandler(args *OpRequest, reply *OpReply) {
	index, _, isLeader := kv.rf.Start(*args)
	if !isLeader {
		reply.Err = ErrWrongLeader
		// kv.mu.Unlock()
		return
	}
	LOG("[server %d] 将op = %+v 交给raft", kv.me, args)
	kv.mu.Lock()
	// 只有当raft节点通过applyCh告知时, 才知道能被应用到状态机, 才能成功返回
	c := kv.GetChan(index)
	kv.mu.Unlock()
	select {
	case opReply := <-c:
		LOG("成功应用到状态机")
		*reply = *opReply
	case <-time.After(ExecuteTimeout):
		LOG("超时了.....")
		reply.Err = ErrTimeout
	}
	kv.mu.Lock()
	kv.ReleaseChan(index)
	kv.mu.Unlock()
}

func (kv *KVServer) applyOp(op OpRequest) *OpReply {
	// LOG("[server] 开始应用op = %+v", op)
	if reply, ok := kv.MultiOp(&op); ok {
		return reply
	}
	reply := &OpReply{Err: OK}
	if op.Type == GET {
		reply.Value, reply.Err = kv.kv.Get(op.Key)
	} else if op.Type == APPEND {
		reply.Err = kv.kv.Append(op.Key, op.Value)
	} else if op.Type == PUT {
		reply.Err = kv.kv.Put(op.Key, op.Value)
	} else {
		panic(fmt.Sprintf("[applyOp] unknown op %d", op.Type))
	}
	kv.lasReply[op.ClientID] = &OpIdentity{OpID: op.OpID, Reply: reply}
	return reply
}

func (kv *KVServer) MultiOp(op *OpRequest) (*OpReply, bool) {
	if opIdentity, ok := kv.lasReply[op.ClientID]; ok && opIdentity.OpID == op.OpID {
		return opIdentity.Reply, true
	}
	return nil, false
}

func (kv *KVServer) shouldSnapshot(op *OpRequest) bool {
	if kv.maxraftstate == -1 {
		return false
	}
	if kv.maxraftstate <=
		2*binary.Size(*op)+8+kv.rf.GetRaftStateSize() {
		return true
	}
	return false
}

func (kv *KVServer) applyTicker() {
	for kv.killed() == false {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				kv.mu.Lock()
				if msg.CommandIndex <= kv.lasApplied {
					kv.mu.Unlock()
					continue
				}
				kv.lasApplied = msg.CommandIndex
				op := msg.Command.(OpRequest)
				reply := kv.applyOp(op)
				// if currentTerm, isLeader := kv.rf.GetState(); isLeader && currentTerm == msg.
				if kv.rf.IsLeader() {
					LOG("收到已提交日志 %+v", msg)
					LOG("op = %+v", op)
					c := kv.GetChan(msg.CommandIndex)
					c <- reply
				}
				if kv.shouldSnapshot(&op) {
					kv.MakeSnapshot(msg.CommandIndex)
				}
				kv.mu.Unlock()
			} else if msg.SnapshotValid {
				kv.mu.Lock()
				kv.recoverFromSnapshot(msg.Snapshot)
				kv.mu.Unlock()
			} else {
				panic(fmt.Sprintf("[applyTicker] unexpected Message %v", msg))
			}
		}
	}
}

// Kill
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// commitIndex前的所有日志生成一个快照
func (kv *KVServer) MakeSnapshot(commitIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(kv.kv)
	_ = e.Encode(kv.lasReply)
	_ = e.Encode(kv.lasApplied)
	kv.rf.Snapshot(commitIndex, w.Bytes())
}

// 从快照中恢复状态机的数据
func (kv *KVServer) recoverFromSnapshot(data []byte) {
	if data != nil && len(data) != 0 {
		var Kvs SimpleKV
		var LasReply map[int64]*OpIdentity
		var LasApplied int
		d := labgob.NewDecoder(bytes.NewBuffer(data))
		err1 := d.Decode(&Kvs)
		err2 := d.Decode(&LasReply)
		err3 := d.Decode(&LasApplied)
		if err1 != nil || err2 != nil || err3 != nil {
			panic(fmt.Sprintf("读取持久化数据失败 err1 = %v, err2 = %v, err3 = %v", err1, err2, err3))
		} else {
			kv.kv = &Kvs
			kv.lasReply = LasReply
			kv.lasApplied = LasApplied
		}
	}
}

// StartKVServer
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(OpRequest{})
	labgob.Register(OpReply{})
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.opChans = make(map[int]chan *OpReply)
	kv.lasReply = make(map[int64]*OpIdentity)
	kv.kv = KVStore(Makekv())
	// You may need initialization code here.
	kv.recoverFromSnapshot(persister.ReadSnapshot())
	go kv.applyTicker()
	return kv
}
