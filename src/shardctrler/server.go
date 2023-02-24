package shardctrler

import (
	"6.5840/raft"
	"fmt"
	"sort"
	"sync/atomic"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead     int32
	configs  []Config // indexed by config num
	opChans  map[int]chan *OpReply
	lasReply map[int64]*OpIdentity
}

type Op struct {
	// Your data here.
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
}

func (sc *ShardCtrler) OpHandler(args *OpRequest, reply *OpReply) {
	index, _, isLeader := sc.rf.Start(*args)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
	LOG("[server %d] 将op = %+v 交给raft", sc.me, args)
	sc.mu.Lock()
	// 只有当raft节点通过applyCh告知时, 才知道能被应用到状态机, 才能成功返回
	c := sc.GetChan(index)
	sc.mu.Unlock()
	select {
	case opReply := <-c:
		//
		//LOG("成功应用到状态机")
		*reply = *opReply
	case <-time.After(ExecuteTimeout):
		LOG("超时了.....")
		reply.Err = ErrTimeout
	}
	sc.mu.Lock()
	sc.ReleaseChan(index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) GetChan(index int) chan *OpReply {
	if c, ok := sc.opChans[index]; ok {
		return c
	}
	sc.opChans[index] = make(chan *OpReply, 1)
	return sc.opChans[index]
}

func (sc *ShardCtrler) ReleaseChan(index int) {
	if c, ok := sc.opChans[index]; ok {
		close(c)
	}
	delete(sc.opChans, index)
}
func (sc *ShardCtrler) MultiOp(op *OpRequest) (*OpReply, bool) {
	if opIdentity, ok := sc.lasReply[op.ClientID]; ok && opIdentity.OpID == op.OpID {
		return opIdentity.Reply, true
	}
	return nil, false
}

func copyMp(mp map[int][]string) map[int][]string {
	newMp := make(map[int][]string)
	for key, value := range mp {
		newMp[key] = value
	}
	return newMp
}

func (sc *ShardCtrler) exeJoin(args *OpRequest, reply *OpReply) {
	LOG("exeJoin")
	lasConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{Num: len(sc.configs), Groups: copyMp(lasConfig.Groups)}
	for gid, servers := range args.Servers {
		if _, ok := newConfig.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}
	// 重新分配分片
	var newShared [NShards]int
	gids := make([]int, 0)
	for gid, _ := range newConfig.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	gid := 0
	gidNum := len(newConfig.Groups)
	LOG("gidNum = %d, gids = %v", gidNum, gids)
	for shard, _ := range lasConfig.Shards {
		if gidNum == 0 {
			newShared[shard] = 0
		} else {
			newShared[shard] = gids[gid]
			gid = (gid + 1) % gidNum
		}
	}
	newConfig.Shards = newShared
	sc.configs = append(sc.configs, newConfig)
	reply.Config = newConfig
	LOG("配置变为%+v", newConfig)
}

func (sc *ShardCtrler) exeLeave(args *OpRequest, reply *OpReply) {
	LOG("exeLeave")
	lasConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{Num: len(sc.configs), Groups: copyMp(lasConfig.Groups)}
	for _, gid := range args.GIDs {
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}
	}
	// 重新分配分片
	var newShared [NShards]int
	gids := make([]int, 0)
	for gid, _ := range newConfig.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	gid := 0
	gidNum := len(newConfig.Groups)
	for shard, _ := range lasConfig.Shards {
		if gidNum == 0 {
			newShared[shard] = 0
		} else {
			newShared[shard] = gids[gid]
			gid = (gid + 1) % len(newConfig.Groups)
		}
	}
	newConfig.Shards = newShared
	sc.configs = append(sc.configs, newConfig)
	reply.Config = newConfig
}

func (sc *ShardCtrler) exeQuery(args *OpRequest, reply *OpReply) {
	// reply.Config =
	if args.Num == -1 || args.Num >= len(sc.configs) {
		reply.Config = sc.configs[len(sc.configs)-1]
	} else {
		reply.Config = sc.configs[args.Num]
	}
}

// 创建一个新的配置，其中将分片args.shared分配给组args.GID。
func (sc *ShardCtrler) exeMove(args *OpRequest, reply *OpReply) {
	lasConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{Num: len(sc.configs), Shards: lasConfig.Shards, Groups: copyMp(lasConfig.Groups)}
	if args.Shard >= len(newConfig.Shards) {
		panic("没有该分片")
	}
	if _, ok := newConfig.Groups[args.GID]; !ok {
		panic("没有该gid")
	}
	newConfig.Shards[args.Shard] = args.GID
	sc.configs = append(sc.configs, newConfig)
	reply.Config = newConfig
}

func (sc *ShardCtrler) applyOp(op OpRequest) *OpReply {
	//
	//LOG("[server] 开始应用op = %+v", op)
	if reply, ok := sc.MultiOp(&op); ok {
		return reply
	}
	reply := &OpReply{Err: OK, WrongLeader: false}
	if op.Type == Join {
		sc.exeJoin(&op, reply)
	} else if op.Type == Leave {
		sc.exeLeave(&op, reply)
	} else if op.Type == Move {
		sc.exeMove(&op, reply)
	} else if op.Type == Query {
		sc.exeQuery(&op, reply)
	} else {
		panic(fmt.Sprintf("[ShardCtrler.applyOp] unknown op %d", op.Type))
	}
	sc.lasReply[op.ClientID] = &OpIdentity{OpID: op.OpID, Reply: reply}
	return reply
}

func (sc *ShardCtrler) applyTicker() {
	for {
		select {
		case msg := <-sc.applyCh:
			if msg.CommandValid {
				sc.mu.Lock()
				op := msg.Command.(OpRequest)
				reply := sc.applyOp(op)
				if sc.rf.IsLeader() {
					c := sc.GetChan(msg.CommandIndex)
					c <- reply
				}
				sc.mu.Unlock()
			} else {
				panic(fmt.Sprintf("[applyTicker] unexpected Message %v", msg))
			}
		}
	}
}

// Kill
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) killed() bool {
	return atomic.LoadInt32(&sc.dead) == 1
}

// Raft
// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// StartServer
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(OpRequest{})
	labgob.Register(OpReply{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.dead = 0
	sc.opChans = make(map[int]chan *OpReply)
	sc.lasReply = make(map[int64]*OpIdentity)
	go sc.applyTicker()
	return sc
}
