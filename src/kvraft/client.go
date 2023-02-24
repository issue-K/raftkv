package kvraft

import (
	"6.5840/labrpc"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderID int
	clientID int64
	opID     int64
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
	ck.clientID = nrand()
	ck.opID = 0
	LOG("!!!!!!!!!!!!!开始启动客户端, clientID = %v, opID = %v", ck.clientID, ck.opID)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := &OpRequest{Type: GET, Key: key}
	reply := &OpReply{Err: OK}
	ck.sendOp(args, reply)
	return reply.Value
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	//if op == "PUT" {
	//	ck.Put(key, value)
	//} else if op == "APP"
	panic(".........")
}

func (ck *Clerk) Put(key string, value string) {
	// ck.PutAppend(key, value, "Put")
	args := &OpRequest{Type: PUT, Key: key, Value: value}
	reply := &OpReply{Err: OK}
	ck.sendOp(args, reply)
}

func (ck *Clerk) Append(key string, value string) {
	// ck.PutAppend(key, value, "Append")
	LOG("========>>> append [%v] [%v]"+
		"", key, value)
	args := &OpRequest{Type: APPEND, Key: key, Value: value}
	reply := &OpReply{Err: OK}
	ck.sendOp(args, reply)
}

func (ck *Clerk) sendOp(args *OpRequest, reply *OpReply) {
	args.ClientID, args.OpID = ck.clientID, ck.opID
	LOG("开始发送op: %+v", args)
	for {
		i := ck.leaderID
		if ok := ck.servers[i].Call("KVServer.OpHandler", args, reply); !ok || reply.Err == ErrTimeout || reply.Err == ErrWrongLeader {

			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			continue
		}
		LOG("发送op成功, opID = %v", args.OpID)
		ck.opID++
		return
	}
}
