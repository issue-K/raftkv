package shardctrler

//
// Shardctrler clerk.
//

import "6.5840/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
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
	// Your code here.
	ck.clientID = nrand()
	ck.opID = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	// args := &QueryArgs{}
	// Your code here.
	args := &OpRequest{}
	args.Num = num
	args.Type = Query
	args.ClientID = ck.clientID
	args.OpID = ck.opID
	for {
		// try each known server.
		for _, srv := range ck.servers {
			// var reply QueryReply
			var reply OpReply
			ok := srv.Call("ShardCtrler.OpHandler", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.opID++
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	// args := &JoinArgs{}
	// Your code here.
	args := &OpRequest{}
	args.Servers = servers
	args.Type = Join
	args.ClientID = ck.clientID
	args.OpID = ck.opID
	for {
		// try each known server.
		for _, srv := range ck.servers {
			// var reply JoinReply
			var reply OpReply
			ok := srv.Call("ShardCtrler.OpHandler", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.opID++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	// args := &LeaveArgs{}
	// Your code here.
	args := &OpRequest{}
	args.GIDs = gids
	args.Type = Leave
	args.ClientID = ck.clientID
	args.OpID = ck.opID
	for {
		// try each known server.
		for _, srv := range ck.servers {
			// var reply LeaveReply
			var reply OpReply
			ok := srv.Call("ShardCtrler.OpHandler", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.opID++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	// args := &MoveArgs{}
	// Your code here.
	args := &OpRequest{}
	args.Shard = shard
	args.GID = gid
	args.Type = Move
	args.ClientID = ck.clientID
	args.OpID = ck.opID
	for {
		// try each known server.
		for _, srv := range ck.servers {
			// var reply MoveReply
			var reply OpReply
			ok := srv.Call("ShardCtrler.OpHandler", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.opID++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
