package kvraft

import (
	"log"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"

	ErrTimeout = "ErrTimeout"
)

const (
	debug = false
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

// OpRequest
// get,append,put都用这个rpc来处理吧
type OpRequest struct {
	Key      string
	Value    string
	Type     Operator
	ClientID int64
	OpID     int64
}

type OpReply struct {
	Key   string
	Value string
	Err   Err
}

type OpIdentity struct {
	OpID  int64
	Reply *OpReply
}

func LOG(str string, args ...interface{}) {
	if debug {
		log.Printf(str, args...)
	}
}
