package kvpaxos

import "time"

type Err string
type OpType string

const (
	OK       Err = "OK"
	ErrNoKey Err = "ErrNoKey"
)

const (
	PutOp       OpType = "Put"
	AppendOp    OpType = "Append"
	GetOp       OpType = "Get"
	NoOp        OpType = "NoOp"
	PutAppendOp OpType = "PutAppend" // required for server.go routing 
)

const (
	InitialBackoff = 10 * time.Millisecond
	MaxBackoff     = 10 * time.Second
)

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key    string
	Value  string
	OpType OpType // "Put" or "Append"
	
	ClientId int64
	Seq      int
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
	ClientId int64
	Seq      int
}

type GetReply struct {
	Value string
	Err   Err

}
