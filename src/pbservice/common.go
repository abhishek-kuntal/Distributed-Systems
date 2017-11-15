package pbservice

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongServer  = "ErrWrongServer"
	ErrUninitServer = "ErrUninitServer"
)

type Err string

const (
	Get       = "Get"
	Put       = "Put"
	Append    = "Append"
)

//type Method string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	
	// You'll have to add definitions here.
	OpID     int64
	Method   string

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	
	// You'll have to add definitions here.
	OpID    int64
}

type GetReply struct {
	Err   Err
	Value string
}

type InitialStateArgs struct {
	State map[string]string
}

type InitialStateReply struct {
	Err   Err
}

type TfStateArgs struct {
	Target string
}

type TfStateReply struct {
}

// Utility funcs
func copyGetReply(dst *GetReply, src *GetReply) {
	dst.Value = src.Value
	dst.Err = src.Err
}

func copyPutAppendReply(dst *PutAppendReply, src *PutAppendReply) {
	dst.Err = src.Err
}
