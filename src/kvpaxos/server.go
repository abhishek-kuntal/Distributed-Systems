package kvpaxos

import "net"
import "fmt"
import "time"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"

const Debug = 0

var log_mu sync.Mutex

//standard logging 
func (kv *KVPaxos) Log(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log_mu.Lock()
		defer log_mu.Unlock()

		me := kv.me

		fmt.Printf(format+"\n", a...)
		fmt.Printf("Server %d:\t", me)
		fmt.Printf("\x1b[0m")
		fmt.Printf("\x1b[%dm", (me%6)+31)
		
	}
	return
}

//debug printf for debugging
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	GetReply       *GetReply
	GetArgs        *GetArgs
	PutAppendReply *PutAppendReply
	PutAppendArgs  *PutAppendArgs
	
	OpType         OpType
	
	ClientId       int64
	PaxosSeq       int
	Seq            int
}

type ClientStatus struct {
	last_op     *Op
	highest_seq int
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	state_lock   sync.Mutex
	
	client_info  map[int64]*ClientStatus
	state        map[string]string
	
	noop_channel chan int
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	//Deffer unlock till code completion
	defer kv.mu.Unlock()

	handled_op, handled := kv.getOp(args.ClientId, args.Seq)

	if handled {
		if args.Seq == kv.client_info[args.ClientId].highest_seq { // has already processed
			reply.Value = handled_op.GetReply.Value
			reply.Err = handled_op.GetReply.Err
			kv.Log("Duplicate get request, orginal reply returned")
			return nil
		} else {
			kv.Log("Already process Get request and is not most recent!")
			return nil
		}
	}
	op := &Op{OpType: GetOp, GetArgs: args, GetReply: reply, ClientId: args.ClientId, Seq: args.Seq}
	instance_num := kv.logOp(op)

	kv.Log("get request finished %d!", instance_num, op)
	return nil
}

func (kv *KVPaxos) doGet(op *Op) {
	kv.Log("get op performed for %s\n", op.GetArgs.Key)
	op.GetReply.Value = kv.state[op.GetArgs.Key]
	op.GetReply.Err = OK
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	//defer unlock till code completions
	defer kv.mu.Unlock()
	//handled 
	handled_op, handled := kv.getOp(args.ClientId, args.Seq)
	if handled {
		if args.Seq == kv.client_info[args.ClientId].highest_seq { // processed already
			//check error
			reply.Err = handled_op.PutAppendReply.Err
			kv.Log("duplicate putapeends orginal reply returned")
			return nil
		} else {
			err := fmt.Errorf("already processed PutAppend (%v) but not most recent", args)
			// log.Panicf(err)
			return err
		}
	}
	//output reference
	op := &Op{OpType: PutAppendOp, PutAppendArgs: args, PutAppendReply: reply, ClientId: args.ClientId, Seq: args.Seq}
	
	//instnace number
	instance_num := kv.logOp(op)

	kv.Log("putAppend request finished %d!", instance_num)
	return nil
}

func (kv *KVPaxos) doPutAppend(op *Op) {
	kv.Log("putAppend operation applying. (%d %d) to %s\n", op.ClientId, op.Seq, op.PutAppendArgs.Key)
	
	//check with previous value

	prev_val := ""
	if op.PutAppendArgs.OpType == AppendOp {
		prev_val, _ = kv.state[op.PutAppendArgs.Key]
	}

	//extract state
	kv.state[op.PutAppendArgs.Key] = prev_val + op.PutAppendArgs.Value
	op.PutAppendReply.Err = OK
}

func (kv *KVPaxos) doOp(op *Op, seq int) {
	//Do operation also check for noOp
	if op.OpType == NoOp {
		kv.Log("NoOp applying")
		return
	}
	//extract handled
	handled_op, handled := kv.getOp(op.ClientId, op.Seq)
	//handling exception
	if handled {
		kv.Log("already op handled with clientId %d, Seq %d (%d, %d) PaxosSeq %d!", op.ClientId, op.Seq, handled_op.ClientId, handled_op.Seq, handled_op.PaxosSeq)
	} else {
		//op type distribution
		switch op.OpType {
		case GetOp:
			kv.doGet(op)
		case PutAppendOp:
			kv.doPutAppend(op)
		}
		//lock state before upgraifng 
		kv.state_lock.Lock()
		kv.client_info[op.ClientId] = &ClientStatus{last_op: op, highest_seq: op.Seq}
		kv.state_lock.Unlock()
	}
}

func (kv *KVPaxos) logOp(op *Op) int {
	attempted_seq := kv.px.Max() + 1
	kv.Log("Attempting Op %#v with seq %d", op, attempted_seq)
	performed := kv.getConsensus(op, attempted_seq)
	for !performed {
		attempted_seq++
		kv.Log("Op %#v was not performed! Retrying again with seq %d", op, attempted_seq)
		performed = kv.getConsensus(op, attempted_seq)
	}
	op.PaxosSeq = attempted_seq

	//now op is in log, must get noops if needed
	time.Sleep(50 * time.Millisecond)
	_, handled := kv.getOp(op.ClientId, op.Seq)
	for !handled {
		kv.noop_channel <- attempted_seq
		time.Sleep(500 * time.Millisecond)
		_, handled = kv.getOp(op.ClientId, op.Seq)
	}

	kv.px.Done(attempted_seq)
	kv.Log("Handled op for first time from client %d, seq num %d Paxos seq %d!", op.ClientId, op.Seq, op.PaxosSeq)
	return attempted_seq
}


//get Op function
func (kv *KVPaxos) getOp(client int64, seq int) (*Op, bool) {
	kv.state_lock.Lock()
	//defer unlcok till code block executes
	defer kv.state_lock.Unlock()
	
	//extract client status
	client_status, exists := kv.client_info[client]
	//error handling 
	if !exists {
		client_status = &ClientStatus{last_op: &Op{}, highest_seq: -1}
		kv.client_info[client] = client_status
	}
	//update hadled variable
	hasBeenHandled := (seq <= client_status.highest_seq)

	return client_status.last_op, hasBeenHandled
}

func (kv *KVPaxos) getConsensus(op *Op, seq int) bool {
	kv.Log("Trying to get consenus for %d with possible op %#v", seq, op)
	kv.px.Start(seq, op)

	to := InitialBackoff
	for {
		fate, current_op := kv.px.Status(seq)
		if fate == paxos.Decided {
			kv.Log("Achieved paxos consensus for # %d for op %#v Op is same?: %t", seq, current_op, current_op == op)
			return current_op == op
		} else if fate == paxos.Forgotten {
			err := fmt.Errorf("Server %d got Forgotten for op %d: %#v", kv.me, seq, op)
			log.Panicln(err)
		}

		kv.Log("Trying to get consenus for %d with possible op %#v. Op is not fate, wait and try again!", seq, op)

		time.Sleep(to)
		if to < MaxBackoff {
			to *= 2
		}
	}

}

func (kv *KVPaxos) handleOps() {
	time.Sleep(100 * time.Millisecond)
	seq := 0
	for {
		select {
		case max_seq := <-kv.noop_channel:
			for seq_start := seq; seq_start < max_seq; seq_start++ {
				kv.Log("Proposing NoOp for Paxos seq: %d", seq_start)
				kv.px.Start(seq_start, &Op{OpType: NoOp})
			}

		default:
			fate, rcv_op := kv.px.Status(seq)
			var op *Op
			switch rcv_op.(type) {
			case Op:
				temp := rcv_op.(Op)
				op = &temp
			case *Op:
				op = rcv_op.(*Op)
			}
			if fate == paxos.Decided {
				kv.Log("Applying op %d: %#v ", seq, op)
				kv.doOp(op, seq)
				seq++
			} else if fate == paxos.Forgotten {
				log.Panicf("Server %d requested Forgotten op %d: %#v", kv.me, seq, op)
			} else {
				time.Sleep(5 * time.Millisecond)
			}
		}
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.

	kv.client_info = make(map[int64]*ClientStatus)
	kv.state = make(map[string]string)
	kv.noop_channel = make(chan int)

	go kv.handleOps()


	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
