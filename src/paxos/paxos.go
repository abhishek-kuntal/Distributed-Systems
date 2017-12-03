package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"
import "time"
import "math"

const Debug = 0
const WaitTime = 100 * time.Millisecond

var log_mu sync.Mutex

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	instances map[int]*PaxosInstance
	dones     []int
}

// A singular instance of Paxos
type PaxosInstance struct {
	seq        int
	n_prep int         // higeest prep seen
	n_acpt int         // higest accept  seen
	v_acpt interface{} // current accepted recent value
	v_dec  interface{} // decided value
}

func (px *Paxos) Instantiate(seq int) *PaxosInstance {
	px.Log("Paxos Instance created for seq %d", seq)
	pi := &PaxosInstance{
		n_prep: 0,
		n_acpt: 0,
		v_acpt: nil,
		v_dec:  nil,
	}

	px.instances[seq] = pi
	return pi
}

// RPC Arguments and Reply Types
type Reply string

const (
	OK       Reply = "OK"
	Rejected Reply = "Rejected"
)

type PrepareReply struct {
	HighestAccepted int
	ValueAccepted   interface{}
	Reply           Reply
	HighestDone     int
}

type PrepareArgs struct {
	Number int
	Seq    int
}

type AcceptReply struct {
	Number      int
	Reply       Reply
	HighestDone int
}

type AcceptArgs struct {
	Number int
	Value  interface{}
	Seq    int
}

type DecidedArgs struct {
	ValueDecided interface{}
	Seq          int
}

type DecidedReply struct {
}

//Logging for debugging purpose. 
func (px *Paxos) Log(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log_mu.Lock()
		//Defer unlock till code executes
		defer log_mu.Unlock()

		me := px.me
		//print logs for debugging 
		fmt.Printf(format+"\n", a...)
		fmt.Printf("Server %d:\t", me)
		fmt.Printf("\x1b[%dm", (px.me%6)+31)
		fmt.Printf("\x1b[0m")
	}
	return
}

func shorten(in interface{}) string {
	val := fmt.Sprintf("%+v", in)
	if len(val) > 50 {
		val = "" + val[0:47] + "..."
	}
	return val
}


// RPC Handlers
// Prepare handler
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	//Defer unlock till code executes
	defer px.mu.Unlock()
	//logs
	px.Log("Seq of the received prepare message %d", args.Seq)
	
	pi, exists := px.instances[args.Seq]//check whether sequence number exists or not
	if !exists {
		pi = px.Instantiate(args.Seq)
	}
	//check for current sequence number to be greater than previous.
	if args.Number > pi.n_prep {
		pi.n_prep = args.Number
		reply.HighestAccepted = pi.n_acpt
		reply.ValueAccepted = pi.v_acpt
		reply.Reply = OK
	} else {
		reply.Reply = Rejected
	}
	//update highest done till now
	reply.HighestDone = px.dones[px.me]

	px.Log("Seq number of the prepare message %d replied to with %s", args.Seq, reply.Reply)
	return nil
}

// Accept handler
func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	//Defer unlock till code executes
	defer px.mu.Unlock()

	px.Log("Seq of the received acceot message %d", args.Seq)
	pi, exists := px.instances[args.Seq]
	if !exists {
		pi = px.Instantiate(args.Seq)
	}
	//check whehter the number came is greater than the previous prepared
	//reject it otherwise
	if args.Number >= pi.n_prep {
		pi.n_prep = args.Number
		pi.n_acpt = args.Number
		pi.v_acpt = args.Value
		reply.Reply = OK
	} else {
		reply.Reply = Rejected
	}
	//update highest done till now. 
	reply.HighestDone = px.dones[px.me]

	px.Log("Replied to Accept Message for seq %d with %s", args.Seq, reply.Reply)
	return nil
}

// handler to
func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
	px.mu.Lock()
	//Defer unlock till code executes
	defer px.mu.Unlock()

	px.Log("Receieved a Decided Message for seq %d", args.Seq)
	pi, exists := px.instances[args.Seq]
	if !exists {
		pi = px.Instantiate(args.Seq)
	}

	pi.v_dec = args.ValueDecided
	return nil
}

// Generate a proposal number for paxos instance
func (px *Paxos) GenerateProposalNumber() int {
	return int(time.Now().UnixNano())*len(px.peers) + px.me
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("Dial() paxos faield : %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {

	px.Log("called start method {seq: %d, proposed value: %s}", seq, shorten(v))
	status, _ := px.Status(seq)

	go px.Propose(seq, v, status)
}

func (px *Paxos) Propose(seq int, v interface{}, status Fate) {
	for !(status == Decided || status == Forgotten || px.isdead()) {
		n := px.GenerateProposalNumber()

		highest_n_a := 0
		v_prime := v

		// Prepare Stage
		prepare_oks := 0
		for idx, peer := range px.peers {
			args := &PrepareArgs{Number: n, Seq: seq}
			var reply PrepareReply
			px.Log("message prepare sending to server %d with seq %d and prepare number %d", idx, seq, n)
			if idx == px.me {
				px.Prepare(args, &reply)
			} else {
				call(peer, "Paxos.Prepare", args, &reply)
			}

			if reply.Reply == OK {
				px.Log("Prepare OK from %d processed with seq %d", idx, seq)
				prepare_oks++
				if reply.HighestAccepted > highest_n_a {
					highest_n_a = reply.HighestAccepted
					v_prime = reply.ValueAccepted
				}
			}

			px.dones[idx] = reply.HighestDone
		}

		if prepare_oks < len(px.peers)/2+1 {
			px.Log("Only received seq %d received %d oKs for prepare retrying with different n", seq, prepare_oks)
			time.Sleep(WaitTime)
			continue
		}

		px.Log("Seq %d prepare stage completed {value: %v, highest_n_a: %d, n: %d}", seq, shorten(v_prime), highest_n_a, n)

		// Accept Stage
		accept_oks := 0
		for idx, peer := range px.peers {
			args := &AcceptArgs{Number: n, Value: v_prime, Seq: seq}
			var reply AcceptReply
			px.Log("Sending Accept message to Server %d with seq %d and number %d", idx, seq, n)
			if idx == px.me {
				px.Accept(args, &reply)
			} else {
				call(peer, "Paxos.Accept", args, &reply)
			}

			if reply.Reply == OK {
				px.Log("Received an Accept OK from %d for seq %d", idx, seq)
				accept_oks++
			}

			px.dones[idx] = reply.HighestDone
		}

		if accept_oks < len(px.peers)/2+1 {
			px.Log("Seq %d only receieved %d OKs for the Accept! Retrying with different n...", seq)
			time.Sleep(WaitTime)
			continue
		}

		px.Log("Seq %d completed the Accept Stage! {value: %v, n: %d}", seq, shorten(v_prime), n)

		// Send Decided message 
		status = Decided
		for idx, peer := range px.peers {
			args := &DecidedArgs{ValueDecided: v_prime, Seq: seq}
			var reply DecidedReply

			px.Log("Sending decided message to Server %d for seq %d and decided value %v", idx, seq, shorten(v_prime))
			if idx == px.me {
				px.Decided(args, &reply)
			} else {
				call(peer, "Paxos.Decided", args, &reply)
			}
		}
	}
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	px.Log("called done with seq %d ", seq)
	if seq > px.dones[px.me] {
		px.dones[px.me] = seq
	}
	px.Min()
}

func (px *Paxos) Forget(threshold int) {
	px.mu.Lock()
	//Defer unlocking till code completes. 
	defer px.mu.Unlock()

	px.Log("called forget with threshold %d", threshold+1)
	//check for threshold values. 
	for seq, _ := range px.instances {
		if seq <= threshold {
			delete(px.instances, seq)
		}
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	//find max sequence number 
	px.mu.Lock()
	//Defer unlocking till code completes. 
	defer px.mu.Unlock()

	max := -1
	//check for max sequnce number
	for seq, _ := range px.instances {
		if max < seq {
			max = seq
		}
	}
	px.Log("Max method called, with answer %d ", max)
	return max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	//find minimum
	min := math.MaxInt32
	for _, z_i := range px.dones {
		if min > z_i {
			min = z_i
		}
	}

	px.Forget(min)
	px.Log("called min method array done is %v, min is %d", px.dones, min+1)

	return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	if seq < px.Min() {
		return Forgotten, nil
	}

	px.mu.Lock()
	//defer unlock till all code executed
	defer px.mu.Unlock()

	pi, exists := px.instances[seq]
	if !exists {
		px.Log("called status for seq %d. answer is (fate: %v, value: %v)", seq, Pending, nil)
		return Pending, nil
	}

	value := pi.v_dec
	fate := Pending
	if value != nil {
		fate = Decided
	}

	px.Log("called status for seq %d. answer is (fate: %v, value: %v)", seq, fate, shorten(value))
	return fate, value
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.dones = make([]int, len(peers))
	//initialize px dones with idx 
	for idx := range px.dones {
		px.dones[idx] = -1
	}
	//map instances 
	px.instances = make(map[int]*PaxosInstance)

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
