package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"

// life of a filter : 10 sec
const FilterLife = int(10000 * time.Millisecond / viewservice.PingInterval)

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	
	// Your declarations here.
	init       bool                   
	view       *viewservice.View     
	keyValueStore    map[string]string       
	dropFilters    map[int64]int          
	replyMap    map[int64]interface{}  
}

func (pb *PBServer) isInitialized() bool {
	return pb.init
}

func (pb *PBServer) InitState(args *InitialStateArgs, reply *InitialStateReply) error {
	pb.mu.Lock()
	if !pb.isInitialized() {
		pb.init = true
		pb.keyValueStore = args.State
	}
	pb.mu.Unlock()

	reply.Err = OK
	return nil
}

func (pb *PBServer) filterDuplicate(opid int64, method string, reply interface{}) bool {
	_, ok := pb.dropFilters[opid]
	if !ok {
		return false
	}
	
	rp, ok2 := pb.replyMap[opid]
	if !ok2 {
		return false
	}

	// bad design, because there would be various types 
	// of operations to be filtered 
	if method == Get {
		reply, ok1 := reply.(*GetReply)
		saved, ok2 := rp.(*GetReply) 
		if ok1 && ok2 {
			copyGetReply(reply, saved)
			return true
		}
	} else if method == Put || method == Append {
		reply, ok1 := reply.(*PutAppendReply)
		saved, ok2 := rp.(*PutAppendReply)
		if ok1 && ok2 {
			copyPutAppendReply(reply, saved)
			return true
		}
	}
	return false
}

func (pb *PBServer) recordOperation(opid int64, reply interface{}) {
	pb.dropFilters[opid] = FilterLife
	pb.replyMap[opid] = reply
}

func (pb *PBServer) doGet(args *GetArgs, reply *GetReply) error {
	
	key := args.Key
	value, ok := pb.keyValueStore[key]
	if ok {
		reply.Err = OK
		reply.Value = value
	} else {
		reply.Err = ErrNoKey
	}
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	// the client (not a PBServer) thinks we are primary
	if pb.me != pb.view.Primary {
		reply.Err = ErrWrongServer
		return nil
	}
	
	ok := pb.filterDuplicate(args.OpID, Get, reply)
	if ok {
		return nil
	}

	// we think we are primary, forward the request to backup
	if pb.view.Backup != "" {
		ok := call(pb.view.Backup, "PBServer.BackupGet", args, reply)
		if ok {
			if reply.Err == ErrUninitServer {
				pb.transferState(pb.view.Backup)
			} else {
				return nil
			}
			// data on backup is more trusted than primary
		} else { 
			// unreliable backup / backup is down / network partition
			reply.Err = ErrWrongServer 
			return nil
		}
	}

	pb.doGet(args, reply)

	pb.recordOperation(args.OpID, reply)

	return nil
}

func (pb *PBServer) BackupGet(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	
	if pb.me != pb.view.Backup {
		reply.Err = ErrWrongServer
		return nil
	}
	
	// the request is from primary and we are backup

	if !pb.isInitialized() {
		reply.Err = ErrUninitServer
		return nil
	}

	ok := pb.filterDuplicate(args.OpID, Get, reply)
	if ok {
		return nil
	}

	pb.doGet(args, reply)

	pb.recordOperation(args.OpID, reply)

	return nil
}

func (pb *PBServer) doPutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	
	key, value := args.Key, args.Value
	method := args.Method
	if method == Put {
		pb.keyValueStore[key] = value
	} else if method == Append {
		pb.keyValueStore[key] += value
	} 
	reply.Err = OK

	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	
	if pb.me != pb.view.Primary {
		reply.Err = ErrWrongServer
		return nil
	}

	ok := pb.filterDuplicate(args.OpID, args.Method, reply)
	if ok {
		return nil
	}

	xferafter :=  false
	if pb.view.Backup != "" {
		
		tries := 1 // tring again doesn't help in many cases
		for tries > 0 {
			ok := call(pb.view.Backup, "PBServer.BackupPutAppend", args, reply)
			if ok {
				if reply.Err == ErrWrongServer {
					return nil
				} 
				if reply.Err == ErrUninitServer {
					xferafter = true
				} 
				break
			}
			tries--
		} 
		if tries == 0 {
			reply.Err = ErrWrongServer
			return nil
		}
	} 

	pb.doPutAppend(args, reply)
	pb.recordOperation(args.OpID, reply)

	if xferafter {
		pb.transferState(pb.view.Backup)
	}

	return nil
}

func (pb *PBServer) BackupPutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	
	if pb.me != pb.view.Backup {
		reply.Err = ErrWrongServer
		return nil
	}

	if !pb.isInitialized() {
		reply.Err = ErrUninitServer
		return nil
	}

	ok := pb.filterDuplicate(args.OpID, args.Method, reply)
	if ok {
		return nil
	}

	pb.doPutAppend(args, reply)
	pb.recordOperation(args.OpID, reply)

	return nil
}

func (pb *PBServer) transferState(target string) bool {
	if target != pb.view.Backup {
		return false
	}
	
	args := &InitialStateArgs{pb.keyValueStore}
	var reply InitialStateReply
	call(target, "PBServer.InitState", args, &reply)
	if reply.Err == OK {
		return true
	}
	return false
}

func (pb *PBServer) TransferState(
	args *TfStateArgs, reply *TfStateReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	pb.transferState(args.Target)

	return nil
}

func (pb *PBServer) pingViewServer() *viewservice.View {
	viewno := uint(0)
	if pb.view != nil {
		viewno = pb.view.Viewnum
	}
	v, e := pb.vs.Ping(viewno)
	if e == nil {
		return &v
	}
	return nil
}

func (pb *PBServer) cleanUpdropFilters() {
	for op := range pb.dropFilters {
		if pb.dropFilters[op] <= 0 {
			delete(pb.dropFilters, op)
			delete(pb.replyMap, op)
		} else {
			pb.dropFilters[op]--
		}
	}
}

func (pb *PBServer) stateStatus(server string) {
	args := &TfStateArgs{}
	args.Target = pb.me
	var reply TfStateReply
	go call(server, "PBServer.TransferState", args, &reply)
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	view := pb.pingViewServer()
	
	if view != nil {
	
		if !pb.init {
			if pb.me == view.Primary {
			} else if pb.me == view.Backup {
				pb.stateStatus(view.Primary)
			}
		}
		pb.view = view
	}

	pb.cleanUpdropFilters()
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.keyValueStore = make(map[string]string)
	pb.dropFilters = make(map[int64]int)
	pb.replyMap = make(map[int64]interface{})

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
