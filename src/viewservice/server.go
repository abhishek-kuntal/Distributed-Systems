package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	curView View
	views       map[string]time.Time
	Ack         bool
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	// Your code here.
	
	vs.mu.Lock()
	defer vs.mu.Unlock() // used defer to defer the unlocking of the lock until the function 
						 //surrounding it unlocks.

	if vs.curView.Primary == args.Me && vs.curView.Viewnum == args.Viewnum {
		vs.Ack = true
	}
	vs.views[args.Me] = time.Now()
	if args.Viewnum == 0 {
		if vs.curView.Primary == "" && vs.curView.Backup == "" {
			vs.curView.Primary = args.Me
			vs.curView.Viewnum = 1
		} else if vs.curView.Primary == args.Me {
			vs.views[args.Me] = time.Time{}
		} else if vs.curView.Backup == args.Me {
			vs.views[args.Me] = time.Time{}
		}
	}
	reply.View = vs.curView

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	reply.View = vs.curView

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	for server, i := range vs.views {
		if time.Now().Sub(i) > DeadPings*PingInterval {
			delete(vs.views, server)
			if vs.Ack {
				if server == vs.curView.Primary {
					vs.curView.Primary = ""
				}
				if server == vs.curView.Backup {
					vs.curView.Backup = ""
				}
			}
		}
	}
	if vs.Ack {
		flag := false
		if vs.curView.Primary == "" && vs.curView.Backup != "" {
			vs.curView.Primary = vs.curView.Backup
			vs.curView.Backup = ""
			flag = true
		}
		if vs.curView.Backup == "" {
			for server, _ := range vs.views {
				if server != vs.curView.Primary {
					vs.curView.Backup = server
					flag = true
					break
				}
			}
		}
		if flag {
			vs.curView.Viewnum++
			vs.Ack = false
		}
	}

}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.curView = View{0, "", ""}
	vs.Ack = false
	vs.views = make(map[string]time.Time)
	
	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs) 

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l
	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept() //block utill Dail
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn) //block utill rpc call
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()
	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
