package mapreduce

import "container/list"
import "fmt"
import "strconv"


type WorkerInfo struct {
	address string
	res *DoJobReply
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) masterCalls(){
    for i := 0; i < nMap; i++ {
        mr.masterJob(Map, i)
    }
    jobCount := nMap
    for jobCount > 0 {
        c := <-mr.replyChannel
        if c.OK {
            jobCount--
        }
    }
    for i := 0; i < nReduce; i++ {
        mr.masterJob(Reduce, i)
    }
    jobCount = nReduce
    for jobCount > 0 {
        c := <-mr.replyChannel
        if c.OK {
            jobCount--
        }
    }
}

func (mr *MapReduce) masterJob(op JobType, i int) {
    args := &DoJobArgs{}
    args.Operation = op
    args.JobNumber = i
        args.File = mr.file
    switch op {
        case Map:
            args.NumOtherPhase = nReduce
        case Reduce:
            args.NumOtherPhase = nMap
    }
    mr.jobChannel <- args
}

func (mr *MapReduce) consumerCalls() {
    go func() {
        for mr.alive {
            select {
            case args := <-mr.jobChannel:
                go mr.consumer(args)
            default:
            }
        }
    }()

}

func (mr *MapReduce) consumer(args *DoJobArgs) {
    worker := <-mr.registerChannel
    var curWorker *WorkerInfo
    mr.mapLock.Lock()
    mr.Workers[strconv.Itoa(args.JobNumber)] = &WorkerInfo{worker, &DoJobReply{false}}
    curWorker = mr.Workers[strconv.Itoa(args.JobNumber)]
    mr.mapLock.Unlock()
    res := call(curWorker.address, "Worker.DoJob", args, curWorker.res)
    if res == false {
        mr.jobChannel <- args
    } else {
        mr.replyChannel <- curWorker.res
    }
    mr.registerChannel <- worker
}

func (mr *MapReduce) RunMaster() *list.List {
    mr.consumerCalls();
    mr.masterCalls();
    // Your code here
 	// go func() {
    //     for mr.alive {
    //         select {
    //         case args := <-mr.jobChannel:
    //             go mr.consumer(args)
    //         default:
    //         }
    //     }
    // }()
	// args := <-mr.jobChannel
	// worker := <- mr.registerChannel
	// var curWorker * WorkerInfo
	// mr.mapLock.Lock()
	// mr.Workers[strconv.Itoa(args.JobNumber)] = &WorkerInfo{worker, &DoJobReply{false}}
	// curWorker = mr.Workers[strconv.Itoa(args.JobNumber)]
	// mr.mapLock.Unlock()
	// ok := call(curWorker.address, "Worker.Job", args, curWorker.res)
	// if ok == false {
	// 	mr.jobChannel <- args
	// } else {
	// 	mr.replyChannel <- curWorker.res
	// }
	// mr.registerChannel <- worker
	
	// for i := 0; i < nMap; i++ {
    //     mr.doMasterJob(Map, i)
    // }
    // // wait for all map jobs to finish
    // jobCount := nMap
    // for jobCount > 0 {
    //     c := <-mr.replyChannel
    //     if c.OK {
    //         jobCount--
    //     }
    // }
    // for i := 0; i < nReduce; i++ {
    //     mr.doMasterJob(Reduce, i)
    // }
    // jobCount = nReduce
    // for jobCount > 0 {
    //     c := <-mr.replyChannel
    //     if c.OK {
    //         jobCount--
    //     }
    // }

	return mr.KillWorkers()
}
