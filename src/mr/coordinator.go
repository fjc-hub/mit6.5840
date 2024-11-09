package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	MAX_TASK_TIME = 10 // max task execution time (second)
)

// map and reduce task status enums
type taskstatus int

const (
	TODO_STATUS taskstatus = iota // tasks need to do
	PRCS_STATUS                   // tasks processing
	COML_STATUS                   // tasks completed
)

type TaskTypeEnum string

const (
	MAP_TASK TaskTypeEnum = "M"
	RDC_TASK TaskTypeEnum = "R"
)

const (
	INTERMEDIATE_FILE_FORMAT = "mr-%v-%v"  // the name format of intermediate file that map task produces
	OUTPUT_FILE_FORMAT       = "mr-out-%v" // the name format of final output file that reduce task produces
)

type Coordinator struct {
	nMap       int
	nReduce    int
	inputFiles []string

	/* Lock order: (mLock, rLock, eLock) for avoiding deadlock when needs acquire multiple locks */
	mLock          sync.Mutex // lock for protecting sharing mTasks + finishedMapCnt + mExpire
	mTasks         []taskstatus
	finishedMapCnt int
	mExpire        []int64

	rLock             sync.Mutex // lock for protecting sharing rTasks + finishedReduceCnt + rExpire
	rTasks            []taskstatus
	finishedReduceCnt int
	rExpire           []int64
}

// RPC handlers for the worker to call.

// acquire todo task
func (c *Coordinator) GetTask(req *GetTaskReq, rsp *GetTaskRsp) error {
	rsp.NMap = c.nMap
	rsp.NReduce = c.nReduce
	rsp.TaskSeqNum = -1 // represent no TODO task

	c.mLock.Lock()
	if c.finishedMapCnt < c.nMap {
		// 1.query unassigned task from mTasks
		for i, s := range c.mTasks {
			if s != TODO_STATUS {
				continue
			}
			// update task status
			c.mTasks[i] = PRCS_STATUS
			// update task expired timestamp
			c.mExpire[i] = time.Now().Unix() + MAX_TASK_TIME
			// package response
			rsp.InputFileName = c.inputFiles[i]
			rsp.TaskType = MAP_TASK
			rsp.TaskSeqNum = i
			break
		}
		c.mLock.Unlock()
		return nil
	} else {
		c.mLock.Unlock()
		// 2.query unassigned task from rTasks
		c.rLock.Lock()
		for i, s := range c.rTasks {
			if s != TODO_STATUS {
				continue
			}
			// update task status
			c.rTasks[i] = PRCS_STATUS
			// update task expired timestamp
			c.rExpire[i] = time.Now().Unix() + MAX_TASK_TIME
			// package response
			rsp.TaskType = RDC_TASK
			rsp.TaskSeqNum = i
			break
		}
		c.rLock.Unlock()
		return nil
	}
}

// hand in completed task
func (c *Coordinator) HandinTask(req *HandinTaskReq, rsp *HandinTaskRsp) error {
	idx := req.TaskSeqNum
	if req.TaskType == MAP_TASK {
		c.mLock.Lock()
		if c.mTasks[idx] == PRCS_STATUS {
			// increase finishedMapCnt counter
			c.finishedMapCnt++
			// update task's status
			c.mTasks[idx] = COML_STATUS
		}
		c.mLock.Unlock()
	} else if req.TaskType == RDC_TASK {
		c.rLock.Lock()
		if c.rTasks[idx] == PRCS_STATUS {
			// increase finishedReduceCnt counter
			c.finishedReduceCnt++
			// update task's status
			c.rTasks[idx] = COML_STATUS
		}
		c.rLock.Unlock()
	} else {
		log.Fatal(fmt.Errorf("HandinTask receive invalid taskType(%v)", req.TaskType))
	}
	return nil
}

func (c *Coordinator) rerunTimeoutTasks() {
	for {
		// 1.checking map tasks
		now := time.Now().Unix()
		c.mLock.Lock()
		if c.finishedMapCnt < len(c.mTasks) {
			for i, s := range c.mTasks {
				if s == PRCS_STATUS && now-c.mExpire[i] > MAX_TASK_TIME {
					c.mTasks[i] = TODO_STATUS          // set task's status to init
					c.mExpire[i] = now + MAX_TASK_TIME // reset expire time
				}
			}
		}
		c.mLock.Unlock()

		// 2.checking reduce tasks
		now = time.Now().Unix()
		c.rLock.Lock()
		if c.finishedReduceCnt < len(c.rTasks) {
			for i, s := range c.rTasks {
				if s == PRCS_STATUS && now-c.rExpire[i] > MAX_TASK_TIME {
					c.rTasks[i] = TODO_STATUS          // set task's status to init
					c.rExpire[i] = now + MAX_TASK_TIME // reset expire time
				}
			}
		}
		c.rLock.Unlock()

		time.Sleep(1 * time.Second)
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	// run a daemon goroutine for checking if task is timeout
	go c.rerunTimeoutTasks()

	// run a master server
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// check if all map/reduce tasks are completed
	c.mLock.Lock()
	if c.finishedMapCnt != len(c.mTasks) {
		c.mLock.Unlock()
		return false
	}
	c.mLock.Unlock()

	c.rLock.Lock()
	if c.finishedReduceCnt != len(c.rTasks) {
		c.rLock.Unlock()
		return false
	}
	c.rLock.Unlock()

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	if len(files) == 0 || nReduce == 0 {
		log.Fatal(fmt.Errorf("invalid arguments: MakeCoordinator"))
	}
	c := Coordinator{
		nMap:       len(files),
		nReduce:    nReduce,
		inputFiles: files,
		mTasks:     make([]taskstatus, len(files)), // inited to TODO_STATUS
		rTasks:     make([]taskstatus, nReduce),    // inited to TODO_STATUS
		mExpire:    make([]int64, len(files)),
		rExpire:    make([]int64, nReduce),
	}

	c.server()
	return &c
}
