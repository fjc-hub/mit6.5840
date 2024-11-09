package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// request for workers acquire task from coordinator(master)
type GetTaskReq struct {
}

// response for coordinator assign task to the worder
type GetTaskRsp struct {
	TaskType      TaskTypeEnum
	TaskSeqNum    int // task sequence number starting from 0. if < 0, represent there is no TODO task this time
	InputFileName string
	NReduce       int // number of reduce task
	NMap          int // number of map task
}

// request for workers submit completed task to coordinator(master)
type HandinTaskReq struct {
	TaskType   TaskTypeEnum
	TaskSeqNum int // task sequence number starting from 0
	// OutputFileName string
}

// response for coordinator recieved completed task
type HandinTaskRsp struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
