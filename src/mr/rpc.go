package mr

//
// RPC definitions.
//

import (
	"os"
	"strconv"
)

type TaskType string

const (
	MapTask    TaskType = "map"
	ReduceTask TaskType = "reduce"
)

// RPC definitions
type GiveTaskArgs struct {
	Pid int
}

type GiveTaskReply struct {
	File string
	Task TaskType
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
