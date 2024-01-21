package mr

//
// RPC definitions.
//

import (
	"os"
	"strconv"
)

// RPC definitions
type GiveTaskArgs struct {
	Pid int
}

type GiveTaskReply struct {
	File     string
	Task     string
	NReducer int
	TaskId   int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
