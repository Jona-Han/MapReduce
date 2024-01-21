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
}

type GiveTaskReply struct {
	File     string
	Task     string
	NReducer int
	NFiles   int
	TaskId   int
}

type MarkTaskCompletedArgs struct {
	TaskId int
}

type MarkTaskCompletedReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
