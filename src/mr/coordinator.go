package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type MapTask struct {
	fileName string
}

type ReduceTask struct {
	intermediateFiles []string
	reducerIndex      int
}

type Coordinator struct {
	nReduce     int
	isDone      bool
	tasksMu     sync.Mutex
	mapTasks    []MapTask
	reduceTasks []ReduceTask
}

// RPC handlers for the worker to call.

func (c *Coordinator) GiveTask(args *GiveTaskArgs, reply *GiveTaskReply) error {
	reply.File = c.mapTasks[0].fileName
	reply.Task = "map"
	return nil
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
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.isDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,
		isDone:  false,
	}
	partitionInputToTasks(files, &c)

	c.server()
	return &c
}

func partitionInputToTasks(files []string, c *Coordinator) {
	c.tasksMu.Lock()
	defer c.tasksMu.Unlock()

	// Iterate over the files and create a MapTask for each
	for _, file := range files {
		mapTask := MapTask{fileName: file}
		c.mapTasks = append(c.mapTasks, mapTask)
	}
}
