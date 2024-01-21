package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type MapTask struct {
	FileName string
}

type ReduceTask struct {
	IntermediateFiles []string
	ReducerIndex      int
}

type TaskStatus int

const (
	TaskNotStarted TaskStatus = iota
	TaskInProgress
	TaskCompleted
)

type TaskInfo struct {
	Type   string // "map" or "reduce"
	Status TaskStatus
	Map    MapTask
	Reduce ReduceTask
}

type Coordinator struct {
	nReducer          int
	nFiles            int
	isDone            bool
	isMapDone         bool
	tasksMu           sync.Mutex
	taskStatus        map[int]TaskInfo
	workerAssignments map[int]time.Time

	// mapTasks    []MapTask
	// reduceTasks []ReduceTask
}

// RPC handlers for the worker to call.

func (c *Coordinator) GiveTask(args *GiveTaskArgs, reply *GiveTaskReply) error {
	c.tasksMu.Lock()
	defer c.tasksMu.Unlock()
	if !c.isMapDone {
		for id, info := range c.taskStatus {
			if info.Status == TaskNotStarted && info.Type == "map" {
				c.AssignMapTask(reply, id, info)
				info.Status = TaskInProgress
				c.taskStatus[id] = info
				c.workerAssignments[id] = time.Now()
				return nil
			}
		}
		//Map is not done but all tasks are in progress
		reply.Task = "none"
	} else {
		for id, info := range c.taskStatus {
			if info.Status == TaskNotStarted && info.Type == "reduce" {
				c.AssignReduceTask(reply, id, info)
				info.Status = TaskInProgress
				c.taskStatus[id] = info
				c.workerAssignments[id] = time.Now()
				return nil
			}
		}
	}
	return nil
}

// Assigns a map task to a worker
func (c *Coordinator) AssignMapTask(reply *GiveTaskReply, id int, info TaskInfo) {
	reply.File = info.Map.FileName
	reply.NReducer = c.nReducer
	reply.NFiles = c.nFiles
	reply.TaskId = id
	reply.Task = "map"
}

// Assigns a reduce task to a worker
func (c *Coordinator) AssignReduceTask(reply *GiveTaskReply, id int, info TaskInfo) {
	reply.File = info.Map.FileName
	reply.NReducer = c.nReducer
	reply.NFiles = c.nFiles
	reply.TaskId = id
	reply.Task = "reduce"
}

func (c *Coordinator) MarkTaskCompleted(args *MarkTaskCompletedArgs, reply *MarkTaskCompletedReply) error {
	c.tasksMu.Lock()
	defer c.tasksMu.Unlock()

	if info, ok := c.taskStatus[args.TaskId]; ok {
		info.Status = TaskCompleted
		c.taskStatus[args.TaskId] = info
	}
	checkAllMapTasksComplete(c)
	return nil
}

func (c *Coordinator) GetTaskStatus(taskID int) (TaskStatus, bool) {
	c.tasksMu.Lock()
	defer c.tasksMu.Unlock()

	info, ok := c.taskStatus[taskID]
	if !ok {
		return TaskInProgress, false
	}
	return info.Status, true
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
func MakeCoordinator(files []string, nReducer int) *Coordinator {
	c := Coordinator{
		nReducer:          nReducer,
		isDone:            false,
		isMapDone:         false,
		workerAssignments: make(map[int]time.Time),
	}
	c.tasksMu.Lock()
	defer c.tasksMu.Unlock()

	partitionInputToTasks(files, &c)

	c.server()
	return &c
}

func partitionInputToTasks(files []string, c *Coordinator) {
	// Iterate over the files and create a MapTask for each
	taskStatus := make(map[int]TaskInfo)
	for idx, fileName := range files {
		mapTask := MapTask{FileName: fileName}
		taskStatus[idx] = TaskInfo{Type: "map", Status: TaskNotStarted, Map: mapTask}
	}
	c.taskStatus = taskStatus
	c.nFiles = len(taskStatus)
}

func checkAllMapTasksComplete(c *Coordinator) {
	for _, info := range c.taskStatus {
		if info.Type == "map" && info.Status != TaskCompleted {
			return
		}
	}
	c.isMapDone = true
	log.Printf("All map tasks are completed.")
}
