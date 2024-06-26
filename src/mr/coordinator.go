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
	ReducerIndex int
}

type TaskStatus int

const (
	TaskNotStarted TaskStatus = iota
	TaskQueued
	TaskInProgress
	TaskCompleted
)

type TaskInfo struct {
	TaskId int
	Status TaskStatus
	Type   string // "map" or "reduce"
	Map    MapTask
	Reduce ReduceTask
}

type Coordinator struct {
	nReducer          int
	nFiles            int
	reduceIsDone      bool
	mapIsDone         bool
	tasksMx           sync.Mutex
	reduceSignal      chan struct{}
	allTasks          map[int]TaskInfo
	workerAssignments map[int]time.Time
}

var taskAvailableChan = make(chan TaskInfo, 10)

// RPC handlers for the worker to call.

// RPC call for worker to request a task
func (c *Coordinator) GiveTask(args *GiveTaskArgs, reply *GiveTaskReply) error {
	taskInfo := <-taskAvailableChan

	c.tasksMx.Lock()
	defer c.tasksMx.Unlock()

	id := taskInfo.TaskId

	switch taskInfo.Type {
	case "map":
		c.AssignTask(reply, id, taskInfo, "map")
	case "reduce":
		c.AssignTask(reply, id, taskInfo, "reduce")
	}

	return nil
}

// Assigns a map task to a worker by editing the reply
func (c *Coordinator) AssignTask(reply *GiveTaskReply, id int, info TaskInfo, taskType string) {
	if info.Type == "map" {
		reply.File = info.Map.FileName
	}
	reply.NReducer = c.nReducer
	reply.NFiles = c.nFiles
	reply.TaskId = id
	reply.Task = taskType

	info.Status = TaskInProgress
	c.allTasks[id] = info
	c.workerAssignments[id] = time.Now()
}

// RPC Call to mark a task as completed
func (c *Coordinator) MarkTaskCompleted(args *MarkTaskCompletedArgs, reply *MarkTaskCompletedReply) error {
	c.tasksMx.Lock()
	defer c.tasksMx.Unlock()

	if info, ok := c.allTasks[args.TaskId]; ok {
		info.Status = TaskCompleted
		c.allTasks[args.TaskId] = info
		delete(c.workerAssignments, args.TaskId)
	}
	if !c.mapIsDone {
		checkAllMapTasksComplete(c)
	} else {
		checkAllReduceTasksComplete(c)
	}

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
	c.tasksMx.Lock()
	defer c.tasksMx.Unlock()
	return c.mapIsDone && c.reduceIsDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReducer int) *Coordinator {
	c := Coordinator{
		nReducer:          nReducer,
		reduceIsDone:      false,
		mapIsDone:         false,
		workerAssignments: make(map[int]time.Time),
		reduceSignal:      make(chan struct{}),
	}
	c.tasksMx.Lock()
	partitionInputToMapTasks(files, &c)
	c.tasksMx.Unlock()

	go c.checkAllTasksAndUpdateQueue()
	go c.createReduceTasks()
	go c.checkForWorkerTimeout()

	c.server()
	return &c
}

func (c *Coordinator) checkAllTasksAndUpdateQueue() {
	for {
		c.tasksMx.Lock()
		if c.mapIsDone && c.reduceIsDone {
			defer c.tasksMx.Unlock()
			return
		}

		for _, info := range c.allTasks {
			if !c.mapIsDone && info.Status == TaskNotStarted && info.Type == "map" {
				c.addTaskToQueue(info)
			}
			if c.mapIsDone && !c.reduceIsDone && info.Status == TaskNotStarted && info.Type == "reduce" {
				c.addTaskToQueue(info)
			}
		}
		c.tasksMx.Unlock()
		time.Sleep(300 * time.Millisecond)
	}
}

func (c *Coordinator) addTaskToQueue(info TaskInfo) {
	// Add the task to the channel
	select {
	case taskAvailableChan <- info:
		info.Status = TaskQueued
		c.allTasks[info.TaskId] = info
	default:
		// Channel is full, do nothing and continue
	}
}

func (c *Coordinator) createReduceTasks() {
	<-c.reduceSignal

	c.tasksMx.Lock()
	defer c.tasksMx.Unlock()

	for i := 0; i < c.nReducer; i++ {
		reduceTask := ReduceTask{ReducerIndex: i}
		newTaskInfo := TaskInfo{
			TaskId: i,
			Status: TaskNotStarted,
			Type:   "reduce",
			Reduce: reduceTask,
		}

		c.allTasks[i] = newTaskInfo
	}
	return
}

func (c *Coordinator) checkForWorkerTimeout() {
	for {
		c.tasksMx.Lock()
		if c.mapIsDone && c.reduceIsDone {
			defer c.tasksMx.Unlock()
			return
		}

		// Iterate over worker assignments and check for timeouts
		for taskID, startTime := range c.workerAssignments {
			if time.Since(startTime).Seconds() > 10 {
				// Timeout detected, requeue the task and remove assignment
				info, ok := c.allTasks[taskID]
				if ok {
					info.Status = TaskNotStarted
					c.allTasks[taskID] = info
				}
				delete(c.workerAssignments, taskID)
			}
		}

		c.tasksMx.Unlock()
		time.Sleep(300 * time.Millisecond)
	}
}

func partitionInputToMapTasks(files []string, c *Coordinator) {
	// Iterate over the files and create a MapTask for each
	taskStatus := make(map[int]TaskInfo)
	for idx, fileName := range files {
		mapTask := MapTask{FileName: fileName}
		taskStatus[idx] = TaskInfo{TaskId: idx, Type: "map", Status: TaskNotStarted, Map: mapTask}
	}
	c.allTasks = taskStatus
	c.nFiles = len(taskStatus)
}

func checkAllMapTasksComplete(c *Coordinator) {
	for _, info := range c.allTasks {
		if info.Type == "map" && info.Status != TaskCompleted {
			return
		}
	}
	c.mapIsDone = true
	close(c.reduceSignal)
	// log.Printf("All map tasks are completed.")
}

func checkAllReduceTasksComplete(c *Coordinator) {
	for _, info := range c.allTasks {
		if info.Type == "reduce" && info.Status != TaskCompleted {
			return
		}
	}
	c.reduceIsDone = true
	// log.Printf("All reduce tasks are completed.")
}
