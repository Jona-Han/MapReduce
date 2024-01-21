package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		TaskObj := requestTask()

		switch TaskObj.Task {
		case "map":
			mapToIntermediates(TaskObj.File, mapf, TaskObj.TaskId, TaskObj.NReducer)
			markTaskComplete(TaskObj.TaskId)
		case "reduce":
			sortAndReduce(TaskObj.NReducer, TaskObj.NFiles)
		default:
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func requestTask() GiveTaskReply {
	args := GiveTaskArgs{}
	reply := GiveTaskReply{}

	ok := call("Coordinator.GiveTask", &args, &reply)

	if ok {
		fmt.Printf("Task reply -- Filename: %s\n", reply.File)
	} else {
		fmt.Printf("RequestTask call failed!\n")
	}
	return reply
}

func markTaskComplete(taskId int) {
	args := MarkTaskCompletedArgs{TaskId: taskId}
	reply := MarkTaskCompletedReply{}

	ok := call("Coordinator.MarkTaskCompleted", &args, &reply)

	if ok {
		fmt.Printf("MarkTaskComplete call success!\n")
	} else {
		fmt.Printf("MarkTaskComplete call failed!\n")
	}
}

func mapToIntermediates(fileName string, mapf func(string, string) []KeyValue, taskId int, nReducer int) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()

	intermediate := mapf(fileName, string(content))

	// Initialize intermediate partitions
	partitions := make([][]KeyValue, nReducer)
	for i := range partitions {
		partitions[i] = make([]KeyValue, 0)
	}

	// Partition the intermediate key-value pairs
	for _, kv := range intermediate {
		reducerIndex := ihash(kv.Key) % nReducer
		partitions[reducerIndex] = append(partitions[reducerIndex], kv)
	}

	// Write intermediate partitions to files
	for i, partition := range partitions {
		writePartitionToFile(taskId, i, partition)
	}
}

func writePartitionToFile(taskId int, index int, partition []KeyValue) {
	// Create a temporary file
	tmpFile, err := os.CreateTemp(".", fmt.Sprintf("mr-%d-%d-", taskId, index))
	if err != nil {
		log.Fatalf("Error creating temporary file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	// Encode and output to temporary file
	enc := json.NewEncoder(tmpFile)
	for _, kv := range partition {
		err := enc.Encode(&kv)

		if err != nil {
			log.Fatalf("Error encoding KeyValue to JSON: %v", err)
		}
	}

	// Close the temporary file
	err = tmpFile.Close()
	if err != nil {
		log.Fatalf("Error closing temporary file: %v", err)
	}

	// Atomically rename the temporary file
	newFilePath := fmt.Sprintf("../mr-%d-%d.txt", taskId, index)
	err = os.Rename(tmpFile.Name(), newFilePath)
	if err != nil {
		log.Fatalf("Error renaming file: %v", err)
	}
}

func sortAndReduce(numPartitions int, numFiles int) {
	// sort.Sort(ByKey(intermediate))

	// oname := "mr-out-0"
	// ofile, _ := os.Create(oname)

	// //
	// // call Reduce on each distinct key in intermediate[],
	// // and print the result to mr-out-0.
	// //
	// i := 0
	// for i < len(intermediate) {
	// 	j := i + 1
	// 	for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
	// 		j++
	// 	}
	// 	values := []string{}
	// 	for k := i; k < j; k++ {
	// 		values = append(values, intermediate[k].Value)
	// 	}
	// 	output := reducef(intermediate[i].Key, values)

	// 	// this is the correct format for each line of Reduce output.
	// 	fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

	// 	i = j
	// }

	// ofile.Close()
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
