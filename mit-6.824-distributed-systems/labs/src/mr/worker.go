package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
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
	workerID := os.Getpid()
	fmt.Printf("Worker %d starting\n", workerID)
	for {
		time.Sleep(500 * time.Millisecond)
		task := ControlTaskRequest(workerID)
		if task.TaskType != "" {
			fmt.Printf("[%d] received %s task: %s\n", workerID, task.TaskType, task.TaskID)
		}
		if task.TaskType == "map" {
			executeMapTask(task, mapf)

			reply := ControlTaskDone(task.TaskID)
			if !reply.Ack {
				log.Fatalf("Worker %d failed to report map task %s done: %v", workerID, task.TaskID, reply)
			}
		}
		if task.TaskType == "reduce" {
			// reduce
		}
		if task.TaskType == "wait" {
			continue
		}
		if task.TaskType == "exit" {
			break
		}
		fmt.Printf("[%d] completed %s task %s\n", workerID, task.TaskType, task.TaskID)
	}
}

func executeMapTask(task *TaskRequestReply, mapf func(string, string) []KeyValue) {
	file_name := task.InputFile
	content, err := os.ReadFile(file_name)
	if err != nil {
		log.Fatalf("cannot read %v", file_name)
	}
	kva := mapf(file_name, string(content))
	for i := 0; i < task.NReduce; i++ {
		intermediateFileName := fmt.Sprintf("mr-%s-%d.txt", task.TaskID, i)
		intermediateFile, err := os.Create(intermediateFileName)
		if err != nil {
			log.Fatalf("cannot create %v", intermediateFileName)
		}
		defer intermediateFile.Close()
		encoder := json.NewEncoder(intermediateFile)
		for _, kv := range kva {
			if ihash(kv.Key)%task.NReduce == i {
				if err := encoder.Encode(kv); err != nil {
					log.Fatalf("cannot write %v", intermediateFileName)
				}
			}
		}
	}
}

func ControlTaskDone(taskID string) *TaskDoneReply {
	reply := new(TaskDoneReply)
	call("Master.ControlTaskDone", &TaskDoneArgs{TaskID: taskID}, reply)
	return reply
}

func ControlTaskRequest(workerID int) *TaskRequestReply {
	reply := new(TaskRequestReply)
	call("Master.ControlTaskRequest", &TaskRequestArgs{ID: workerID}, reply)
	return reply
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
