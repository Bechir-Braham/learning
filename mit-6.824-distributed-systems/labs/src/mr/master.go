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

type SubmittedMapJob struct {
	FileName  string
	TaskID    string
	WorkerID  int
	StartTime time.Time
}

type Master struct {
	remaining_map_jobs []KeyValue
	submitted_map_jobs []SubmittedMapJob
	mapLock            sync.Mutex
	nReduce            int
	isMapDone          bool
	isReduceDone       bool
}

func (m *Master) ControlTaskRequest(args *TaskRequestArgs, reply *TaskRequestReply) error {
	if m.isMapDone {
		reply.TaskType = "exit"
		return nil
	}
	if len(m.remaining_map_jobs) > 0 {
		// Assign a map task
		m.mapLock.Lock()
		defer m.mapLock.Unlock()
		job := m.remaining_map_jobs[0]
		m.remaining_map_jobs = m.remaining_map_jobs[1:]
		reply.TaskType = "map"
		reply.TaskID = job.Key
		reply.InputFile = job.Value
		reply.NReduce = m.nReduce
		m.submitted_map_jobs = append(m.submitted_map_jobs, SubmittedMapJob{
			FileName:  job.Value,
			TaskID:    reply.TaskID,
			WorkerID:  args.ID,
			StartTime: time.Now(),
		})
		fmt.Printf("[%d] Assigned map task %s for file %s\n", args.ID, reply.TaskID, job.Value)
	}
	return nil
}
func (m *Master) ControlTaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	m.mapLock.Lock()
	defer m.mapLock.Unlock()
	for i, job := range m.submitted_map_jobs {
		if job.TaskID == args.TaskID {
			// Mark the job as done by removing it from submitted_map_jobs
			m.submitted_map_jobs = append(m.submitted_map_jobs[:i], m.submitted_map_jobs[i+1:]...)
			fmt.Printf("[%d] Map task %v completed\n", job.WorkerID, job.TaskID)
			break
		}
	}
	if len(m.remaining_map_jobs) == 0 && len(m.submitted_map_jobs) == 0 {
		m.isMapDone = true
	}
	reply.Ack = true
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	return m.isMapDone
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.nReduce = nReduce
	m.server()
	// Populate the remaining_map_jobs with the input files
	for task_id, file := range files {
		m.remaining_map_jobs = append(m.remaining_map_jobs, KeyValue{Key: fmt.Sprint(task_id), Value: file})
	}
	fmt.Print("Collected ", len(m.remaining_map_jobs), " map jobs\n")
	for {
		if m.isMapDone {
			time.Sleep(3 * time.Second)
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Print("Map phase done\n")

	return &m
}
