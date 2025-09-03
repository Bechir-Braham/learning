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

type MapJob struct {
	MapJobInput MapJobInput
	WorkerID    int
	state       string // "started" or "done" or "failed" or "pending"
	startedAt   time.Time
}

type ReduceJob struct {
	ReduceJobInput ReduceJobInput
	WorkerID       int
	state          string // "started" or "done" or "failed" or "pending"
	startedAt      time.Time
}

type MapJobInput struct {
	FileName string
	TaskID   string
}

type ReduceJobInput struct {
	IntermediateFiles []string
	TaskID            string
}

type Master struct {
	map_jobs     []MapJob
	jobLock      sync.Mutex
	isMapDone    bool
	nReduce      int
	reduce_jobs  []ReduceJob
	isReduceDone bool
}

func (m *Master) ControlTaskRequest(args *TaskRequestArgs, reply *TaskRequestReply) error {
	if !m.isMapDone {
		return m.handleMapTaskRequest(args, reply)
	}
	if m.isMapDone && !m.isReduceDone {
		return m.handleReduceTaskRequest(args, reply)
	} else if m.isMapDone && m.isReduceDone {
		reply.TaskType = "exit"
		return nil
	}
	reply.TaskType = "wait"
	return nil
}

func (m *Master) handleReduceTaskRequest(args *TaskRequestArgs, reply *TaskRequestReply) error {
	m.jobLock.Lock()
	defer m.jobLock.Unlock()
	job := get_pending_reduce_job(m)
	if job == nil {
		reply.TaskType = "wait"
		return nil
	}
	// Assign the job to the worker
	job.state = "in-progress"
	job.WorkerID = args.ID
	job.startedAt = time.Now()

	reply.TaskType = "reduce"
	reply.TaskID = job.ReduceJobInput.TaskID
	reply.IntermediateFiles = job.ReduceJobInput.IntermediateFiles
	reply.NReduce = m.nReduce

	fmt.Printf("[%d] Assigned reduce task %s for files %v\n", args.ID, reply.TaskID, job.ReduceJobInput.IntermediateFiles)
	return nil
}

func get_pending_reduce_job(m *Master) *ReduceJob {
	for i := 0; i < len(m.reduce_jobs); i++ {
		if m.reduce_jobs[i].state == "pending" {
			return &m.reduce_jobs[i]
		}
	}
	return nil
}

func (m *Master) handleMapTaskRequest(args *TaskRequestArgs, reply *TaskRequestReply) error {
	m.jobLock.Lock()
	defer m.jobLock.Unlock()
	job := get_pending_map_job(m)
	if job == nil {
		reply.TaskType = "wait"
		return nil
	}
	// Assign the job to the worker
	job.state = "in-progress"
	job.WorkerID = args.ID
	job.startedAt = time.Now()

	reply.TaskType = "map"
	reply.TaskID = job.MapJobInput.TaskID
	reply.InputFile = job.MapJobInput.FileName
	reply.NReduce = m.nReduce

	fmt.Printf("[%d] Assigned map task %s for file %s\n", args.ID, reply.TaskID, job.MapJobInput.FileName)
	return nil
}

func get_pending_map_job(m *Master) *MapJob {
	for i := 0; i < len(m.map_jobs); i++ {
		if m.map_jobs[i].state == "pending" {
			return &m.map_jobs[i]
		}
	}
	return nil
}

func (m *Master) ControlTaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	if !m.isMapDone {
		return m.handleMapTaskDone(args, reply)
	}
	if m.isMapDone && !m.isReduceDone {
		return m.handleReduceTaskDone(args, reply)
	}
	return nil
}

func (m *Master) handleReduceTaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	m.jobLock.Lock()
	defer m.jobLock.Unlock()
	areAllJobsDone := true
	for i := range m.reduce_jobs {
		job := &m.reduce_jobs[i]
		if job.ReduceJobInput.TaskID == args.TaskID {
			job.state = "done"
			fmt.Printf("[%d] Reduce task %v completed\n", job.WorkerID, job.ReduceJobInput.TaskID)
		}
		areAllJobsDone = areAllJobsDone && (job.state == "done")
	}
	m.isReduceDone = areAllJobsDone
	reply.Ack = true
	return nil
}

func (m *Master) handleMapTaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	m.jobLock.Lock()
	defer m.jobLock.Unlock()
	areAllJobsDone := true
	for i := range m.map_jobs {
		job := &m.map_jobs[i]
		if job.MapJobInput.TaskID == args.TaskID {
			job.state = "done"
			fmt.Printf("[%d] Map task %v completed\n", job.WorkerID, job.MapJobInput.TaskID)
		}
		areAllJobsDone = areAllJobsDone && (job.state == "done")
	}
	m.isMapDone = areAllJobsDone
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
	return m.isMapDone && m.isReduceDone
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.nReduce = nReduce
	nMapJobs := len(files)
	m.isMapDone = false
	m.isReduceDone = false
	for i, file := range files {
		m.map_jobs = append(m.map_jobs,
			MapJob{
				MapJobInput: MapJobInput{FileName: file, TaskID: fmt.Sprint(i)},
				state:       "pending",
			})
	}
	fmt.Print("Collected ", len(m.map_jobs), " map jobs\n")
	for i := 0; i < nReduce; i++ {
		var intermediateFiles []string
		for j := 0; j < nMapJobs; j++ {
			intermediateFiles = append(intermediateFiles, fmt.Sprintf("mr-%d-%d.txt", j, i))
		}
		m.reduce_jobs = append(m.reduce_jobs,
			ReduceJob{
				ReduceJobInput: ReduceJobInput{
					IntermediateFiles: intermediateFiles,
					TaskID:            fmt.Sprint(i),
				},
				state: "pending",
			})
	}
	fmt.Print("Collected ", len(m.reduce_jobs), " reduce jobs\n")

	m.server()
	for {
		if m.isMapDone {
			time.Sleep(3 * time.Second)
			break
		}
		time.Sleep(100 * time.Millisecond)
		handle_timeout_map_jobs(&m)
	}
	fmt.Print("Map phase done\n")
	for {
		if m.isReduceDone {
			time.Sleep(3 * time.Second)
			break
		}
		time.Sleep(100 * time.Millisecond)
		handle_timeout_reduce_jobs(&m)
	}
	fmt.Print("Reduce phase done\n")

	return &m
}

func handle_timeout_reduce_jobs(master *Master) {
	master.jobLock.Lock()
	defer master.jobLock.Unlock()
	for i := range master.reduce_jobs {
		job := &master.reduce_jobs[i]
		if job.state == "in-progress" {
			if time.Since(job.startedAt) > 10*time.Second {
				fmt.Printf("Reduce task %s timed out. Reassigning...\n", job.ReduceJobInput.TaskID)
				job.state = "pending"
				job.WorkerID = 0
			}
		}
	}
}

func handle_timeout_map_jobs(master *Master) {
	master.jobLock.Lock()
	defer master.jobLock.Unlock()
	for i := range master.map_jobs {
		job := &master.map_jobs[i]
		if job.state == "in-progress" {
			if time.Since(job.startedAt) > 10*time.Second {
				fmt.Printf("Map task %s timed out. Reassigning...\n", job.MapJobInput.TaskID)
				job.state = "pending"
				job.WorkerID = 0
			}
		}
	}
}
