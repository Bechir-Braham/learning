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
	MapJobInput MapJobInput
	WorkerID    int
	StartTime   time.Time
	state       string // "in-progress" or "done" or "failed"
}

type SubmittedReduceJob struct {
	ReduceJobInput ReduceJobInput
	WorkerID       int
	StartTime      time.Time
	state          string // "in-progress" or "done" or "failed"
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
	remaining_map_jobs    []MapJobInput
	submitted_map_jobs    []SubmittedMapJob
	jobLock               sync.Mutex
	isMapDone             bool
	nReduce               int
	remaining_reduce_jobs []ReduceJobInput
	submitted_reduce_jobs []SubmittedReduceJob
	isReduceDone          bool
}

func (m *Master) ControlTaskRequest(args *TaskRequestArgs, reply *TaskRequestReply) error {
	if !m.isMapDone {
		if len(m.remaining_map_jobs) > 0 {
			m.jobLock.Lock()
			defer m.jobLock.Unlock()
			job := m.remaining_map_jobs[0]
			m.remaining_map_jobs = m.remaining_map_jobs[1:]
			reply.TaskType = "map"
			reply.TaskID = job.TaskID
			reply.InputFile = job.FileName
			reply.NReduce = m.nReduce
			m.submitted_map_jobs = append(m.submitted_map_jobs, SubmittedMapJob{
				MapJobInput: job,
				WorkerID:    args.ID,
				StartTime:   time.Now(),
				state:       "in-progress",
			})
			fmt.Printf("[%d] Assigned map task %s for file %s\n", args.ID, reply.TaskID, job.FileName)
		}
		return nil
	}
	if m.isMapDone && !m.isReduceDone {
		if len(m.remaining_reduce_jobs) > 0 {
			m.jobLock.Lock()
			defer m.jobLock.Unlock()
			job := m.remaining_reduce_jobs[0]
			m.remaining_reduce_jobs = m.remaining_reduce_jobs[1:]
			reply.TaskType = "reduce"
			reply.TaskID = job.TaskID
			reply.IntermediateFiles = job.IntermediateFiles
			m.submitted_reduce_jobs = append(m.submitted_reduce_jobs, SubmittedReduceJob{
				ReduceJobInput: job,
				WorkerID:       args.ID,
				StartTime:      time.Now(),
				state:          "in-progress",
			})
			fmt.Printf("[%d] Assigned reduce task %s\n", args.ID, reply.TaskID)
		}
		return nil
	} else if m.isMapDone && m.isReduceDone {
		reply.TaskType = "exit"
		return nil
	}

	// else
	reply.TaskType = "wait"
	return nil
}
func (m *Master) ControlTaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	m.jobLock.Lock()
	defer m.jobLock.Unlock()
	if !m.isMapDone {
		for i, job := range m.submitted_map_jobs {
			if job.MapJobInput.TaskID == args.TaskID {
				// Mark the job as done by removing it from submitted_map_jobs
				m.submitted_map_jobs = append(m.submitted_map_jobs[:i], m.submitted_map_jobs[i+1:]...)
				fmt.Printf("[%d] Map task %v completed\n", job.WorkerID, job.MapJobInput.TaskID)
				break
			}
		}
		if len(m.remaining_map_jobs) == 0 && len(m.submitted_map_jobs) == 0 {
			m.isMapDone = true
		}
		reply.Ack = true
	}
	if m.isMapDone && !m.isReduceDone {
		for i, job := range m.submitted_reduce_jobs {
			if job.ReduceJobInput.TaskID == args.TaskID {

				// Mark the job as done by removing it from submitted_reduce_jobs
				m.submitted_reduce_jobs = append(m.submitted_reduce_jobs[:i], m.submitted_reduce_jobs[i+1:]...)
				fmt.Printf("[%d] Reduce task %v completed\n", job.WorkerID, job.ReduceJobInput.TaskID)
				break
			}
		}
		if len(m.remaining_reduce_jobs) == 0 && len(m.submitted_reduce_jobs) == 0 {
			m.isReduceDone = true
		}
		reply.Ack = true
	}
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
	m.server()
	// Populate the remaining_map_jobs with the input files
	task_counter := 0
	for _, file := range files {
		m.remaining_map_jobs = append(m.remaining_map_jobs, MapJobInput{FileName: file, TaskID: fmt.Sprint(task_counter)})
		task_counter++
	}
	fmt.Print("Collected ", len(m.remaining_map_jobs), " map jobs\n")
	for i := 0; i < nReduce; i++ {
		var intermediateFiles []string
		for j := 0; j < nMapJobs; j++ {
			intermediateFiles = append(intermediateFiles, fmt.Sprintf("mr-%d-%d.txt", j, i))
		}
		m.remaining_reduce_jobs = append(m.remaining_reduce_jobs, ReduceJobInput{
			IntermediateFiles: intermediateFiles,
			TaskID:            fmt.Sprint(i),
		})
	}
	fmt.Print("Collected ", len(m.remaining_reduce_jobs), " reduce jobs\n")
	for {
		if m.isMapDone {
			time.Sleep(3 * time.Second)
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Print("Map phase done\n")
	for {
		if m.isReduceDone {
			time.Sleep(3 * time.Second)
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Print("Reduce phase done\n")

	return &m
}
