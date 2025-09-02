package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type TaskRequestArgs struct {
	ID int
}

type TaskRequestReply struct {
	TaskType          string // "map" or "reduce" or "wait" or "exit"
	TaskID            string
	InputFile         string   // for map tasks
	IntermediateFiles []string // for reduce tasks
	NReduce           int
}

type TaskDoneArgs struct {
	TaskID string
}

type TaskDoneReply struct {
	Ack bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
