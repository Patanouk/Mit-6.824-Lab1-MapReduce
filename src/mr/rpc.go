package mr

import "os"
import "strconv"

type TaskRequest struct {
}

type TaskResponse struct {
	TaskType   TaskType
	TaskNumber int
	FileName   string
	NReduce    int
	//This field is only populated in case of a reduce task
	ReduceFileList []string
}

type TaskCompletedRequest struct {
	TaskType   TaskType
	TaskNumber int
	//This field is only populated in case of a map task response
	ReduceFiles []string
}

type TaskCompletedResponse struct {
}

type TaskType int

const (
	Map TaskType = 1 << iota
	Reduce
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
