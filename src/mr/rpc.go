package mr

import "os"
import "strconv"

type TaskRequest struct {
}

type TaskResponse struct {
	TaskNumber int
	FileName   string
	NReduce    int
}

type TaskCompletedRequest struct {
	TaskNumber int
}

type TaskCompletedResponse struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
