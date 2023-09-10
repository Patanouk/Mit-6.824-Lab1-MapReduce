package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	mapTasks map[string]TaskStatus
}

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

// Your code here -- RPC handlers for the worker to call.

// RequestTask an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) RequestTask(args *TaskRequest, reply *TaskResponse) error {
	for fileName, taskStatus := range c.mapTasks {
		if taskStatus == Idle {
			reply.FilePath = fileName
		}
	}

	return nil
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTasks := make(map[string]TaskStatus)
	for _, file := range files {
		mapTasks[file] = Idle
	}
	log.Printf("Created %d map tasks", len(files))

	c := Coordinator{mapTasks: mapTasks}
	c.server()
	return &c
}

//
// start a thread that listens for RPCs from worker.go
//
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

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return allTasksCompleted(c.mapTasks)
}

func allTasksCompleted(input map[string]TaskStatus) bool {
	for _, value := range input {
		if value != Completed {
			return false
		}
	}

	return true
}
