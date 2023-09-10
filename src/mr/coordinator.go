package mr

import (
	"log"
	"time"
)
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

// RequestTask an example RPC handler.
func (c *Coordinator) RequestTask(args *TaskRequest, reply *TaskResponse) error {
	for {
		fileName, found := c.searchForNewTask()
		if found {
			reply.FileName = fileName
			return nil
		} else {
			time.Sleep(time.Second)
		}
	}
}

func (c *Coordinator) searchForNewTask() (fileName string, found bool) {
	for fileName, taskStatus := range c.mapTasks {
		if taskStatus == Idle {
			log.Printf("Sending map file %v to a worker", fileName)
			return fileName, true
		}
	}

	return "", false
}

func (c *Coordinator) MarkTaskAsCompleted(args *TaskCompletedRequest, reply *TaskCompletedResponse) error {
	log.Printf("Marking map task %v as completed", args.FilePath)
	c.mapTasks[args.FilePath] = Completed
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
	completed := allTasksCompleted(c.mapTasks)
	if completed {
		log.Printf("All tasks completed. Shutting down coordinator")
	}

	return completed
}

func allTasksCompleted(input map[string]TaskStatus) bool {
	for _, value := range input {
		if value != Completed {
			return false
		}
	}

	return true
}
