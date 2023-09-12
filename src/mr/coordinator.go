package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	mapTasks    []MapTask
	reduceTasks []ReduceTask

	lock *sync.RWMutex
}

type ReduceTask struct {
	fileNames []string
	status    TaskStatus
}

type MapTask struct {
	fileName string
	status   TaskStatus
	nReduce  int
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
		c.lock.Lock()
		task, taskNumber, found := c.searchForMapTask()
		if found {
			reply.TaskNumber = taskNumber
			reply.FileName = task.fileName
			reply.NReduce = task.nReduce
			reply.TaskType = Map

			c.mapTasks[taskNumber].status = InProgress

			return nil
		} else {
			log.Printf("No new task to give. Will search again in one second")
			time.Sleep(time.Second)
		}
	}
}

func (c *Coordinator) searchForMapTask() (task *MapTask, taskNumber int, found bool) {
	for i, task := range c.mapTasks {
		if task.status == Idle {
			log.Printf("Sending map task %v to a worker", task)
			return &task, i, true
		}
	}

	return nil, 0, false
}

func (c *Coordinator) MarkTaskAsCompleted(args *TaskCompletedRequest, reply *TaskCompletedResponse) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	switch args.TaskType {
	case Map:
		log.Printf("Marking map task %v as completed", args.TaskNumber)
		c.mapTasks[args.TaskNumber].status = Completed
		for i, reduceFile := range args.ReduceFiles {
			c.reduceTasks[i].fileNames = append(c.reduceTasks[i].fileNames, reduceFile)
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
	mapTasks := make([]MapTask, len(files))
	for i, file := range files {
		mapTasks[i] = MapTask{file, Idle, nReduce}
	}

	reduceTasks := make([]ReduceTask, nReduce)
	for i := 0; i < nReduce; i++ {
		reduceTasks[i] = ReduceTask{status: Idle}
	}

	log.Printf("Created %d map tasks", len(files))

	c := Coordinator{mapTasks: mapTasks, reduceTasks: reduceTasks, lock: &sync.RWMutex{}}
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
	c.lock.RLock()
	c.lock.RUnlock()

	completed := c.allMapTasksCompleted() && c.allReduceTasksCompleted()
	if completed {
		log.Printf("All tasks completed. Shutting down coordinator")
	}

	return completed
}

func (c *Coordinator) allMapTasksCompleted() bool {
	for _, value := range c.mapTasks {
		if value.status != Completed {
			return false
		}
	}

	return true
}

func (c *Coordinator) allReduceTasksCompleted() bool {
	for _, value := range c.reduceTasks {
		if value.status != Completed {
			return false
		}
	}

	return true
}
