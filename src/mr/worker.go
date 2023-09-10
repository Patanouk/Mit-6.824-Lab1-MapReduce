package mr

import (
	"fmt"
	"io/ioutil"
	"os"
)
import "log"
import "net/rpc"
import "hash/fnv"

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for true {
		task := RequestTask()
		runMapTask(task.FileName, mapf)
		MarkTaskAsCompleted(task.FileName)
	}
}

func RequestTask() *TaskResponse {
	request := TaskRequest{}
	reply := TaskResponse{}
	// send the RPC request, wait for the reply.
	call("Coordinator.RequestTask", &request, &reply)
	return &reply
}

func MarkTaskAsCompleted(fileName string) {
	request := TaskCompletedRequest{fileName}
	reply := TaskCompletedResponse{}
	call("Coordinator.MarkTaskAsCompleted", &request, &reply)
}

func runMapTask(fileName string, mapf func(string, string) []KeyValue) {
	log.Printf("Starting map task for file %v", fileName)
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()

	mapf(fileName, string(content))

}

// KeyValue
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
