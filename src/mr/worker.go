package mr

import (
	"encoding/json"
	"fmt"
	"os"
)
import "log"
import "net/rpc"
import "hash/fnv"

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for true {
		task := RequestTask()

		if task.FileName != "" {
			runMapTask(task, mapf)
			MarkTaskAsCompleted(task.TaskNumber)
		}
	}
}

func RequestTask() *TaskResponse {
	request := TaskRequest{}
	response := TaskResponse{}
	// send the RPC request, wait for the reply.
	call("Coordinator.RequestTask", &request, &response)
	return &response
}

func MarkTaskAsCompleted(taskNumber int) {
	request := TaskCompletedRequest{taskNumber}
	reply := TaskCompletedResponse{}
	call("Coordinator.MarkTaskAsCompleted", &request, &reply)
}

func runMapTask(task *TaskResponse, mapf func(string, string) []KeyValue) {
	log.Printf("Starting map task for file %v", task.FileName)

	fileContent := readFileContent(task.FileName)
	keyValues := mapf(task.FileName, string(fileContent))
	partionedKeyValues := partition(keyValues, task.NReduce)

	writeToFiles(partionedKeyValues, task.TaskNumber)
}

func writeToFiles(partionedKeyValues [][]KeyValue, taskNumber int) {
	tempFiles := make([]string, len(partionedKeyValues))

	for i, keyValues := range partionedKeyValues {
		tempFileName := fmt.Sprintf("mr-%d-%d-temp", taskNumber, i)
		log.Printf("Writing to temp file %v", tempFileName)
		err := writeKeyValuesToTempFile(tempFileName, keyValues)
		if err != nil {
			log.Fatalf("Failed to sync file %v to disk", tempFileName)
		}
		tempFiles[i] = tempFileName

		//Rename the file to make sure the end file has the full content of the map task
		err = os.Rename(tempFileName, fmt.Sprintf("mr-%d-%d", taskNumber, i))
		if err != nil {
			log.Fatalf("Failed to rename file %v", tempFileName)
		}
	}
}

func writeKeyValuesToTempFile(tempFileName string, keyValues []KeyValue) error {
	tempFile, err := os.Create(tempFileName)
	if err != nil {
		log.Fatalf("cannot open file %v, error %v", tempFileName, err)
	}
	defer tempFile.Close()

	enc := json.NewEncoder(tempFile)

	for _, keyValue := range keyValues {
		err := enc.Encode(&keyValue)
		if err != nil {
			log.Fatalf("failed to encode kv in file %v", tempFileName)
		}
	}

	return tempFile.Sync()
}

func readFileContent(fileName string) []byte {
	content, err := os.ReadFile(fileName)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}

	return content
}

func partition(keyValues []KeyValue, nReduce int) [][]KeyValue {
	result := make([][]KeyValue, nReduce)

	for _, keyValue := range keyValues {
		partitionNumber := ihash(keyValue.Key) % nReduce
		result[partitionNumber] = append(result[partitionNumber], keyValue)
	}

	return result
}

// KeyValue
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % nReduce to choose the reduce
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
