package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)
import "log"
import "net/rpc"
import "hash/fnv"

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for true {
		log.Printf("Requesting for a task")
		task := RequestTask()

		switch task.TaskType {
		case Map:
			reduceFiles := runMapTask(task, mapf)
			MarkMapTaskAsCompleted(task, reduceFiles)

		case Reduce:
			log.Printf("Received reduce task %v", task)
			runReduceTask(task, reducef)
			MarkReduceTaskAsCompleted(task)
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

func MarkMapTaskAsCompleted(task *TaskResponse, reduceFiles []string) {
	request := TaskCompletedRequest{task.TaskType, task.TaskNumber, reduceFiles}
	reply := TaskCompletedResponse{}
	call("Coordinator.MarkTaskAsCompleted", &request, &reply)
}

func MarkReduceTaskAsCompleted(task *TaskResponse) {
	request := TaskCompletedRequest{TaskType: task.TaskType, TaskNumber: task.TaskNumber}
	reply := TaskCompletedResponse{}
	call("Coordinator.MarkTaskAsCompleted", &request, &reply)
}

func runReduceTask(task *TaskResponse, reducef func(string, []string) string) string {
	accumulatedResults := make(map[string][]string)
	reduceResult := make(map[string]string)

	for _, reduceFileName := range task.ReduceFileList {
		reduceFile, err := os.Open(reduceFileName)
		if err != nil {
			log.Fatalf("Failed to open file %v, err %v", reduceFileName, reduceFile)
		}

		scanner := bufio.NewScanner(reduceFile)
		for scanner.Scan() {
			var keyValue KeyValue
			text := scanner.Text()
			err := json.Unmarshal([]byte(text), &keyValue)
			if err != nil {
				log.Fatalf("Failed to unmarshal line %v to KeyValue", text)
			}

			accumulatedResults[keyValue.Key] = append(accumulatedResults[keyValue.Key], keyValue.Value)
			reduceResult[keyValue.Key] = reducef(keyValue.Key, accumulatedResults[keyValue.Key])
		}

		reduceFile.Close()
	}

	return writeReduceResultToFile(task.TaskNumber, reduceResult)
}

func writeReduceResultToFile(number int, reduceResult map[string]string) string {
	file, err := os.Open(fmt.Sprintf("mr-out-%d", number))
	if err != nil {
		log.Fatalf("Failed to open file %v, err %v", file, err)
	}

	for key, value := range reduceResult {
		_, err := file.Write([]byte(fmt.Sprintf("%v %v\n", key, value)))
		if err != nil {
			log.Fatalf("Failed to write to file %v", file)
		}
	}

	return file.Name()
}

func runMapTask(task *TaskResponse, mapf func(string, string) []KeyValue) []string {
	log.Printf("Starting map task for file %v", task.FileName)

	fileContent := readFileContent(task.FileName)
	keyValues := mapf(task.FileName, string(fileContent))
	partionedKeyValues := partition(keyValues, task.NReduce)

	return writeToFiles(partionedKeyValues, task.TaskNumber)
}

func writeToFiles(partionedKeyValues [][]KeyValue, taskNumber int) []string {
	reduceFiles := make([]string, len(partionedKeyValues))

	for i, keyValues := range partionedKeyValues {
		tempFileName := fmt.Sprintf("mr-%d-%d-temp", taskNumber, i)
		log.Printf("Writing to temp file %v", tempFileName)
		err := writeKeyValuesToTempFile(tempFileName, keyValues)
		if err != nil {
			log.Fatalf("Failed to sync file %v to disk", tempFileName)
		}

		//Rename the file to make sure the end file has the full content of the map task
		reduceFileName := fmt.Sprintf("mr-%d-%d", taskNumber, i)
		err = os.Rename(tempFileName, reduceFileName)
		reduceFiles[i] = reduceFileName
		if err != nil {
			log.Fatalf("Failed to rename file %v", tempFileName)
		}
	}

	return reduceFiles
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
