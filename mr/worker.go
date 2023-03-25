package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// -----------------------------------------------------------------------------------
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// -----------------------------------------------------------------------------------
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// -----------------------------------------------------------------------------------
type WorkerF struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

// -----------------------------------------------------------------------------------
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Your worker implementation here.
	worker := WorkerF{
		mapf:    mapf,
		reducef: reducef,
	}

	worker.RequestMapTask()
	worker.RequestReduceTask()
}

// TODO: implement w.RequestReduceTask
// -----------------------------------------------------------------------------------
func (w *WorkerF) RequestMapTask() {

	for {

		checkArgs := EmptyArgs{}
		checker := MapsEmptyReply{}
		call("Coordinator.MapDone", &checkArgs, &checker)

		if checker.MapsDone {
			break
		}
		args := EmptyArgs{}
		reply := MapTask{}

		call("Coordinator.RequestMapTask", &args, &reply)
		if reply.NumReduce == -1 {
			continue
		}

		if reply.FileName == "" {
			break
		}

		file, err := os.Open(reply.FileName)
		if err != nil {
			log.Fatalf("Cannot open %v", reply.FileName)
			break
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", reply.FileName)
			break
		}

		file.Close()
		kva := w.mapf(reply.FileName, string(content))
		fmt.Println("Task Completed", reply.FileName, "completed")
		// fmt.Println(kva)

		w.MakePartition(kva, reply.NumReduce, reply.TaskCounter)
		emptyReply := EmptyReply{}
		call("Coordinator.TaskCompleted", &reply, &emptyReply)

		// fmt.Println("reply ", emptyReply.MapCompleted, " ", reply.FileName)
		if emptyReply.MapCompleted {
			break
		}
	}

}

// -----------------------------------------------------------------------------------
// function to make partitions
func (w *WorkerF) MakePartition(kva []KeyValue, NumReduce int, TaskCounter int) {
	// making a slice for the number of encoder files
	encs := make([]*json.Encoder, NumReduce)
	//open NumReduce number of encoder files
	for i := 0; i < NumReduce; i++ {
		// /Users/arkashjain/Desktop/Spring 2023/CS 350/assignment1/code/main/mr-
		file, err := os.Create("./mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(TaskCounter))

		if err != nil {
			log.Fatalf("Cannot open the file")
		}
		defer file.Close()
		encs[i] = json.NewEncoder(file)
	}

	// iterate over the key value pairs, make partitions and put them in slices
	for _, kv := range kva {
		hash := ihash(kv.Key) % NumReduce
		err := encs[hash].Encode(&kv)
		if err != nil {
			log.Fatalf("Cannot encode keyvalue pairs %v", kv)
		}
	}
}

// -----------------------------------------------------------------------------------
// function to start reducing
func (w *WorkerF) RequestReduceTask() {
	for {
		checkArgs := EmptyArgs{}
		checker := ReduceEmptyReply{}
		call("Coordinator.ReduceDone", &checkArgs, &checker)
		if checker.ReduceDone {
			break
		}
		args := EmptyArgs{}
		reply := ReduceTask{}

		call("Coordinator.RequestReduceTask", &args, &reply)

		if reply.NumReduce == 0 {
			break
		}

		kva := []KeyValue{}

		for _, fname := range reply.IntermediateFName {
			file, err := os.Open(fname)
			if err != nil {
				log.Fatalf("Error in opening the files")
				break
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
			file.Close()
		}

		sort.Sort(ByKey(kva))

		outFile, err := os.Create("./mr-" + "out-" + strconv.Itoa(reply.PartitionNum))
		if err != nil {
			log.Fatalf("Cannot create %v", outFile)
			break
		}

		i := 0
		for i < len(kva) {
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			output := w.reducef(kva[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(outFile, "%v %v\n", kva[i].Key, output)
			i = j
		}
		outFile.Close()

		emptyReply := EmptyReply{}
		call("Coordinator.ReduceCompleted", &reply, &emptyReply)
		// fmt.Println("Reduce task", emptyReply.ReduceCompleted, "done")
		if emptyReply.ReduceCompleted {
			// wg.Done()
			break
		}
	}
}

// -----------------------------------------------------------------------------------
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
