package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

// -----------------------------------------------------------------------------------
type Coordinator struct {
	// track partitions, number of map tasks
	// files, locking, check if the maps finish or not
	NumReduces        int             // partitions
	MapsToDo          chan MapTask    // channel to dynamically update number of maps
	Files             []string        // Read files
	CompletedTasks    map[string]bool // take files that have been mapped and add them here
	CompletedReducers map[int]bool    // track reduce tasks
	ReduceTasks       chan ReduceTask //channel for reduce task
	MapReduceDone     bool
	// MapTasksDone      string
	MapsDone       bool
	MapsDoneNumber []int
	mu             sync.Mutex //locks to prevent race
	// TaskCounter       int
}

// -----------------------------------------------------------------------------------
// function to read the files and push them onto the channel
func (c *Coordinator) Start() {
	// fmt.Println("Adding map tasks to the channel")
	// go func() {
	for counter, file := range c.Files {
		mapTask := MapTask{
			FileName:    file,
			NumReduce:   c.NumReduces,
			TaskCounter: counter,
			// CheckString: "Map",
		}
		// fmt.Println("MapTask", mapTask, " Added")
		c.MapsToDo <- mapTask
		c.CompletedTasks["map_"+mapTask.FileName] = false
	}
	// }()
	c.server()
}

// -----------------------------------------------------------------------------------
// function to call idle workers
func (c *Coordinator) RequestMapTask(args *EmptyArgs, reply *MapTask) error {
	// // fmt.Println("Request a map task from an idle worker")
	// c.mu.Lock()
	// if c.MapTasksDone == "Reduce" {
	// 	reply.CheckString = "Reduce"
	// }
	// c.mu.Unlock()

	select {

	case task := <-c.MapsToDo:
		fmt.Println("Found MapTask,", task.FileName)
		// c.TaskCounter++
		*reply = task

		go c.WaitForWorker(task)
	default:
		c.mu.Lock()
		if c.MapsDone == false {
			stallMapRequest := MapTask{
				NumReduce: -1,
			}
			*reply = stallMapRequest
		}

		c.mu.Unlock()
	}

	return nil
}

// -----------------------------------------------------------------------------------
// wait until the worker responds
func (c *Coordinator) WaitForWorker(task MapTask) {
	time.Sleep(time.Second * 10)
	c.mu.Lock()
	if !c.CompletedTasks["map_"+task.FileName] {
		fmt.Println("Still mapping,", task.FileName, "Putting it back in the Queue")
		c.MapsToDo <- task
	}
	c.mu.Unlock()
}

// -----------------------------------------------------------------------------------
// function to report a completed task
func (c *Coordinator) TaskCompleted(args *MapTask, reply *EmptyReply) error {

	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.CompletedTasks["map_"+args.FileName] {
		c.CompletedTasks["map_"+args.FileName] = true
		c.MapsDoneNumber = append(c.MapsDoneNumber, args.TaskCounter)
		fmt.Println("Task", args, "completed")
	}
	//check if all the map tasks are done or not. If they are, break out of the RequestMapTask
	// fmt.Println(c.CompletedTasks)
	for key := range c.CompletedTasks {
		if !c.CompletedTasks[key] {
			return nil
		}
	}
	reply.MapCompleted = true
	c.MapsDone = true
	// c.MapTasksDone = "Reduce"
	// args.CheckString = "Reduce"
	c.StartReducer()

	return nil
}

// -----------------------------------------------------------------------------------
// function to start reducers
func (c *Coordinator) StartReducer() {
	fmt.Println("Putting reduce tasks in a channel")

	// go func() {
	for i := 0; i < c.NumReduces; i++ {
		reduceTask := ReduceTask{}
		reduceTask.NumReduce = c.NumReduces
		reduceTask.PartitionNum = i
		for _, index := range c.MapsDoneNumber {
			reduceTask.IntermediateFName = append(reduceTask.IntermediateFName, fmt.Sprintf("./mr-"+strconv.Itoa((i))+"-"+strconv.Itoa(index)))
		}

		// add reduce tasks to the channel and set the map for every reducer to false
		c.ReduceTasks <- reduceTask
		c.CompletedReducers[i] = false
	}
	// }()
	// go c.server()
}

// -----------------------------------------------------------------------------------
// call reduce tasks and give the partition number to the worker
func (c *Coordinator) RequestReduceTask(args *EmptyArgs, reply *ReduceTask) error {
	// fmt.Println("Check if there is a reduce task")
	// if there are reduce tasks in the channel, set them to the reply of this rpc and call go routines
	// c.mu.Lock()
	// defer c.mu.Unlock()
	// if c.MapTasksDone == "Done" {
	// 	reply.CheckStringReduce = "Done"
	// }

	select {
	case task := <-c.ReduceTasks:
		*reply = task
		go c.WaitForReduceWorker(task)
	default:

	}

	return nil
}

// -----------------------------------------------------------------------------------
// Wait for workers to finish
func (c *Coordinator) WaitForReduceWorker(task ReduceTask) {
	time.Sleep(time.Second * 10)
	c.mu.Lock()
	// if the current reduce task is not done, queue the reduce task again
	if !c.CompletedReducers[task.PartitionNum] {
		fmt.Println("Reduce not done yet", task.PartitionNum)
		c.ReduceTasks <- task
	}

	c.mu.Unlock()
}

// -----------------------------------------------------------------------------------
// Check if all reducers done or not
func (c *Coordinator) ReduceCompleted(args *ReduceTask, reply *EmptyReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.CompletedReducers[args.PartitionNum] = true
	// fmt.Println("Reducer with task number ", args.PartitionNum, "done")

	for key := range c.CompletedReducers {
		if !c.CompletedReducers[key] {
			return nil
		}
	}
	reply.ReduceCompleted = true
	fmt.Println("All reduce done")
	// c.MapTasksDone = "Done"
	// args.CheckStringReduce = "Done"
	c.AllReduceDone(reply.ReduceCompleted)
	return nil
}

// -----------------------------------------------------------------------------------
// set the mapReduce function to true to exit coordinator
func (c *Coordinator) AllReduceDone(ReduceCompleted bool) {
	time.Sleep(time.Second * 5)

	if ReduceCompleted {
		c.MapReduceDone = true
	}
}

// -----------------------------------------------------------------------------------
// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	fmt.Println("Running server")
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

// -----------------------------------------------------------------------------------
func (c *Coordinator) MapDone(args *EmptyArgs, reply *MapsEmptyReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.MapsDone = c.MapsDone
	return nil
}

// -----------------------------------------------------------------------------------
func (c *Coordinator) ReduceDone(args *EmptyArgs, reply *ReduceEmptyReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.ReduceDone = c.MapReduceDone
	return nil
}

// -----------------------------------------------------------------------------------
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.MapReduceDone {
		ret = true
	}
	return ret
}

// -----------------------------------------------------------------------------------
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NumReduces:        nReduce,
		MapsToDo:          make(chan MapTask, 200),
		Files:             files,
		CompletedTasks:    make(map[string]bool),
		ReduceTasks:       make(chan ReduceTask, 100),
		CompletedReducers: make(map[int]bool),
		MapReduceDone:     false,
		MapsDone:          false,
	}

	fmt.Println("Starting coordinator")
	c.Start()
	return &c
}

//-----------------------------------------------------------------------------------
