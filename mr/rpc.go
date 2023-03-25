package mr

import (
	"os"
	"strconv"
)

// Input empty argument for RPC definition
type EmptyArgs struct {
}

// Return empty reply for RPC call
type EmptyReply struct {
	MapCompleted    bool //map tasks done
	ReduceCompleted bool //reduce tasks done
}

type MapsEmptyReply struct {
	MapsDone bool
}

type ReduceEmptyReply struct {
	ReduceDone bool
}

// Defining map tasks - 1) take in filename 2) number of reduce tasks
type MapTask struct {
	FileName    string
	NumReduce   int
	TaskCounter int
}

type ReduceTask struct {
	IntermediateFName []string
	NumReduce         int
	PartitionNum      int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
