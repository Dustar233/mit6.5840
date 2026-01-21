package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type Task_Replies struct {
	Task_type string //Map, Reduce, Wait, Done
	//false for map task, true for reduce task
	Read_path   string
	Reduce_path []string
	NReduce     int
	NMap        int
	Task_id     int
}

type Task_Args struct {
	Req_type    string //Task, OK
	Task_id     int
	Result_path []string
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
