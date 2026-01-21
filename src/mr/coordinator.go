package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type phase_type string

type Coordinator struct {
	// Your definitions here.
	task_phase phase_type

	map_time   []time.Time
	map_status []int //0 for free, 1 for running, 2 for complete
	map_paths  []string

	reduce_time   []time.Time
	reduce_status []int //0 for free, 1 for running, 2 for complete
	reduce_paths  []string

	rest_send_counts int
	rest_comp_counts int
	nReduce          int
	nMap             int
	mutex            sync.Mutex

	task_done bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RPC_handler(args *Task_Args, replies *Task_Replies) error {

	if args.Req_type == "Task" {
		c.mutex.Lock()
		replies.NReduce = c.nReduce
		if c.task_phase == "Map" {
			for i, r := range c.map_time {
				if time.Since(r) > 10*time.Second && c.map_status[i] == 1 {
					c.rest_send_counts++
					c.map_status[i] = 0
				}
			}
		} else {
			for i, r := range c.reduce_time {
				if time.Since(r) > 10*time.Second && c.reduce_status[i] == 1 {
					c.rest_send_counts++
					c.reduce_status[i] = 0
				}
			}
		}

		if c.rest_send_counts > 0 {

			if c.task_phase == "Map" {
				replies.Task_type = "Map"
				for i, status := range c.map_status {
					if status == 0 {
						replies.Read_path = c.map_paths[i]
						c.map_status[i] = 1
						c.map_time[i] = time.Now()
						replies.Task_id = i
						break
					}
				}
			} else {
				replies.Task_type = "Reduce"
				replies.Reduce_path = make([]string, c.nMap)
				for i, status := range c.reduce_status {
					if status == 0 {
						replies.Task_id = i
						c.reduce_status[i] = 1
						c.reduce_time[i] = time.Now()
						break
					}
				}
				for i := 0; i < c.nReduce; i++ {
					index := (i*c.nReduce + replies.Task_id)
					if index < len(c.reduce_paths) {
						replies.Reduce_path[i] = c.reduce_paths[index]
						//fmt.Printf("assign %v\n", replies.reduce_path[i])
					}
				}
			}
			c.rest_send_counts--

		} else {
			replies.Task_type = "Wait"
		}

		c.mutex.Unlock()

	} else if args.Req_type == "OK" {
		replies.Task_type = "OK"
		c.mutex.Lock()
		defer c.mutex.Unlock()
		if c.task_phase == "Map" {

			for i, k := range args.Result_path {
				c.reduce_paths[args.Task_id*c.nReduce+i] = k
			}
			if c.map_status[args.Task_id] == 2 {

				return nil
			}
			c.map_status[args.Task_id] = 2
			c.rest_comp_counts--
			if c.rest_comp_counts <= 0 {

				c.rest_comp_counts = c.nReduce
				c.rest_send_counts = c.nReduce
				c.task_phase = "Reduce"

			}

		} else if c.task_phase == "Reduce" {

			if c.reduce_status[args.Task_id] == 2 {

				return nil
			}
			c.reduce_status[args.Task_id] = 2
			c.rest_comp_counts--
			if c.rest_comp_counts <= 0 {
				c.task_done = true
				replies.Task_type = "Done"
			}

		}
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.task_done == true {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.nMap = len(files)
	c.map_paths = files
	c.map_status = make([]int, len(files))
	c.map_time = make([]time.Time, len(files))

	c.reduce_paths = make([]string, len(files)*nReduce)
	c.reduce_status = make([]int, nReduce)
	c.reduce_time = make([]time.Time, nReduce)

	for i := 0; i < len(c.map_paths); i++ {
		c.map_status[i] = 0
		c.map_time[i] = time.Now()
	}
	for i := 0; i < nReduce; i++ {
		c.reduce_status[i] = 0
		c.reduce_time[i] = time.Now()
	}

	c.rest_comp_counts = len(files)
	c.rest_send_counts = len(files)
	c.task_phase = "Map"

	c.task_done = false

	c.server()
	return &c
}
