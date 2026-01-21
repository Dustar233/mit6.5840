package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type by_kv []KeyValue

func (a by_kv) Len() int           { return len(a) }
func (a by_kv) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a by_kv) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := Task_Args{}
		reply := Task_Replies{}
		args.Req_type = "Task"

		ok := call("Coordinator.RPC_handler", &args, &reply)
		if ok {
			fmt.Printf("success for task call\n")
		} else {
			fmt.Printf("call failed!\n")
		}

		if reply.Task_type == "Wait" {
			time.Sleep(1 * time.Second)
			continue
		}

		if reply.Task_type == "Map" {
			task_id := reply.Task_id
			path := reply.Read_path
			nReduce := reply.NReduce
			file, err := os.Open(path)
			if err != nil {
				log.Fatalf("cannot open %v", path)
			}
			content, err := io.ReadAll(file)
			file.Close()
			if err != nil {
				log.Fatal("cannot read %v", path)
			}

			kvs := mapf(path, string(content))

			temp_files := make([]*os.File, nReduce)
			encoders := make([]*json.Encoder, nReduce)

			for i := 0; i < nReduce; i++ {
				temp_file, err := os.CreateTemp("", fmt.Sprintf("mr-temp-%d-%d", task_id, i))
				if err != nil {
					log.Fatal("create temp: ", err)
				}
				temp_files[i] = temp_file
				encoders[i] = json.NewEncoder(temp_file)
			}

			for _, kv := range kvs {
				k := ihash(kv.Key) % nReduce
				err := encoders[k].Encode(&kv)
				if err != nil {
					log.Fatal("encode: ", err)
				}
			}

			args.Result_path = make([]string, nReduce)

			for i := 0; i < nReduce; i++ {
				temp_files[i].Close()
				name := fmt.Sprintf("mr-%d-%d", task_id, i)
				args.Result_path[i] = name
				os.Rename(temp_files[i].Name(), name)
			}

			args.Req_type = "OK"
			args.Task_id = task_id
			reply = Task_Replies{}
			ok := call("Coordinator.RPC_handler", &args, &reply)
			if ok {
				fmt.Printf("success for task call\n")
			} else {
				fmt.Printf("call failed!\n")
			}

		} else if reply.Task_type == "Reduce" {
			task_id := reply.Task_id
			path := reply.Reduce_path

			var intermediate []KeyValue

			for _, s := range path {
				filename := s
				file, err := os.Open(filename)
				if err != nil {
					fmt.Printf("file cannot open: %v", filename)
				}
				defer file.Close()

				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}

			}

			sort.Sort(by_kv(intermediate))

			oname := fmt.Sprintf("mr-out-%d", task_id)
			ofile, _ := os.Create(oname)

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				var temp_save []string
				for k := i; k < j; k++ {
					temp_save = append(temp_save, intermediate[k].Value)
				}

				result := reducef(intermediate[i].Key, temp_save)

				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, result)
				i = j
			}

			ofile.Close()

			reply = Task_Replies{}
			args.Req_type = "OK"
			args.Task_id = task_id
			ok := call("Coordinator.RPC_handler", &args, &reply)
			if ok {
				fmt.Printf("success for task call\n")
				if reply.Task_type == "Done" {
					fmt.Printf("Done!\n")
					return
				}
			} else {
				fmt.Printf("call failed!\n")
			}

		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
