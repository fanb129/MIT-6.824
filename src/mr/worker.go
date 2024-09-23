package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type SortedKey []KeyValue

func (k SortedKey) Len() int {
	return len(k)
}

func (k SortedKey) Swap(i, j int) {
	k[i], k[j] = k[j], k[i]
}

func (k SortedKey) Less(i, j int) bool {
	return k[i].Key < k[j].Key
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.
	flag := true
	for flag {
		task := GetTask()
		switch task.TaskType {
		case MapTask:
			{
				DoMapTask(mapf, &task)
				callDone(&task)
			}
		case ReduceTask:
			{
				DoReduceTask(reducef, &task)
				callDone(&task)
			}
		case WaitingTask:
			{
				//fmt.Println("waiting...")
				time.Sleep(5 * time.Second)
			}
		case ExitTask:
			{
				//fmt.Println("Task[", task.TaskId, "] is terminated...")
				flag = false
			}
		default:
			panic("unhandled default case")
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

//goland:noinspection GoDeprecation
func DoMapTask(mapf func(string, string) []KeyValue, response *Task) {
	var interMediate []KeyValue
	filename := response.Files[0]

	// read file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// map
	interMediate = mapf(filename, string(content))

	rn := response.ReduceNum
	HashKV := make([][]KeyValue, rn)

	// merge kv
	for _, kv := range interMediate {
		k := ihash(kv.Key) % rn
		HashKV[k] = append(HashKV[k], kv)
	}
	// write file
	for i := range rn {
		oname := "mr-tmp-" + strconv.Itoa(response.TaskId) + "-" + strconv.Itoa(i)
		ofile, err := os.Create(oname)
		if err != nil {
			log.Fatalf("cannot create %v", oname)
		}
		encoder := json.NewEncoder(ofile)
		for _, kv := range HashKV[i] {
			encoder.Encode(kv)
		}
		ofile.Close()
	}
}

func DoReduceTask(reducef func(string, []string) string, task *Task) {
	id := task.TaskId
	intermediate := shuffle(task.Files)
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		//fmt.Println("Failed to create temp File", err)
	}
	// merge
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	fn := fmt.Sprintf("mr-out-%d", id)
	os.Rename(tempFile.Name(), fn)
}

func shuffle(files []string) []KeyValue {
	var kvs []KeyValue
	for _, file := range files {
		open, _ := os.Open(file)
		decoder := json.NewDecoder(open)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		open.Close()
	}
	sort.Sort(SortedKey(kvs))
	return kvs
}
func callDone(args *Task) Task {
	reply := Task{}
	if ok := call("Coordinator.MarkFinished", args, &reply); ok {

	} else {
		//fmt.Println("call failed")
	}
	return reply
}

func GetTask() Task {
	args := TaskArgs{}
	reply := Task{}
	if ok := call("Coordinator.PollTask", &args, &reply); ok {
		//fmt.Println(reply)
	} else {
		//fmt.Println("call failed")
	}
	return reply
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	//fmt.Printf("reply.Y %v\n", reply.Y)
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

	//fmt.Println(err)
	return false
}
