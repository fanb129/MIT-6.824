package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

type TaskArgs struct{}

type Task struct {
	TaskType  TaskType // 判断任务类型是map还是reduce
	TaskId    int
	ReduceNum int      // 传入的reduce的数量，用于hash
	Files     []string // 输入文件
}

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitingTask
	ExitTask
)

// Phase 对于分配任务阶段
type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	AllDone
)

// State 任务状态
type State int

const (
	Working State = iota
	Waiting
	Done
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
