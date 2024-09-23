package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var (
	mu sync.Mutex
)

type Coordinator struct {
	// Your definitions here.
	ReduceNum         int
	TaskId            int   // 用于生成task的特殊id
	DistPhase         Phase // 目前整个框架应该处于什么任务阶段
	TaskChannelReduce chan *Task
	TaskChannelMap    chan *Task
	taskMetaHolder    TaskMetaHolder
	files             []string // 传入的文件数组
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:             files,
		ReduceNum:         nReduce,
		DistPhase:         MapPhase,
		TaskChannelMap:    make(chan *Task, len(files)),
		TaskChannelReduce: make(chan *Task, nReduce),
		taskMetaHolder: TaskMetaHolder{
			MetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce),
		},
	}

	// Your code here.
	c.makeMapTasks(files)
	c.server()

	go c.CrashDetector()
	return &c
}

func (c *Coordinator) CrashDetector() {
	for {
		time.Sleep(time.Second * 2)
		mu.Lock()
		if c.DistPhase == AllDone {
			mu.Unlock()
			break
		}

		for _, v := range c.taskMetaHolder.MetaMap {
			if v.state == Working {
				//fmt.Println("task[", v.TaskAdr.TaskId, "] is working: ", time.Since(v.StartTime), "s")
			}

			if v.state == Working && time.Since(v.StartTime) > 9*time.Second {
				fmt.Printf("the task[ %d ] is crash,take [%d] s\n", v.TaskAdr.TaskId, time.Since(v.StartTime))

				switch v.TaskAdr.TaskType {
				case MapTask:
					c.TaskChannelMap <- v.TaskAdr
					v.state = Waiting
				case ReduceTask:
					c.TaskChannelReduce <- v.TaskAdr
					v.state = Waiting

				}
			}
		}
		mu.Unlock()
	}

}

type TaskMetaInfo struct {
	state     State
	TaskAdr   *Task
	StartTime time.Time
}

// TaskMetaHolder 保存全部任务的元数据
type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo
}

// 将taskMetaInfo放到TaskMetaHolder
func (t *TaskMetaHolder) acceptMeta(taskInfo *TaskMetaInfo) bool {
	taskId := taskInfo.TaskAdr.TaskId
	meta, _ := t.MetaMap[taskId]
	if meta != nil {
		//fmt.Println("meta contains task which id = ", taskId)
		return false
	} else {
		t.MetaMap[taskId] = taskInfo
	}
	return true
}

// 判断给定任务是否在工作，并修正其目前任务信息状态,如果任务不在工作的话返回true
func (t *TaskMetaHolder) judgeState(taskId int) bool {
	taskInfo, ok := t.MetaMap[taskId]
	if !ok || taskInfo.state != Waiting {
		return false
	}
	taskInfo.state = Working
	taskInfo.StartTime = time.Now()
	return true
}

func (t *TaskMetaHolder) checkTaskDone() bool {
	var (
		mapDoneNum      = 0
		mapUnDoneNum    = 0
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)
	for _, info := range t.MetaMap {
		if info.TaskAdr.TaskType == MapTask {
			if info.state == Done {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		} else if info.TaskAdr.TaskType == ReduceTask {
			if info.state == Done {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}
	}

	if (mapDoneNum > 0 && mapUnDoneNum == 0) &&
		(reduceDoneNum == 0 && reduceUnDoneNum == 0) {
		return true
	} else {
		if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
			return true
		}
	}
	return false
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) generateTaskId() int {
	res := c.TaskId
	c.TaskId++
	return res
}

func (c *Coordinator) makeMapTasks(files []string) {
	for _, v := range files {
		id := c.generateTaskId()
		task := Task{
			TaskType:  MapTask,
			TaskId:    id,
			ReduceNum: c.ReduceNum,
			Files:     []string{v},
		}

		taskMetaInfo := TaskMetaInfo{
			state:   Waiting,
			TaskAdr: &task,
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)

		//fmt.Println("make a map task:", &task)
		c.TaskChannelMap <- &task
	}
}

func (c *Coordinator) makeReduceTasks() {
	for i := range c.ReduceNum {
		id := c.generateTaskId()
		task := Task{
			TaskType: ReduceTask,
			TaskId:   id,
			Files:    selectReduceName(i),
		}

		taskMetaInfo := TaskMetaInfo{
			state:   Waiting,
			TaskAdr: &task,
		}

		c.taskMetaHolder.acceptMeta(&taskMetaInfo)

		//fmt.Println("make a reduce task:", &task)
		c.TaskChannelReduce <- &task
	}
}

func selectReduceName(reduceNum int) []string {
	var s []string
	dir, _ := os.Getwd()
	files, _ := ioutil.ReadDir(dir)
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "mr-tmp") &&
			strings.HasSuffix(file.Name(), strconv.Itoa(reduceNum)) {
			s = append(s, file.Name())
		}
	}
	return s
}

func (c *Coordinator) toNextPhase() {
	if c.DistPhase == MapPhase {
		c.makeReduceTasks()
		c.DistPhase = ReducePhase
	} else if c.DistPhase == ReducePhase {
		c.DistPhase = AllDone
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// PollTask rpc方法，用于分配任务
func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()

	switch c.DistPhase {
	case MapPhase:
		if len(c.TaskChannelMap) > 0 {
			*reply = *<-c.TaskChannelMap
			if !c.taskMetaHolder.judgeState(reply.TaskId) {
				//fmt.Printf("taskid[ %d ] is running\n", reply.TaskId)
			}
		} else {
			// map 被分配完了
			reply.TaskType = WaitingTask
			if c.taskMetaHolder.checkTaskDone() {
				c.toNextPhase()
			}
		}
		break
	case ReducePhase:
		if len(c.TaskChannelReduce) > 0 {
			*reply = *<-c.TaskChannelReduce
			if !c.taskMetaHolder.judgeState(reply.TaskId) {
				//fmt.Printf("taskid[ %d ] is running\n", reply.TaskId)
			}
		} else {
			reply.TaskType = WaitingTask
			if c.taskMetaHolder.checkTaskDone() {
				c.toNextPhase()
			}
		}
		break
	case AllDone:
		reply.TaskType = ExitTask
		break
	default:
		panic("the phase undefined")
	}

	return nil
}

// MarkFinished rpc方法 用于通知任务完成
func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()

	switch args.TaskType {
	case MapTask:
		meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]
		if ok && meta.state == Working {
			meta.state = Done
			//fmt.Printf("Map task Id[ %d ] is finished\n", args.TaskId)
		} else {
			//fmt.Printf("Map task Id[ %d ] is finished, already\n", args.TaskId)
		}
	case ReduceTask:
		meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]
		if ok && meta.state == Working {
			meta.state = Done
			//fmt.Printf("Reduce task Id[ %d ] is finished\n", args.TaskId)
		} else {
			//fmt.Printf("Reduce task Id[ %d ] is finished, already\n", args.TaskId)
		}
	default:
		panic("unhandled default case")

	}
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
	mu.Lock()
	defer mu.Unlock()
	if c.DistPhase == AllDone {
		return true
	} else {
		return false
	}
}
