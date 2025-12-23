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

const (
	MapStatus = iota
	ReduceStatus
	Finished
)

type Coordinator struct {
	// Your definitions here.
	Status         int          // 当前系统状态
	MapChan        chan *Task   // map任务通道
	ReduceChan     chan *Task   // reduce任务通道
	Nmap           int          // map任务数(实际就是原文本文件数)
	MapCount       int          // 已完成的map任务数
	Nreduce        int          // reduce任务数
	ReduceCount    int          // 已完成的reduce任务数
	Mutex          sync.Mutex   // 经典大锁
	MapAssign      map[int]bool // map任务是否被分配(taskId -> bool)
	MapFinished    map[int]bool // map任务是否完成(taskId -> bool)
	ReduceAssign   map[int]bool // reduce任务是否被分配(taskId -> bool)
	ReduceFinished map[int]bool // reduce任务是否完成(taskId -> bool)
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignTask(args *TaskReq, reply *Task) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	switch c.Status {
	case MapStatus: // 处于map状态
		if len(c.MapChan) > 0 {
			tmp := <-c.MapChan
			*reply = *tmp
			c.MapAssign[reply.Id] = true
			go c.timeoutHandler(reply)
		} else {
			reply.Type = Wait // 无任务可分配，等待
		}

	case ReduceStatus: // 处于reduce状态
		if len(c.ReduceChan) > 0 {
			tmp := <-c.ReduceChan
			*reply = *tmp
			c.ReduceAssign[reply.Id] = true
			go c.timeoutHandler(reply)
		} else {
			reply.Type = Wait // 无任务可分配，等待
		}

	case Finished: // 整个系统完成
		reply.Type = End
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
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	ret := c.Status == Finished

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Status:         MapStatus,
		MapChan:        make(chan *Task, len(files)),
		ReduceChan:     make(chan *Task, nReduce),
		Nmap:           len(files),
		MapCount:       0,
		Nreduce:        nReduce,
		ReduceCount:    0,
		MapAssign:      make(map[int]bool),
		ReduceAssign:   make(map[int]bool),
		MapFinished:    make(map[int]bool),
		ReduceFinished: make(map[int]bool),
	}

	// Your code here.
	c.createMapTasks(files) // 创建map任务

	c.server()
	return &c
}

func (c *Coordinator) createMapTasks(files []string) {
	for i, file := range files {
		mapTask := Task{
			Id:       i,
			Type:     Map,
			FileName: file,
			Nreduce:  c.Nreduce,
		}
		c.MapChan <- &mapTask
		c.MapAssign[i] = false   // 标记map任务未分配
		c.MapFinished[i] = false // 标记map任务未完成
	}
}

func (c *Coordinator) createReduceTasks() {
	for i := 0; i < c.Nreduce; i++ {
		reduceTask := Task{
			Id:      i,
			Type:    Reduce,
			Nreduce: c.Nreduce,
		}
		c.ReduceChan <- &reduceTask
		c.ReduceAssign[i] = false   // 标记reduce任务未分配
		c.ReduceFinished[i] = false // 标记reduce任务未完成
	}
}

func (c *Coordinator) TaskFinishHandler(req *FinishedReq, reply *Reply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	switch c.Status {
	case MapStatus:
		if c.MapFinished[req.Id] {
			reply.Success = false
		} else {
			c.MapFinished[req.Id] = true
			reply.Success = true
			c.MapCount++
			if c.MapCount == c.Nmap {
				// 所有 Map 完成，进入 Reduce 阶段
				c.Status = ReduceStatus
				c.createReduceTasks()
			}
		}
	case ReduceStatus:
		if c.ReduceFinished[req.Id] {
			reply.Success = false
		} else {
			c.ReduceFinished[req.Id] = true
			reply.Success = true
			c.ReduceCount++
			if c.ReduceCount == c.Nreduce {
				c.Status = Finished
			}
		}
	default:
		reply.Success = false
	}
	return nil
}

func (c *Coordinator) timeoutHandler(task *Task) {
	time.Sleep(10 * time.Second)
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	switch task.Type {
	case Map:
		// 如果该 Map 任务在超时后仍未完成，重新放回队列
		if !c.MapFinished[task.Id] {
			log.Printf("Map 任务 %d (文件 %s) 超时，重新调度\n", task.Id, task.FileName)
			c.MapChan <- task
			c.MapAssign[task.Id] = false
		}
	case Reduce:
		// 如果 Reduce 任务在超时后仍未完成，重新放回队列
		if !c.ReduceFinished[task.Id] {
			log.Printf("Reduce 任务 %d 超时，重新调度\n", task.Id)
			c.ReduceChan <- task
			c.ReduceAssign[task.Id] = false
		}
	}
}
