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

const (
	Map = iota
	Reduce
	Wait
	End
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type Task struct {
	Id       int    // 任务id
	Type     int    // 任务类型
	FileName string // 文件名
	Nreduce  int    // reduce任务数
}

type TaskReq struct {
	Success bool
}

type TaskReply struct { // 没什么吊用，单纯填它的参数
	Success bool
}

type FinishedReq struct {
	Id int // 任务id
}

type Reply struct { // 没什么吊用，单纯填它的参数
	Success bool
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
