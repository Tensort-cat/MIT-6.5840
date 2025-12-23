package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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
		task := CallPullTask() // 请求任务

		switch task.Type {
		case Map:
			doMap(task, mapf)

		case Reduce:
			doReduce(task, reducef)

		case Wait:
			time.Sleep(5 * time.Second)

		default:
			log.Printf("a worker end\n")
			return
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func doMap(task *Task, mapf func(string, string) []KeyValue) {
	// log.Printf("map task %d start, handler fileName %s", task.Id, task.FileName)
	content, err := os.ReadFile(task.FileName)
	if err != nil {
		log.Printf("read file %s failed: %v", task.FileName, err)
		return
	}

	kvPairs := mapf(task.FileName, string(content))
	buckets := make([][]KeyValue, task.Nreduce)
	for _, kv := range kvPairs {
		rid := ihash(kv.Key) % task.Nreduce
		buckets[rid] = append(buckets[rid], kv)
	}

	prefix := task.Id
	suffix := rand.Int()
	tmpFiles := make([]string, 0)

	for rid, kvs := range buckets {
		if len(kvs) == 0 {
			continue
		}

		tmpName := fmt.Sprintf("mr-%d-%d-%d.txt", prefix, rid, suffix)
		file, err := os.Create(tmpName)
		if err != nil {
			log.Fatal(err)
		}

		writer := bufio.NewWriter(file)

		for _, kv := range kvs {
			writer.WriteString(kv.Key)
			writer.WriteByte(' ')
			writer.WriteString(kv.Value)
			writer.WriteByte('\n')
		}

		writer.Flush()
		file.Close()

		tmpFiles = append(tmpFiles, tmpName)
	}

	for _, oldName := range tmpFiles { // 让临时文件转正
		newName := oldName[:strings.LastIndex(oldName, "-")] + "-reduce.txt"
		os.Rename(oldName, newName)
	}

	// 通知coordinator完成了map任务
	req := FinishedReq{
		Id: task.Id,
	}
	reply := Reply{
		Success: true,
	}
	CallTaskFinished(&req, &reply)
}

func doReduce(task *Task, reducef func(string, []string) string) {
	/*
		1. 通过读取reduce任务文件，调用reducef函数，得到结果
		2. 将结果写mr-out-X.txt文件中，X为reduce任务ID
	*/
	files, err := filepath.Glob(fmt.Sprintf("*%d-reduce.txt", task.Id)) // 读取属于该任务的reduce任务文件
	if err != nil {
		log.Printf("Read dir failed. taskID: %d, err: %v", task.Id, err)
		return
	}

	countMap := make(map[string][]string)
	for _, fileName := range files { // 处理每一个reduce文件
		file, err := os.Open(fileName)
		if err != nil {
			log.Printf("Open file failed. fileName: %s, err: %v", fileName, err)
			return
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			params := strings.Split(line, " ")
			word, count := params[0], params[1]
			countMap[word] = append(countMap[word], count)
		}
		file.Close()
	}

	// 收集所有 key
	keys := make([]string, 0, len(countMap))
	for k := range countMap {
		keys = append(keys, k)
	}

	// 按字典序排序
	sort.Strings(keys)
	suffix := rand.Int()
	// 与map任务相同，要考虑超时情况，因此要先将数据写入临时文件，然后再根据coordinator的回复，决定是删除临时文件还是重命名为最终结果文件
	tmpFileName := fmt.Sprintf("mr-out-%d-%d.txt", task.Id, suffix)
	tmpOutFile, err := os.OpenFile(tmpFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Open file failed. fileName: %s, err: %v", tmpFileName, err)
		return
	}
	// 按排序后的顺序输出
	for _, word := range keys {
		list := countMap[word]
		wc := reducef(word, list)
		line := word + " " + wc + "\n"
		_, err = tmpOutFile.WriteString(line)
		if err != nil {
			log.Printf("Write file failed. fileName: %s, err: %v",
				tmpFileName, err)
			return
		}
	}
	tmpOutFile.Close()

	// 最后通知coordinator完成了reduce任务
	req := FinishedReq{
		Id: task.Id,
	}
	reply := Reply{
		Success: true,
	}
	// 让临时文件转正
	newName := fmt.Sprintf("mr-out-%d.txt", task.Id)
	os.Rename(tmpFileName, newName)

	// 最后通知coordinator完成了reduce任务
	CallTaskFinished(&req, &reply)

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

func CallPullTask() *Task {
	req := TaskReq{}
	reply := Task{}
	ok := call("Coordinator.AssignTask", &req, &reply)
	// 如果 RPC 失败，说明协调器已退出，worker 退出
	if !ok {
		// 设置一个特殊类型，使主循环中的 default 分支退出
		reply.Type = -1
	}
	return &reply
}

func CallTaskFinished(req *FinishedReq, reply *Reply) {
	call("Coordinator.TaskFinishHandler", req, reply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args any, reply any) bool {
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
