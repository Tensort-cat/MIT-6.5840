package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

type Value struct {
	Val string
	Ver rpc.Tversion
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data map[string]Value
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		data: make(map[string]Value),
	}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, exists := kv.data[args.Key]
	if !exists {
		reply.Err = rpc.ErrNoKey
		return
	} else {
		reply.Value = value.Val
		reply.Version = value.Ver
		reply.Err = rpc.OK
	}

}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	v, exists := kv.data[args.Key]

	// 情况 1：key 不存在，但版本号不是0
	if !exists && args.Version != 0 {
		reply.Err = rpc.ErrNoKey
		return
	}

	// 情况 2：key 存在，但版本不匹配
	if exists && args.Version != v.Ver {
		reply.Err = rpc.ErrVersion
		return
	}

	// 其他情况皆能成功put
	kv.data[args.Key] = Value{
		Val: args.Value,
		Ver: args.Version + 1,
	}
	reply.Err = rpc.OK
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
