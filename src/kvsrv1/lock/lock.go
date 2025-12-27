package lock

import (
	"strings"

	kvtest "6.5840/kvtest1"
)

const (
	LOCK   = "LOCK"
	UNLOCK = "UNLOCK"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	id string // 客户端唯一标识
	l  string // 锁的名字
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck: ck,
		id: kvtest.RandValue(8),
		l:  l,
	}
	// You may add code here
	return lk
}

/*
	锁在服务端的存储格式: lockName -> "id:LOCK"
*/

func (lk *Lock) Acquire() {
	// Your code here
	lockName := lk.l
	want := lk.id + ":" + LOCK
	/*
		锁被LOCK，就不停get直到UNLOCK(某客户端释放锁)
		发现UNLOCK，尝试put锁，此时因为并发环境会导致竞争，竞争失败则继续不断get直到下次put
		第二个if中的val == ""是因为一开始服务器中没有锁，由第一个使用锁的客户端扮演创建锁的角色
		注：golang的零值真是一个伟大的设计
	*/
	for {
		val, ver, _ := lk.ck.Get(lockName)
		if val == want {
			return
		}
		if strings.HasSuffix(val, UNLOCK) || val == "" {
			lk.ck.Put(lockName, want, ver)
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	/*
		由于一个客户端的Release()一定发生在Acquire()之后, 现在锁一定是自己的
		此时Get("l")的结果一定是"id:LOCK", 仅需将其改为"id:UNLOCK"即可
		这个过程不会因为并发让锁被其他客户端抢占，他们都会卡在自己的Acquire()
	*/
	lockName := lk.l
	_, ver, _ := lk.ck.Get(lockName) // 没版本号没法put，只能先get再put
	lk.ck.Put(lockName, lk.id+":"+UNLOCK, ver)
}
