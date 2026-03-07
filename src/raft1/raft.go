package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const (
	FOLLOWER           = "follower"
	CANDIDATE          = "candidate"
	LEADER             = "leader"
	HEARTBEAT_INTERVAL = 100 * time.Millisecond
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	role string // "follower", "candidate", or "leader"

	lastHeartbeat time.Time // 上次收到心跳的时间

	applyCh chan raftapi.ApplyMsg // 用于向服务或测试器发送 ApplyMsg 消息的通道
}

type LogEntry struct {
	Command any // command for state machine
	Term    int // term when entry was received by leader
}

type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	reply.Term = rf.currentTerm

	rf.lastHeartbeat = time.Now() // 更新上次收到心跳的时间
	// 当 follower 和 candidate 收到任期号比自己大的 AppendEntries RPC 时，说明自己过时了，转变为 FOLLOWER
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = FOLLOWER
	}

	// candidate 收到 AppendEntries(term >= currentTerm) 会立即转 follower
	if args.Term == rf.currentTerm && rf.role == CANDIDATE {
		rf.role = FOLLOWER
		rf.votedFor = -1
	}

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	// 在日志中没有包含一个索引为 prevLogIndex 且任期号为 prevLogTerm 的条目时，回复 false
	if args.PrevLogIndex >= len(rf.log) { // prevLogIndex 可能超过了当前日志的长度
		return
	}

	if args.PrevLogIndex >= 0 && args.PrevLogIndex < len(rf.log) && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	// 当一个已经存在的条目和一个新的条目发生冲突（索引相同但任期号不同）时，删除已经存在的条目和它之后的所有条目
	offset := 0
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index < len(rf.log) {
			// 找到第一个冲突的条目，删除它和之后的所有条目
			if rf.log[index].Term != entry.Term {
				rf.log = rf.log[:index]
				break
			}
			offset = i + 1
		} else {
			break
		}
	}

	// Append any new entries not already in the log
	// 将所有新的条目追加到日志中
	for i := offset; i < len(args.Entries); i++ {
		rf.log = append(rf.log, args.Entries[i])
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// 当 leaderCommit 大于 commitIndex 时，将 commitIndex 更新为 leaderCommit 和最后一个新条目的索引中的较小值
	if args.LeaderCommit > rf.commitIndex {
		lastIndex := len(rf.log) - 1
		rf.commitIndex = min(args.LeaderCommit, lastIndex)
		rf.applyEntries()
	}

	reply.Success = true
	reply.Term = rf.currentTerm
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == LEADER
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // 候选人的任期号
	CandidateId  int // 请求选票的候选人的 ID
	LastLogIndex int // 候选人的最后日志条目的索引值
	LastLogTerm  int // 候选人最后日志条目的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // 当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool // 候选人赢得了此张选票时为真
}

func (rf *Raft) lastLogIndexTerm() (int, int) {
	lastIndex := len(rf.log) - 1
	if lastIndex < 0 {
		return -1, 0
	}
	return lastIndex, rf.log[lastIndex].Term
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (3A, 3B).
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	rf.lastHeartbeat = time.Now() // 更新上次收到心跳的时间
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = FOLLOWER
	}

	// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	// 如果 votedFor 是 null 或 candidateId，并且候选人的日志至少和接收者的日志一样新，则投票给候选人
	lastIndex, lastTerm := rf.lastLogIndexTerm()
	up2date := args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && up2date {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}

	reply.Term = rf.currentTerm
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command any) (int, int, bool) {
	// index := -1
	// term := -1
	// isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader := rf.role == LEADER

	if !isLeader {
		return -1, rf.currentTerm, isLeader
	}

	index := len(rf.log)
	term := rf.currentTerm

	rf.log = append(rf.log, LogEntry{
		Command: command,
		Term:    term,
	})

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		time.Sleep(10 * time.Millisecond)
		timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond

		rf.mu.Lock()
		// 如果不是 leader，并且距离上次收到心跳的时间超过了超时时间，则开始选举
		if rf.role != LEADER && time.Since(rf.lastHeartbeat) >= timeout {
			rf.mu.Unlock()
			rf.startElection()
			continue
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) sendHeartbeat() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			rf.mu.Lock()
			term := rf.currentTerm
			prevLogIndex := rf.nextIndex[server] - 1
			prevLogTerm := rf.log[prevLogIndex].Term
			entries := rf.log[rf.nextIndex[server]:]
			args := AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()

			reply := AppendEntriesReply{}
			rf.sendAppendEntries(server, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// 因为并发问题当前rf可能已经不是leader或者任期号已经改变了，需要检查
			if rf.role != LEADER || term != rf.currentTerm {
				return
			}

			// If successful: update nextIndex and matchIndex for follower
			if reply.Success {
				rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			} else {
				// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
				if reply.Term == rf.currentTerm {
					rf.nextIndex[server] = max(1, rf.nextIndex[server]-1)
				}
			}

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.role = FOLLOWER
				rf.votedFor = -1
				rf.lastHeartbeat = time.Now()
			}
		}(i)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.role == LEADER {
		rf.mu.Unlock()
		return
	}

	rf.role = CANDIDATE // 转变为候选人
	rf.currentTerm++    // 任期号加一
	rf.votedFor = rf.me // 给自己投票
	var votes int32 = 1 // 已经获得的选票数，初始值为 1（自己投的一票）
	term := rf.currentTerm
	lastIndex, lastTerm := rf.lastLogIndexTerm()
	rf.lastHeartbeat = time.Now()
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			}
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 如果回复的任期号比当前任期号大，说明自己过时了，转变为 FOLLOWER
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.role = FOLLOWER
				rf.votedFor = -1
				rf.lastHeartbeat = time.Now()
				return
			}

			// 因为并发问题当前rf可能已经不是candidate或者任期号已经改变了，需要检查
			if rf.role != CANDIDATE || rf.currentTerm != term {
				return
			}

			if reply.VoteGranted {
				newVotes := atomic.AddInt32(&votes, 1)
				if newVotes > int32(len(rf.peers)/2) {
					rf.role = LEADER
					rf.lastHeartbeat = time.Now()

					lastIndex := len(rf.log)
					for i := range rf.peers {
						rf.nextIndex[i] = lastIndex
						rf.matchIndex[i] = 0
					}

					go rf.leaderLoop()
					go rf.sendHeartbeat()
				}
			}

		}(i)
	}
}

func (rf *Raft) updateCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
	// 找到满足条件的最大的 N，将 commitIndex 更新为 N
	for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
		if rf.log[N].Term != rf.currentTerm {
			continue
		}
		count := 1 // 包括 leader 自己
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			rf.applyEntries() // 提交新的日志条目
			break
		}
	}
}

func (rf *Raft) leaderLoop() {
	for rf.killed() == false {
		time.Sleep(HEARTBEAT_INTERVAL)
		rf.mu.Lock()
		if rf.role != LEADER {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		rf.sendHeartbeat()
		rf.updateCommitIndex()
	}
}

func (rf *Raft) applyEntries() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		rf.applyCh <- raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{{Term: 0}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.role = FOLLOWER // 一开始都是 FOLLOWER

	// nextIndex 和 matchIndex 在选举后要重新初始化
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.lastHeartbeat = time.Now() // 初始化上次收到心跳的时间

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
