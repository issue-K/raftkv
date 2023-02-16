package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{} // 命令
	Term    int
	Index   int
}

// Raft : A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh   chan ApplyMsg
	applyCond *sync.Cond
	applyAble bool
	state     Role
	// 在超时周期前收到过leader的信息
	connectable bool
	// 服务器已知最新的任期（在服务器首次启动时初始化为0，单调递增)
	currentTerm int
	// 当前任期内收到选票的 candidateId，如果没有投给任何候选人 则为空
	votedFor int
	// 日志条目；每个条目包含了用于状态机的命令，以及领导人接收到该条目时的任期（初始索引为1）
	log []LogEntry

	// 已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	commitIndex int
	// 已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied int
	// 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导人最后的日志条目的索引+1）
	nextIndex []int
	// 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）
	matchIndex []int
	// 对每一台机器, 开启一个go routinue同步日志, 不需要同步时用条件变量阻塞
	syncLogCond   []*sync.Cond
	shouldSyncLog []bool
	// 最后一次收到leader消息的时间
	lasLeaderMsgTime time.Time
	// 超时计数器时间
	electionTimout time.Duration
	// 是否需要重置超时计数器
	resetTimeout bool
}

// GetState
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
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
	// Your code here (2C).
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
	// Your code here (2C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// 当前任期
	Term int
	// 请求选票的候选人的 ID
	CandidateId int
	// 候选人的最后日志条目的索引值
	LastLogIndex int
	// 候选人最后日志条目的任期号
	LastLogTerm int
}

type RequestVoteReply struct {
	// Your data here (2A).
	// 当前任期号，以便于候选人去更新自己的任期号
	Term int
	// 候选人赢得了此张选票时为真
	VoteGranted bool
}

type AppendEntriesArgs struct {
	// 领导人的任期
	Term int
	// 领导人ID. 这样跟随者才能对客户端进行重定向
	LeaderId int
	// 紧邻新日志条目之前的那个日志条目的索引
	PrevLogIndex int
	// 紧邻新日志条目之前的那个日志条目的任期
	PrevLogTerm int
	// 需要被保存的日志条目（被当做心跳使用时，则日志条目内容为空；为了提高效率可能一次性发送多个)
	Entries []LogEntry
	// 领导人的已知已提交的最高的日志条目的索引
	LeaderCommit int
}

type AppendEntriesReply struct {
	// 当前任期，对于领导人而言 它会更新自己的任期
	term int
	// 如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了，则为 true
	success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	// 无法接收比自己term小的节点
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		return
	}
	// 要么没投票, 要么本任期投票给了CandidateId, 且CandidateId日志比自己新
	// todo: 什么时候会在voteFor = CandidateId的情况下再次收到它的投票请求, 且需要再次给它投票
	lasTerm := rf.log[len(rf.log)-1].Term
	lasIndex := rf.log[len(rf.log)-1].Index
	logMoreNew := args.LastLogTerm > lasTerm || (args.LastLogTerm == lasTerm && args.LastLogIndex >= lasIndex)
	if rf.votedFor == -1 || (rf.votedFor == args.CandidateId && logMoreNew) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}

	rf.updTerm(args.Term)
}

// call会保证rpc返回, 所以不需要自己做超时处理
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.term = rf.currentTerm
	// 不接受任期比自己小的情况
	if args.Term < rf.currentTerm {
		reply.success = false
		return
	}
	// 检查receiver是否存在PreLog这条日志
	logLen := len(rf.log)
	if logLen-1 < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.success = false
		return
	}
	// 开始同步日志
	del := false
	for i := args.PrevLogIndex + 1; i <= args.PrevLogIndex+len(args.Entries); i++ {
		eid := i - args.PrevLogIndex
		// 如果原来没有这条日志, 或者之前某条日志冲突导致receiver之后的日志被删除, 直接append新日志即可
		if logLen-1 < i || del {
			rf.log = append(rf.log, args.Entries[eid])
			continue
		}
		// 如果之前存在这条日志, 那么比较日志是否一致
		if rf.log[i].Term != args.Entries[eid].Term {
			rf.log = rf.log[:i]
			del = true
		}
	}
	// 更新已提交的日志索引
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}
	rf.updTerm(args.Term)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// Kill
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

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm++

	me := rf.me
	term := rf.currentTerm
	lasLog := rf.log[len(rf.log)-1]
	halfNum := int32(len(rf.peers)-1) / 2
	rf.mu.Unlock()

	var votes atomic.Int32
	votes.Store(1)
	for id, _ := range rf.peers {
		if id == me {
			continue
		}
		go func(serverId int) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateId:  me,
				LastLogIndex: lasLog.Index,
				LastLogTerm:  lasLog.Term,
			}
			reply := &RequestVoteReply{
				Term:        term,
				VoteGranted: false,
			}
			ok := rf.sendRequestVote(serverId, args, reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// 过时的rpc回复, 忽略并返回。。。
			ASSERT(rf.currentTerm >= term)
			if rf.currentTerm > term || rf.state != Candidate {
				return
			}
			if reply.Term > term {
				ASSERT(reply.VoteGranted == false)
				// 切换回跟随者状态
				rf.updTerm(reply.Term)
				return
			}
			if reply.VoteGranted == true {
				votes.Add(1)
				if votes.Load() >= halfNum {
					// 切换为leader
					rf.state = Leader
					// 马上广播一次心跳包
				}
			}
		}(id)
	}
}

func (rf *Raft) electionTicker() {
	rf.lasLeaderMsgTime = time.Now()
	for rf.killed() == false {
		// 随机一个超时计数器
		ms := 50 + (rand.Int63() % 300)
		for {
			rf.mu.Lock()
			// 判断当前超时计数器是否超时. wait为真表示没超时
			wait := rf.lasLeaderMsgTime.Add(time.Duration(ms) * time.Millisecond).After(time.Now())
			if !wait || rf.resetTimeout {
				break
			}
			rf.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
		}
		// 超时了 or 需要重置超时计数器. 检查是哪种情况
		rf.mu.Lock()
		if rf.state == Leader || rf.resetTimeout {
			rf.resetTimeout = false
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()
		// 超时了, 需要开始新的一轮选举
		rf.StartElection()
	}
}

func (rf *Raft) heartBeatOne() {
	rf.mu.Lock()
	term := rf.currentTerm
	me := rf.me
	rf.mu.Unlock()
	for peer := range rf.peers {
		go func(pid int) {
			args := &AppendEntriesArgs{
				Term:         term,
				LeaderId:     me,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      nil,
				LeaderCommit: 0,
			}
			reply := &AppendEntriesReply{
				term:    term,
				success: false,
			}
			ok := rf.sendAppendEntries(pid, args, reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if term != rf.currentTerm || rf.state != Leader {
				return
			}
			if reply.term > rf.currentTerm {
				rf.updTerm(reply.term)
			}
		}(peer)
	}
}

func (rf *Raft) syncPeerLog(pid int) {
	for !rf.killed() {
		rf.syncLogCond[pid].L.Lock()
		for !rf.shouldSyncLog[pid] {
			rf.syncLogCond[pid].Wait()
		}
		rf.syncLogCond[pid].L.Unlock()
		// 开始同步日志了
		rf.mu.Lock()
		term := rf.currentTerm
		me := rf.me
		commitIndex := rf.commitIndex
		preLog := rf.log[len(rf.log)-1]
		entries := []LogEntry{rf.log[rf.nextIndex[pid]]}
		rf.mu.Unlock()
		args := &AppendEntriesArgs{
			Term:         term,
			LeaderId:     me,
			PrevLogIndex: preLog.Index,
			PrevLogTerm:  preLog.Term,
			Entries:      entries,
			LeaderCommit: commitIndex,
		}
		reply := &AppendEntriesReply{
			term:    term,
			success: false,
		}
		ok := rf.sendAppendEntries(pid, args, reply)
		if !ok {
			return
		}
		rf.mu.Lock()
		if term != rf.currentTerm || rf.state != Leader {
			rf.mu.Unlock()
			continue
		}
		if reply.term > rf.currentTerm {
			rf.updTerm(reply.term)
			rf.mu.Unlock()
			continue
		}
		if reply.success {
			rf.nextIndex[pid]++
			rf.matchIndex[pid]++
		} else {
			rf.nextIndex[pid]--
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) applyChTicker() {
	for !rf.killed() {
		// 等待条件变量
		rf.applyCond.L.Lock()
		for !rf.applyAble {
			rf.applyCond.Wait()
		}
		rf.applyCond.L.Unlock()

		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msg := ApplyMsg{
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.log[rf.lastApplied].Index,
			}
			rf.applyCh <- msg
		}
		rf.mu.Unlock()
	}
}

// 调用函数前保证对rf加锁过
func (rf *Raft) updTerm(term int) {
	if rf.currentTerm < term {
		rf.currentTerm = term
		rf.state = Follower
	}
}

func (rf *Raft) newElectionTimeout() {

}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		if rf.state != Leader && !rf.connectable {
			// 开始新一轮选举
			go rf.StartElection()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// 检查在超时时间到期前是否能收到leader信息
		rf.mu.Lock()
		rf.connectable = false
		rf.mu.Unlock()
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// raft自身一些状态
	rf.state = Follower
	rf.currentTerm = 0
	rf.connectable = false
	rf.votedFor = -1

	// 与log相关
	rf.log = []LogEntry{{nil, 0, 0}}
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 0
		rf.matchIndex[i] = 0
	}
	rf.commitIndex = 0
	rf.lastApplied = 0

	// 与apply相关
	rf.applyCond = sync.NewCond(&sync.Mutex{})
	rf.applyAble = false
	rf.applyCh = applyCh
	// 最后一次leader消息的时间, 这个等electionTicker执行前再执行吧..
	// rf.lasLeaderMsgTime = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.electionTicker()
	go rf.applyChTicker()
	return rf
}
