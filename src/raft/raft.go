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
	"6.5840/labgob"
	"bytes"
	//	"bytes"
	"math/rand"
	"sort"
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

const (
	HeatBeatTime = 50 * time.Millisecond
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
	applyCh chan ApplyMsg

	state Role
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

	// 最后一次收到leader消息的时间
	lasLeaderMsgTime time.Time
	// 超时计数器时间
	electionTimout time.Duration
	// 是否需要重置超时计数器
	resetTimeout bool
}

func (rf *Raft) GetLog(index int) LogEntry {
	id := index - rf.log[0].Index
	ASSERT(id >= 0 && id <= len(rf.log)-1)
	return rf.log[id]
}

// GetState
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.votedFor)
	_ = e.Encode(rf.log)
	rf.persister.Save(w.Bytes(), rf.persister.ReadSnapshot())
}

func (rf *Raft) persistAndSnapshot(snapshot []byte) {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.votedFor)
	_ = e.Encode(rf.log)
	rf.persister.Save(w.Bytes(), snapshot)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&log) != nil {
		LOG("[Persist ERROR] err")
		panic("[Persist ERROR] err")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.log = log
		rf.commitIndex, rf.lastApplied = rf.log[0].Index, rf.log[0].Index
	}
}

// Snapshot
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// 截止并包括到索引index, 生成一个快照
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	baseIndex := rf.log[0].Index
	if index <= baseIndex {
		return
	}
	var newLog []LogEntry
	// 先把索引为Index的日志装进去
	newLog = append(newLog, LogEntry{Term: rf.log[index-baseIndex].Term, Index: index})
	for i := index - baseIndex + 1; i < len(rf.log); i++ {
		newLog = append(newLog, rf.log[i])
	}
	rf.log = newLog
	rf.persistAndSnapshot(snapshot)
	LOG("[Snapshot] 节点%d 生成快照index = %d, snapshot", rf.me, rf.log[0].Index)
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
	Term int
	// 如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了，则为 true
	Success bool
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedTerm  int
	LastIncludedIndex int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lasTerm := rf.log[len(rf.log)-1].Term
	lasIndex := rf.log[len(rf.log)-1].Index
	LOG("[ask RequestVote] %d任期为%d,votefor = %d, lasTerm = %d, lasIndex = %d, 收到投票请求args = %+v", rf.me, rf.currentTerm, rf.votedFor, lasTerm, lasIndex, args)
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	// 无法接收比自己term小的节点
	if rf.currentTerm > args.Term {
		return
	}
	rf.updTerm(args.Term)
	// 要么没投票, 要么本任期投票给了CandidateId, 且CandidateId日志比自己新
	// todo: 什么时候会在voteFor = CandidateId的情况下再次收到它的投票请求, 且需要再次给它投票

	logMoreNew := args.LastLogTerm > lasTerm || (args.LastLogTerm == lasTerm && args.LastLogIndex >= lasIndex)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && logMoreNew {
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
		// 接收了投票, 那么需要重置超时计数器
		rf.resetTimeout = true
		rf.lasLeaderMsgTime = time.Now()
		LOG("[agree RequestVote] %d任期为%d, 同意%d任期%d的投票请求", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	}
}

// call会保证rpc返回, 所以不需要自己做超时处理
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = true
	// 不接受任期比自己小的情况
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	rf.updTerm(args.Term)

	logLen := len(rf.log)
	// 接收者缺少PrevLogIndex日志
	if args.PrevLogIndex < rf.log[0].Index {
		// reply.Term = -1
		reply.Success = false
		return
	}
	// 如果存在PrevLogIndex, 那么应该是rf.log[id]
	id := args.PrevLogIndex - rf.log[0].Index
	if id > logLen-1 || rf.log[id].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}
	// 接收本次的日志: 1、重置超时计数器 2、转换为Follower Todo: 当自己是leader时收到该rpc应该如何处理?
	LOG("[accept AppendEntries] %d任期%d收到心跳包,%d任期%d", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	rf.resetTimeout = true
	rf.lasLeaderMsgTime = time.Now()
	rf.ToFollow()

	// 开始同步日志
	del := false
	flag := false
	//if id == 0 {
	//	rf.log[0] = LogEntry{
	//		Term: args.PrevLogTerm, Index: args.PrevLogIndex,
	//	}
	//}
	for i := id + 1; i <= id+len(args.Entries); i++ {
		eid := i - id - 1
		// 如果原来没有这条日志, 或者之前某条日志冲突导致receiver之后的日志被删除, 直接append新日志即可
		if logLen-1 < i || del {
			flag = true
			rf.log = append(rf.log, args.Entries[eid])
			continue
		}
		// 如果之前存在这条日志, 那么比较日志是否一致
		if rf.log[i].Term != args.Entries[eid].Term {
			rf.log[i] = args.Entries[eid]
			rf.log = rf.log[:i+1]
			del = true
			flag = true
		}
	}
	if flag {
		rf.persist()
	}
	// LOG("[accept AppendEntries]节点%d当前日志情况%+v", rf.me, rf.log)
	// 更新已提交的日志索引
	logLen = len(rf.log)
	if args.LeaderCommit > rf.commitIndex {
		precommit := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, rf.log[logLen-1].Index)
		LOG("[upd commit] 节点%d任期%d的commitIndex更新%d => %d", rf.me, rf.currentTerm, precommit, rf.commitIndex)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	LOG("[InstallSnapshot] 节点%d任期为%d收到%d节点快照", rf.me, rf.currentTerm, args.LeaderId)

	if rf.currentTerm > args.Term {
		return
	}
	baseIndex := rf.log[0].Index
	// 过时的快照
	if args.LastIncludedIndex <= baseIndex {
		return
	}
	rf.updTerm(args.Term)
	rf.ToFollow()
	rf.resetTimeout = true
	rf.lasLeaderMsgTime = time.Now()

	//先持久化快照
	rf.persistAndSnapshot(args.Data)
	// 1、存在这条日志, 保留之后所有的
	id := args.LastIncludedIndex - baseIndex
	if id < len(rf.log) && rf.log[id].Term == args.LastIncludedTerm {
		// 保留之后的日志
		var newlog []LogEntry
		newlog = append(newlog, rf.log[id:]...)
		rf.log = newlog
	} else {
		// 2、不存在, 抛弃之后所有日志
		newlog := []LogEntry{{Term: args.LastIncludedTerm, Index: args.LastIncludedIndex}}
		rf.log = newlog
	}

	// 无条件安装日志
	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
		rf.mu.Lock()
		// 当rf.commitIndex和rf.lastApplied较小时, 取max显然正确
		// 当rf.commitIndex和rf.lastApplied较大时, 比如这是重复发的一个快照, 而且节点已经apply了一些后面的日志, 忽略即可. 当然这里取max也没问题
		rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
		rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)
		LOG("节点%d应用快照完成, commitIndex = %d, lastApplied = %d", rf.me, rf.commitIndex, rf.lastApplied)
		rf.mu.Unlock()
	}()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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

// Start 如果这个server不是leader，返回false.
// 否则，接收这条日志并立即返回
// 不保证这条日志被提交，因为leader可能故障
// 第一个返回值是命令将出现的索引 ,第二个参数是当前任期, 第三个参数返回true表示是否是leader
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}
	rf.log = append(rf.log, LogEntry{
		Command: command,
		Term:    rf.currentTerm,
		Index:   rf.log[len(rf.log)-1].Index + 1,
	})
	rf.persist()
	LOG("[app entry] 接收日志[%d %d %d]", rf.me, rf.log[len(rf.log)-1].Index, rf.log[len(rf.log)-1].Command)
	return rf.log[len(rf.log)-1].Index, rf.currentTerm, true
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
	rf.ToCandidate()

	me := rf.me
	term := rf.currentTerm
	lasLog := rf.log[len(rf.log)-1]
	halfNum := int32(len(rf.peers)+1) / 2
	rf.mu.Unlock()

	var votes atomic.Int32
	votes.Store(1)
	for id := range rf.peers {
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
				// Todo: 切换回跟随者状态, 是否需要刷新超时计时器
				rf.updTerm(reply.Term)
				return
			}
			if reply.VoteGranted == true {
				votes.Add(1)
				if votes.Load() >= halfNum {
					// 切换为leader, 并马上广播一次心跳
					rf.ToLeader()
					// 马上广播一次心跳包
					go rf.syncLogAndHeartBeat()
				}
			}
		}(id)
	}
}

// 目前的超时计数器逻辑
// 每次随机一个超时时间, 然后用time.Sleep()以一个小的参数来驱动检查是否超时
// 当需要重置超时计数器时, 直接将rf.resetTimeout置为true, 那么当timeoutTicker发现时候
// 就会展开新的一轮循环, 重新随机超时时间
func (rf *Raft) timeoutTicker() {
	rf.lasLeaderMsgTime = time.Now()
	for rf.killed() == false {
		// 随机一个超时计数器
		ms := 120 + (rand.Int63() % 200)
		// LOG("[timeoutTicker] 节点%d %+v %d", rf.me, rf.lasLeaderMsgTime, ms)
		for {
			time.Sleep(10 * time.Millisecond)
			rf.mu.Lock()
			// 判断当前超时计数器是否超时. wait为假表示超时
			wait := rf.lasLeaderMsgTime.Add(time.Duration(ms) * time.Millisecond).After(time.Now())
			if !wait || rf.resetTimeout {
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()
		}

		// 超时了 or 需要重置超时计数器. 检查是哪种情况
		rf.mu.Lock()
		// Leader可以无视超时计数器. 否则检查是否需要重置超时计数器
		if rf.state == Leader || rf.resetTimeout {
			rf.resetTimeout = false
			rf.mu.Unlock()
			continue
		}
		// 超时了, 需要转换为candidate, 并发起一轮投票请求
		// 需要刷新lasLeaderMsgTime.
		rf.lasLeaderMsgTime = time.Now()
		rf.mu.Unlock()
		go rf.StartElection()
	}
}

// 向所有peer同步日志(也用作心跳)
func (rf *Raft) syncLogAndHeartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG("[Leader Sync] %d在任期%d同步日志, %+v", rf.me, rf.currentTerm, rf.log)
	term := rf.currentTerm
	me := rf.me
	commitIndex := rf.commitIndex
	nextIndex := make([]int, len(rf.nextIndex))
	copy(nextIndex, rf.nextIndex)

	baseIndex := rf.log[0].Index
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		var entries []LogEntry
		// 需要发送的日志在快照中
		if nextIndex[peer]-1 < rf.log[0].Index {
			// 去同步快照
			// panic("install snapshot not implement...")
			LOG("[发送快照] 节点%d发送快照给%d, nextindex = %d, lastindex = %d", rf.me, peer, nextIndex[peer], rf.log[0].Index)
			go rf.syncPeerSnapshotOne(peer, term, me, rf.log[0].Term, rf.log[0].Index, rf.persister.ReadSnapshot())
		} else {
			id := nextIndex[peer] - baseIndex
			// Todo: 这里能否直接使用切片? 应该不行, 如果状态不为leader了, log是可能会变化的
			// 这里检查一下..
			entries = append(entries, rf.log[id:]...)
			ASSERT(id-1 <= len(rf.log)-1)
			preLog := rf.log[id-1]
			go rf.syncPeerLogOne(peer, term, me, commitIndex, nextIndex[peer], entries, preLog)
		}
	}
}

// 向一个peer同步快照
func (rf *Raft) syncPeerSnapshotOne(pid int, term int, me int, lasIncludedTerm int, lasIncludedIndex int, data []byte) {
	args := &InstallSnapshotArgs{
		Term:              term,
		LeaderId:          me,
		LastIncludedTerm:  lasIncludedTerm,
		LastIncludedIndex: lasIncludedIndex,
		Data:              data,
	}
	reply := &InstallSnapshotReply{
		Term: term,
	}
	// LOG("[发送快照...]data = %v", data)
	ok := rf.sendInstallSnapshot(pid, args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nextIndex[pid] = lasIncludedIndex + 1
	rf.updTerm(reply.Term)
}

// 向一个peer同步日志
func (rf *Raft) syncPeerLogOne(pid int, term int, me int, commitIndex int, nxtId int, entries []LogEntry, preLog LogEntry) {
	// 开始同步日志了

	args := &AppendEntriesArgs{
		Term:         term,
		LeaderId:     me,
		PrevLogIndex: preLog.Index,
		PrevLogTerm:  preLog.Term,
		Entries:      entries,
		LeaderCommit: commitIndex,
	}
	reply := &AppendEntriesReply{
		Term:    term,
		Success: false,
	}
	ok := rf.sendAppendEntries(pid, args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 过时的回复
	if term != rf.currentTerm || rf.state != Leader || rf.nextIndex[pid] != nxtId {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.updTerm(reply.Term)
		return
	}
	if reply.Success {
		if len(entries) != 0 {
			rf.nextIndex[pid] = entries[len(entries)-1].Index + 1
			rf.matchIndex[pid] = entries[len(entries)-1].Index
		}
	} else {
		// 优化, 一次回退所有此任期的日志
		rf.nextIndex[pid]--
		// Todo: 先回退到0去??
		//baseIndex := rf.log[0].Index
		//for i := rf.nextIndex[pid] - 1 - baseIndex; i >= 0; i-- {
		//
		//	if rf.log[i].Term != rf.log[rf.nextIndex[pid]-1-baseIndex].Term {
		//		rf.nextIndex[pid] = rf.log[i].Index
		//		break
		//	}
		//}
	}
}

// leader定时给peer发心跳包
func (rf *Raft) heartBeatTicker() {
	for !rf.killed() {
		// 等待成为Leader
		for !rf.IsLeader() && !rf.killed() {
			time.Sleep(10 * time.Millisecond)
		}
		// 开始定时发心跳包
		for {
			if !rf.IsLeader() || rf.killed() {
				break
			}
			go rf.syncLogAndHeartBeat()
			time.Sleep(HeatBeatTime)
		}
	}
}

// 需要先持有锁
func (rf *Raft) updCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return
	}
	var ma []int
	for peer, match := range rf.matchIndex {
		if peer == rf.me {
			continue
		}
		ma = append(ma, match)
	}
	sort.Ints(ma)
	if rf.commitIndex > ma[len(ma)/2] {
		LOG("rf.commitIndex = %d, ma = %v", rf.commitIndex, ma)
		return
	}
	baseIndex := rf.log[0].Index
	if rf.log[ma[len(ma)/2]-baseIndex].Term != rf.currentTerm {
		return
	}
	ASSERT(rf.commitIndex <= ma[len(ma)/2])
	rf.commitIndex = ma[len(ma)/2]
}

// 定期将commit的日志应用到applyCh
func (rf *Raft) applyChTicker() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		// 如果是leader, 去更新一下自己的commitIndex
		rf.updCommitIndex()

		rf.mu.Lock()
		commitIndex := rf.commitIndex
		baseIndex := rf.log[0].Index
		entries := make([]LogEntry, commitIndex-rf.lastApplied)
		ASSERT(rf.lastApplied <= rf.commitIndex)
		// 需要apply的日志在快照中, 只可能是InstallSnapshot向applyCh发快照被卡住了, 等等
		if rf.lastApplied+1 < rf.log[0].Index {
			rf.mu.Unlock()
			continue
		}
		// LOG("[applyChTicker]节点%d要开始apply %d,%d的日志, 而baseIndex = %d", rf.me, commitIndex, rf.lastApplied, baseIndex)
		copy(entries, rf.log[rf.lastApplied+1-baseIndex:commitIndex-baseIndex+1])
		rf.mu.Unlock()
		for _, entry := range entries {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
			LOG("[[[[APPLY]]]]节点%d 发送entry id = %d", rf.me, entry.Index)
			rf.applyCh <- msg
		}
		rf.mu.Lock()
		// rf.commitIndex可能变大了,所以用之前的版本
		rf.lastApplied = max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

// 调用函数前保证对rf加锁过
func (rf *Raft) updTerm(term int) {
	if rf.currentTerm < term {
		rf.votedFor = -1
		rf.currentTerm = term

		rf.ToFollow()
		rf.persist()
	}
}

func (rf *Raft) ToFollow() {
	rf.state = Follower
}

// ToCandidate
// 使用本方法前需拥有锁
// 对应图2中的conversion to Candidate的前三点.
// 第四点发送RequestVote Rpc给所有peer在StartElection()中实现
func (rf *Raft) ToCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	LOG("[ToCandidate] %d超时, 发起新的一轮选举, 任期变为%d", rf.me, rf.currentTerm)
}

func (rf *Raft) ToLeader() {
	LOG("[ToLeader] %d在任期%d成功当选", rf.me, rf.currentTerm)
	rf.state = Leader
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// 考虑第一次成为leader, 将log分发到少部分节点上但未提交
		// 成为follower后, log可能和新leader冲突截断, 导致nextIndex比log还长的多
		// 下次成为leader时, 应该把nextIndex变为rf.log[len(rf.log)-1].Index+1
		rf.nextIndex[i] = min(rf.nextIndex[i], rf.log[len(rf.log)-1].Index+1)
	}
}

func (rf *Raft) IsLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Todo: 成为leader时, 是否需要初始化所有follower的nextIndex为len(rf.log)? 以及matchIndex是否要初始化?
	return rf.state == Leader
}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
// Todo: 加速日志匹配. 而不是日志不匹配时把nextIndex--, 这样太慢. 落后的follower需要很大代价追上来
// Todo: 应该给每个peer起一个线程用于同步日志, 使用条件变量, 每次调用start时唤醒, 开始同步.(现在的实现必须等待心跳把日志带过去)
// Todo: 异步apply日志. 当leader提交日志 or follower收到leader的commitIndex更新时都需要应用日志. 仍然可以使用线程+条件变量实现 (现在的实现是在applyChTicker线程中循环检查)
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
	rf.votedFor = -1

	// 与log相关
	rf.log = []LogEntry{{nil, 0, 0}}
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if len(rf.log) == 0 {
		rf.log = []LogEntry{{nil, 0, 0}}
	}
	las := rf.log[len(rf.log)-1].Index
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = las + 1
		rf.matchIndex[i] = 0
	}

	// start ticker goroutine to start elections
	// go rf.ticker()
	LOG("[Make] %d启动...", rf.me)
	go rf.heartBeatTicker()
	go rf.timeoutTicker()

	go rf.applyChTicker()
	return rf
}
