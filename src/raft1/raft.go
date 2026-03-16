package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"bytes"
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
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

	applyCh   chan raftapi.ApplyMsg
	applyCond sync.Cond

	state       int
	currentTerm int
	votedFor    int
	logs        []logEntry
	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	lastHeartBeat   time.Time
	electionTimeout time.Duration

	voteTotal int
}

func (rf *Raft) resetTimeOut() {
	ms := heartBeatTimeOut + (rand.Int63() % heartBeatTimeOutDuration)
	rf.electionTimeout = time.Duration(ms) * time.Millisecond
}

const (
	heartBeatTimeOut         = 550
	heartBeatTimeOutDuration = 1300
	heartBeatFreq            = 110
)

func (rf *Raft) isHeartBeat() bool {

	if time.Since(rf.lastHeartBeat) > rf.electionTimeout {
		return false
	}
	return true
}

type logEntry struct {
	Data interface{}
	Term int
}

const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
type persistStates struct {
	CurrentTerm int
	VoteFor     int
	Logs        []logEntry
}

func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	var states persistStates

	states = persistStates{
		CurrentTerm: rf.currentTerm,
		VoteFor:     rf.votedFor,
		Logs:        rf.logs,
	}

	e.Encode(states)
	// empty for snapshot
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var states persistStates

	if d.Decode(&states) != nil {
		fmt.Print("Failed to readPersist\n")
	} else {
		rf.currentTerm = states.CurrentTerm
		rf.votedFor = states.VoteFor
		rf.logs = states.Logs
	}

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
	Term         int
	NodeId       int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	OK   bool
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1 //不能无条件投票，交由之后处理

		rf.persist()

	}

	if args.Term < rf.currentTerm {
		reply.OK = false

		return
	}

	if (rf.votedFor == -1 || rf.votedFor == args.NodeId) &&
		((args.LastLogTerm > rf.logs[len(rf.logs)-1].Term) || (args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex >= len(rf.logs)-1)) {
		reply.OK = true
		rf.votedFor = args.NodeId
		rf.lastHeartBeat = time.Now()
		rf.resetTimeOut()

		rf.persist()
	} else {
		reply.OK = false
	}

}

type AppendEntriesArgs struct {
	Term              int
	LeaderId          int
	PrevLogTerm       int
	PrevLogIndex      int
	Entries           []logEntry
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term int
	OK   bool

	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	reply.Term = rf.currentTerm
	reply.OK = false

	if args.Term < rf.currentTerm {
		return
	}

	rf.state = Follower
	rf.lastHeartBeat = time.Now()
	rf.resetTimeOut()

	reply.OK = true

	//if heartbeat (Entry nil)
	// if args.Entries == nil {

	// 	return
	// }

	if len(rf.logs) <= args.PrevLogIndex {
		reply.OK = false
		reply.ConflictIndex = len(rf.logs)
		reply.ConflictTerm = -1
		return
	}
	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.OK = false
		reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term

		reply.ConflictIndex = 1

		for i := args.PrevLogIndex - 1; i >= 0; i-- {
			if rf.logs[i].Term != reply.ConflictTerm {
				reply.ConflictIndex = i + 1
				break
			}
		}

		return
	}

	//优化：不可靠网络环境下，顺序不确定，可能导致覆盖问题。改为找不同点一次截断
	step := args.PrevLogIndex + 1
	for i, Entry := range args.Entries {

		if step >= len(rf.logs) {
			rf.logs = append(rf.logs, args.Entries[i:]...)
			rf.persist()
			break
		} else {
			if rf.logs[step].Term != Entry.Term {
				rf.logs = rf.logs[:step]
				rf.logs = append(rf.logs, args.Entries[i:]...)
				rf.persist()
				break
			}
		}

		step++
	}

	if args.LeaderCommitIndex > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommitIndex, len(rf.logs)-1)
		rf.applyCond.Signal()
	}

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

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}

	index = len(rf.logs)
	term = rf.currentTerm
	newEntry := logEntry{
		Data: command,
		Term: term,
	}

	rf.logs = append(rf.logs, newEntry)

	rf.persist()

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
		rf.mu.Lock()
		if rf.isHeartBeat() == false && rf.state != Leader {

			rf.currentTerm++
			rf.state = Candidate
			rf.votedFor = rf.me

			rf.lastHeartBeat = time.Now()
			rf.resetTimeOut()

			currentTerm := rf.currentTerm

			rf.persist()

			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				NodeId:       rf.me,
				LastLogIndex: len(rf.logs) - 1,
				LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
			}

			times := len(rf.peers)

			rf.voteTotal = 1
			rf.mu.Unlock()

			for i := 0; i < times; i++ {

				if rf.me == i {
					continue
				}

				go func(i int) {
					rf.mu.Lock()

					if rf.state != Candidate || rf.currentTerm != currentTerm {
						rf.mu.Unlock()
						return
					}

					reply := RequestVoteReply{}
					rf.mu.Unlock()

					rf.sendRequestVote(i, &args, &reply)

					rf.mu.Lock()

					if rf.state != Candidate || rf.currentTerm != args.Term {
						rf.mu.Unlock()
						return
					}

					if reply.Term > rf.currentTerm {
						rf.state = Follower
						rf.currentTerm = reply.Term
						rf.votedFor = -1

						rf.persist()

						rf.lastHeartBeat = time.Now()
						rf.resetTimeOut()

						rf.mu.Unlock()

						return
					}

					if reply.OK {
						rf.voteTotal++
						if rf.voteTotal > len(rf.peers)/2 {
							rf.state = Leader

							for i := 0; i < len(rf.peers); i++ {
								rf.nextIndex[i] = len(rf.logs)
								rf.matchIndex[i] = 0
							}

							go rf.broadCastHeartBeat()
						}
					}

					rf.mu.Unlock()
				}(i)
			}
		} else {
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 30
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) updateCommitIndex() {

	tempArr := make([]int, len(rf.matchIndex))
	copy(tempArr, rf.matchIndex)
	tempArr[rf.me] = len(rf.logs) - 1

	slices.Sort(tempArr)

	n := len(tempArr)

	newCommitIndex := tempArr[(n-1)/2]

	if newCommitIndex > rf.commitIndex && rf.logs[newCommitIndex].Term == rf.currentTerm {
		rf.commitIndex = newCommitIndex
		rf.applyCond.Signal()
	}

}

func (rf *Raft) broadCastHeartBeat() {

	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	index := len(rf.peers)
	rf.mu.Unlock()

	for i := 0; i < index; i++ {

		rf.mu.Lock()
		if i == rf.me {
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()

		go func(i int) {

			rf.mu.Lock()

			step := rf.nextIndex[i] - 1

			if step < 0 || rf.state != Leader {
				rf.mu.Unlock()
				return
			}

			args := AppendEntriesArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				PrevLogTerm:       rf.logs[step].Term,
				PrevLogIndex:      step,
				Entries:           make([]logEntry, len(rf.logs)-(step+1)),
				LeaderCommitIndex: rf.commitIndex,
			}
			copy(args.Entries, rf.logs[step+1:])
			reply := AppendEntriesReply{}

			rf.mu.Unlock()
			ok := rf.sendAppendEntries(i, &args, &reply)
			if ok {
				rf.mu.Lock()
				if rf.currentTerm != args.Term || rf.state != Leader { //加入过期检查
					rf.mu.Unlock()
					return
				}

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1

					rf.persist()

					rf.lastHeartBeat = time.Now()
					rf.resetTimeOut()
				} else if reply.Term == rf.currentTerm {

					if reply.OK == false {
						if reply.ConflictTerm == -1 {
							// Case 1: Follower 日志太短
							rf.nextIndex[i] = reply.ConflictIndex
						} else {
							// Case 2 & 3: Term 冲突
							// 尝试在 Leader 自己的日志中找一下这个 ConflictTerm
							lastIndexWithTerm := -1
							for j := len(rf.logs) - 1; j > 0; j-- {
								if rf.logs[j].Term == reply.ConflictTerm {
									lastIndexWithTerm = j
									break
								}
							}

							if lastIndexWithTerm != -1 {
								// Case 2: Leader 有这个 Term，则从该 Term 的下一条开始试
								rf.nextIndex[i] = lastIndexWithTerm + 1
							} else {
								// Case 3: Leader 没这个 Term，直接跳到 Follower 那个 Term 的起点
								rf.nextIndex[i] = reply.ConflictIndex
							}
						}
						// 防止减过头（虽然逻辑上不应该）
						rf.nextIndex[i] = max(1, rf.nextIndex[i])
					} else {
						newMatch := args.PrevLogIndex + len(args.Entries)
						if newMatch > rf.matchIndex[i] {
							rf.matchIndex[i] = newMatch
						}
						rf.nextIndex[i] = rf.matchIndex[i] + 1

						rf.updateCommitIndex()

					}

				}
				rf.mu.Unlock()
			}

		}(i)

	}
}

func (rf *Raft) heartBeatTicker() {
	for rf.killed() == false {

		rf.broadCastHeartBeat()

		ms := heartBeatFreq
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		tempArr := make([]logEntry, rf.commitIndex-rf.lastApplied)
		copy(tempArr, rf.logs[rf.lastApplied+1:rf.commitIndex+1])
		lastApplied := rf.lastApplied

		rf.mu.Unlock()

		cnt := 0
		for i, Entry := range tempArr {
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      Entry.Data,
				CommandIndex: lastApplied + i + 1,
			}
			cnt++
		}

		rf.mu.Lock()
		rf.lastApplied = lastApplied + cnt
		rf.mu.Unlock()
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
	rf.applyCh = applyCh
	rf.applyCond = *sync.NewCond(&rf.mu)
	// Your initialization code here (3A, 3B, 3C).

	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]logEntry, 1)
	rf.logs[0] = logEntry{Term: 0}

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))

	rf.lastHeartBeat = time.Now()
	rf.resetTimeOut()

	// initialize from state persisted before a crash
	if rf.persister.RaftStateSize() > 0 {
		rf.readPersist(rf.persister.ReadRaftState())
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartBeatTicker()
	go rf.applier()

	return rf
}
