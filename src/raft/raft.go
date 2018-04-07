package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"labrpc"
	"math/rand"
	"sort"
	"sync"
	"time"
)

const (
	Follower = iota
	Candidate
	Leader
)

const (
	electionTime  = 400 // 400ms-800ms
	heartbeatTime = 100 // 100ms
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// Log Entry
type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	Peers     []*labrpc.ClientEnd // RPC end points of all Peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into Peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state int // follower, candidate or leader

	// Persistent state on all servers
	CurrentTerm int        // latest term server has seen (initialized to 0)
	VotedFor    int        // candidateId that received vote in current Term
	Logs        []LogEntry // log entries; each entry contains command for state machine(first index is 1)

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed (initialized to 0)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0)

	// Volatile state on leaders (Reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int //  for each server, index of highest log entry known to be replicated on server (initialized to 0)

	// timer
	electionInterval   time.Duration
	electionTimer      *time.Timer
	resetElectionTimer chan struct{} //reset election timer
	heartbeatInterval  time.Duration

	commitIndexCond *sync.Cond   // update commitIndex sync
	newEntryCond    []*sync.Cond // New LogEntries sync

	applyCh chan ApplyMsg // a channel on which the tester or service expects Raft to send ApplyMsg messages
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.CurrentTerm
	isLeader = rf.state == Leader

	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's Term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // Term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func (rf *Raft) newRequestVoteArgs() *RequestVoteArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.CurrentTerm += 1
	rf.VotedFor = rf.me

	args := &RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateID:  rf.me,
		LastLogIndex: len(rf.Logs) - 1, // logs[0] is placeholder
		LastLogTerm:  rf.Logs[len(rf.Logs)-1].Term,
	}
	return args
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
	} else {
		// switch to follower
		if args.Term > rf.CurrentTerm {
			rf.turnTo(Follower)
			rf.CurrentTerm = args.Term
		}

		// null(Follower) or (Voted for itself)candidate
		if rf.VotedFor == -1 || rf.VotedFor == args.CandidateID {
			// check candidate's log is at least as update
			lastLogIndex, lastLogTerm := rf.lastLogInfo()

			if (args.LastLogIndex == lastLogTerm && args.LastLogIndex >= lastLogIndex) ||
				args.LastLogTerm > lastLogTerm {

				rf.resetElectionTimer <- struct{}{}

				rf.turnTo(Follower)
				rf.VotedFor = args.CandidateID
				reply.VoteGranted = true
			}
		}
	}
	DPrintf("Peers[%d]Term[%d]->peer[%d]Term[%d] Vote:%t", rf.me, rf.CurrentTerm, args.CandidateID, args.Term, reply.VoteGranted)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.Peers[].
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.Peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int        // leader's Term
	LeaderID     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // Term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and PrevLogTerm

	ConflictTerm  int // term of conflicting entry
	ConflictIndex int // the first index of conflicting term
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}

	if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
	}

	// old leader
	if rf.state == Leader {
		rf.turnTo(Follower)
		rf.commitIndexCond.Broadcast()
		for i := range rf.Peers {
			if i != rf.me {
				rf.newEntryCond[i].Broadcast()
			}
		}
	}

	if rf.VotedFor != args.LeaderID {
		rf.VotedFor = args.LeaderID
	}

	// this is a Valid AppendEntries(including heartbeat)
	rf.resetElectionTimer <- struct{}{}

	var preLogTerm, preLogIndex int
	// including log is match
	if len(rf.Logs) > args.PrevLogIndex {
		preLogIndex = args.PrevLogIndex
		preLogTerm = rf.Logs[preLogIndex].Term
	}

	// leader's log match my log
	if preLogTerm == args.PrevLogTerm && preLogIndex == args.PrevLogIndex {
		reply.Success = true

		// trim the logs
		rf.Logs = rf.Logs[:preLogIndex+1]
		DPrintf("Peers[%d]Term[%d]: AppendEntries success from leader %d (%d cmd @ %d), commit index: l->%d, f->%d.\n",
			rf.me, rf.CurrentTerm, args.LeaderID, len(args.Entries), preLogIndex+1, args.LeaderCommit, rf.commitIndex)
		rf.Logs = append(rf.Logs, args.Entries...)

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.Logs)-1)
			// signal possible update commit index
			go func() { rf.commitIndexCond.Broadcast() }()
		}
		// tell leader to update matched index
		reply.ConflictTerm = rf.Logs[len(rf.Logs)-1].Term
		reply.ConflictIndex = len(rf.Logs) - 1
	} else {
		// mismatch, find out latest matching index
		// if leader knows about the conflicting term:
		// 		move nextIndex[i] back to leader's last entry for the conflicting term
		// else:
		// 		move nextIndex[i] back to follower's first index
		reply.Success = false

		// TODO
		consistentIndex := 1
		reply.ConflictTerm = preLogTerm
		if reply.ConflictTerm == 0 {
			// leader has more logs or follower has empty log
			consistentIndex = len(rf.Logs)
			reply.ConflictTerm = rf.Logs[consistentIndex-1].Term
		} else {
			for i := preLogIndex - 1; i > 0; i-- {
				if rf.Logs[i].Term != preLogTerm {
					consistentIndex = i + 1
					break
				}
			}
		}
		reply.ConflictIndex = consistentIndex
		if len(rf.Logs) <= args.PrevLogIndex {
			DPrintf("[%d-%s]:leader %d has more logs (%d > %d), reply: Term[%d]Index[%d]", rf.me, rf, args.LeaderID, args.PrevLogIndex, len(rf.Logs)-1, reply.ConflictTerm, reply.ConflictIndex)
		} else {
			DPrintf("[%d-%s]:leader %d, pre idx/term mismatch (%d != %d, %d != %d).\n",
				rf.me, rf, args.LeaderID, args.PrevLogIndex, preLogIndex, args.PrevLogTerm, preLogTerm)
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.Peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//

// isleader: false if this server isn't the Raft leader, client should try another
// Term: currentTerm, to help caller detect if leader is demoted
// index: log entry to watch to see if the command was committed
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	Term := 0
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Leader {
		isLeader = true
		newLog := LogEntry{
			Term:    rf.CurrentTerm,
			Command: command,
		}
		rf.Logs = append(rf.Logs, newLog)

		index = len(rf.Logs) - 1
		Term = rf.CurrentTerm

		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1

		DPrintf("Peers[%d]Term[%d] client add log index[%d]:%v", rf.me, rf.CurrentTerm, index, command)

		for i := 0; i < len(rf.Peers); i++ {
			if i != rf.me {
				rf.newEntryCond[i].Broadcast()
			}
		}
		rf.persist()
	}

	return index, Term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in Peers[]. this
// server's port is Peers[me]. all the servers' Peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(Peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.Peers = Peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()
	rf.turnTo(Follower)
	rf.mu.Unlock()
	rf.Logs = make([]LogEntry, 1) //Start from 1
	rf.Logs[0] = LogEntry{
		Term:    0,
		Command: nil,
	}

	rf.nextIndex = make([]int, len(Peers))
	rf.matchIndex = make([]int, len(Peers))

	for i := 0; i < len(Peers); i++ {
		rf.nextIndex[i] = len(rf.Logs)
	}

	rf.electionInterval = time.Duration(electionTime+rand.Intn(400)) * time.Millisecond
	rf.electionTimer = time.NewTimer(rf.electionInterval)
	rf.heartbeatInterval = heartbeatTime * time.Millisecond
	rf.resetElectionTimer = make(chan struct{})
	DPrintf("peer[%d]: electionTime [%dms], heartbeatTime [%dms]", rf.me, rf.electionInterval/time.Millisecond, rf.heartbeatInterval/time.Millisecond)

	rf.commitIndexCond = sync.NewCond(&rf.mu)
	rf.newEntryCond = make([]*sync.Cond, len(rf.Peers))
	for i := 0; i < len(rf.Peers); i++ {
		rf.newEntryCond[i] = sync.NewCond(&rf.mu)
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// electionDaemon
	go func() {
		for {
			select {
			case <-rf.resetElectionTimer:
				rf.electionTimer.Reset(rf.electionInterval)
			case <-rf.electionTimer.C:
				rf.mu.Lock()
				DPrintf("Peer[%d]Term[%d] election timeout, open new election", rf.me, rf.CurrentTerm)
				rf.mu.Unlock()
				go rf.newElection()
				rf.electionTimer.Reset(rf.electionInterval)
			}
		}
	}()

	// applyLogEntryDaemon
	go func() {
		for {
			var logs []LogEntry
			// wait
			rf.mu.Lock()
			for rf.lastApplied == rf.commitIndex {
				rf.commitIndexCond.Wait()
			}
			last, cur := rf.lastApplied, rf.commitIndex
			if last < cur {
				rf.lastApplied = rf.commitIndex
				logs = make([]LogEntry, cur-last)
				copy(logs, rf.Logs[last+1:cur+1])
			}
			rf.mu.Unlock()

			for i := 0; i < cur-last; i++ {
				// current command is replicated, ignore nil command
				reply := ApplyMsg{
					Index:   last + i + 1,
					Command: logs[i].Command,
				}

				// reply to outer service
				DPrintf("[%d]: peer %d apply %v to client.\n", rf.me, rf.me, reply)
				// Note: must in the same goroutine, or may result in out of order apply
				rf.applyCh <- reply
			}
		}
	}()

	return rf
}

// On conversion to candidate, start election:
// • Increment currentTerm
// • Vote for self
// • Reset election timer
// • Send RequestVote RPCs to all other servers
func (rf *Raft) newElection() {
	rf.mu.Lock()
	rf.turnTo(Candidate)
	rf.mu.Unlock()
	voteArgs := rf.newRequestVoteArgs()
	replies := make([]RequestVoteReply, len(rf.Peers))

	//sendRequestVotes
	var wg sync.WaitGroup
	for i := range rf.Peers {
		wg.Add(1)
		go func(j int) {
			if rf.state == Candidate {
				defer wg.Done()
				rf.sendRequestVote(j, voteArgs, &replies[j])
			}

		}(i)
	}
	wg.Wait()
	DPrintf("Peers[%d] counting the votes", rf.me)

	// count the votes
	var votes int
	for i := range rf.Peers {
		if replies[i].Term > voteArgs.Term {
			DPrintf("peer[%d] gets higher Term", i)
			// Turn to Follower
			rf.mu.Lock()
			rf.turnTo(Follower)
			rf.mu.Unlock()
			return
		}
		if replies[i].VoteGranted {
			votes++
		}
	}
	if votes > len(rf.Peers)/2 {
		//switch to leader
		rf.mu.Lock()
		DPrintf("Peers[%d] become leader", rf.me)
		rf.turnTo(Leader)
		rf.mu.Unlock()
		rf.resetElection()
		// append new log entry consistency check
		go func() {
			for i := 0; i < len(rf.Peers); i++ {
				if i != rf.me {
					// append new entry and routines heartbeat
					go rf.consistencyCheck(i)
				}
			}
		}()

		// send Heartbeat
		go rf.heartBeats()
	}
}

func (rf *Raft) consistencyCheck(server int) {
	for {
		rf.mu.Lock()
		rf.newEntryCond[server].Wait()

		if rf.state == Leader {
			var args AppendEntriesArgs
			args.Term = rf.CurrentTerm
			args.LeaderID = rf.me
			args.LeaderCommit = rf.commitIndex
			args.PrevLogIndex = rf.nextIndex[server] - 1
			args.PrevLogTerm = rf.Logs[args.PrevLogIndex].Term

			if rf.nextIndex[server] < len(rf.Logs) {
				// new entries
				args.Entries = append(args.Entries, rf.Logs[rf.nextIndex[server]:]...)
			} else {
				// nil for heartbeat
				args.Entries = nil
			}
			rf.mu.Unlock()

			replyCh := make(chan AppendEntriesReply, 1)
			go func() {
				var reply AppendEntriesReply
				if rf.sendAppendEntries(server, &args, &reply) {
					replyCh <- reply
				}
			}()

			select {
			// Append Entries RPC request timeout
			case <-time.After(rf.heartbeatInterval):
				DPrintf("Peers[%d]Term[%d] send to Peers[%d] AppendEntries RPC timeout", rf.me, rf.CurrentTerm, server)
			case reply := <-replyCh:
				rf.mu.Lock()
				if reply.Success {
					// Consistency check success
					rf.matchIndex[server] = reply.ConflictIndex
					rf.nextIndex[server] = rf.matchIndex[server] + 1

					// update itself(leader) commitIndex for commit new Entries
					rf.updateCommitIndex()
				} else {
					// find newer
					if reply.Term > args.Term {
						if reply.Term > rf.CurrentTerm {
							rf.CurrentTerm = reply.Term
							rf.VotedFor = -1
							if rf.state == Leader {
								rf.turnTo(Follower)
								rf.commitIndexCond.Broadcast()
								for i := 0; i < len(rf.Peers); i++ {
									if i != rf.me {
										rf.newEntryCond[i].Broadcast()
									}
								}
							}
							rf.mu.Unlock()
							rf.resetElectionTimer <- struct{}{}
							DPrintf("[%d]:found newer term (heartbeat resp from peer[%d]Term[%d]), turn to follower.", rf.me, server, reply.Term)

							return
						}
					}

					// get conflictingIndex
					isKnown, lastIndex := false, 0
					if reply.ConflictTerm != 0 {
						for i := len(rf.Logs) - 1; i > 0; i-- {
							if rf.Logs[i].Term == reply.ConflictTerm {
								isKnown = true
								lastIndex = i
								DPrintf("[%d-%s]: leader %d have entry %d is the last entry in term %d.",
									rf.me, rf, rf.me, i, reply.ConflictTerm)
								break
							}
						}
						if isKnown {
							if lastIndex > reply.ConflictIndex {
								lastIndex = reply.ConflictIndex
							}
							rf.nextIndex[server] = lastIndex
						} else {
							rf.nextIndex[server] = reply.ConflictIndex
						}
					} else {
						rf.nextIndex[server] = reply.ConflictIndex
					}
					rf.nextIndex[server] = min(max(rf.nextIndex[server], 1), len(rf.Logs))
				}
				rf.mu.Unlock()
			}
		} else {
			rf.mu.Unlock()
			return
		}
	}
}

// updateCommitIndex find new commit id, must be called when hold lock
func (rf *Raft) updateCommitIndex() {
	match := make([]int, len(rf.matchIndex))
	copy(match, rf.matchIndex)
	sort.Ints(match)

	DPrintf("Peers[%d]Term[%d]: try to update commit index: %v",
		rf.me, rf.CurrentTerm, rf.matchIndex)

	target := match[len(rf.Peers)/2]
	if rf.commitIndex < target {
		if rf.Logs[target].Term == rf.CurrentTerm {
			DPrintf("Peers[%d]Term[%d]: update commit index %d -> %d",
				rf.me, rf.CurrentTerm, rf.commitIndex, target)
			rf.commitIndex = target
			go func() { rf.commitIndexCond.Broadcast() }()
		} else {
			DPrintf("Peers[%d]Term[%d]: update commit index %d failed (log term %d != current Term %d)",
				rf.me, rf.CurrentTerm, rf.commitIndex, rf.commitIndex, rf.Logs[target].Term)
		}
	}
}

func (rf *Raft) resetElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := 0; i < len(rf.Peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = len(rf.Logs)
		if i == rf.me {
			rf.matchIndex[i] = len(rf.Logs) - 1
		}
	}
}

func (rf *Raft) heartBeats() {
	for {
		if _, isLeader := rf.GetState(); isLeader {
			rf.resetElectionTimer <- struct{}{}
			for i := range rf.Peers {
				if i != rf.me {
					rf.newEntryCond[i].Broadcast()
				}
			}
		} else {
			break
		}

		time.Sleep(rf.heartbeatInterval)
	}
}

func (rf *Raft) newAppendEntriesArgs() *AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return &AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: len(rf.Logs) - 1,
		PrevLogTerm:  rf.Logs[len(rf.Logs)-1].Term,
		Entries:      nil, // nil for heartbeats
		LeaderCommit: rf.commitIndex,
	}
}

func (rf *Raft) lastLogInfo() (lastLogIndex, lastLogTerm int) {
	lastLogIndex = len(rf.Logs) - 1 // logs[0] is placeholder
	lastLogTerm = rf.Logs[lastLogIndex].Term
	return
}

func (rf *Raft) turnTo(state int) {
	switch state {
	case Follower:
		//DPrintf("Peers[%d]Term[%d]:Follower", rf.me, rf.CurrentTerm)
		rf.state = Follower
		rf.VotedFor = -1
	case Candidate:
		//DPrintf("Peers[%d]Term[%d]:Candidate", rf.me, rf.CurrentTerm)
		rf.state = Candidate
		rf.VotedFor = rf.me
	case Leader:
		//DPrintf("Peers[%d]Term[%d]:Leader", rf.me, rf.CurrentTerm)
		rf.state = Leader
		rf.VotedFor = rf.me
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
