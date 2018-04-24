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
	"bytes"
	"encoding/gob"
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

	commitIndexCond *sync.Cond // update commitIndex sync

	// log compaction
	snapshotIndex int // snapshot's last included index
	snapshotTerm  int // snapshot's last included term

	applyCh  chan ApplyMsg // a channel on which the tester or service expects Raft to send ApplyMsg messages
	shutdown chan struct{} // a channel for shut down raft gracefully
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

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	// Persistent state on all servers
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)

	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.Logs)

	d.Decode(&rf.snapshotIndex)
	d.Decode(&rf.snapshotTerm)
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

// should be called when hold the lock
func (rf *Raft) newRequestVoteArgs() *RequestVoteArgs {

	lastLogIndex, lastLogTerm := rf.lastLogInfo()

	args := &RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateID:  rf.me,
		LastLogIndex: lastLogIndex, // logs[0] is placeholder
		LastLogTerm:  lastLogTerm,
	}
	return args
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if rf.isShutDown() {
		DPrintf("Peer[%d]Term[%d] is shutdown - RequestVote", rf.me, rf.CurrentTerm)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLogIndex, lastLogTerm := rf.lastLogInfo()

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
			if (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) || args.LastLogTerm > lastLogTerm {
				rf.resetElectionTimer <- struct{}{}

				rf.turnTo(Follower)
				rf.VotedFor = args.CandidateID

				reply.VoteGranted = true
			}
		}
	}
	DPrintf("Peers[%d]->peer[%d] Vote:%t Term %d->%d, index %d->%d", rf.me, args.CandidateID, reply.VoteGranted, rf.CurrentTerm, args.LastLogTerm, lastLogIndex, args.LastLogIndex)
	rf.persist()
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

// On conversion to candidate, start election:
// • Increment currentTerm
// • Vote for self
// • Reset election timer
// • Send RequestVote RPCs to all other servers
func (rf *Raft) newElection() {
	if rf.isShutDown() {
		DPrintf("Peer[%d]Term[%d] is shutdown - NewElection", rf.me, rf.CurrentTerm)
		return
	}

	// candidate's prepare
	rf.mu.Lock()
	rf.turnTo(Candidate)
	voteArgs := rf.newRequestVoteArgs()
	rf.mu.Unlock()

	// vote for itself, thread safe
	votes := 1
	//sendRequestVotes
	for i := range rf.Peers {
		if i != rf.me {
			go func(server int) {
				DPrintf("Peer[%d]Term[%d] -> Peer[%d] - RequestVote",
					rf.me, rf.CurrentTerm, server)
				var reply RequestVoteReply
				if rf.sendRequestVote(server, voteArgs, &reply) {
					// no need to wait all replies
					func(reply *RequestVoteReply) {
						rf.mu.Lock()
						defer rf.mu.Unlock()

						if rf.state == Candidate {
							// get msg from higher term's leader
							if reply.Term > voteArgs.Term {
								DPrintf("Peer[%d]Term[%d] RequestVoteReply - get higher term from Peer[%d]Term[%d]",
									rf.me, rf.CurrentTerm, server, reply.Term)
								rf.CurrentTerm = reply.Term
								rf.turnTo(Follower)
								rf.resetElectionTimer <- struct{}{}
								rf.persist()
								return
							}
							if reply.VoteGranted {
								votes++
							}
							if votes > len(rf.Peers)/2 {
								rf.turnTo(Leader)
								rf.resetIndex() //reset match and next indexes
								DPrintf("Peer[%d]Term[%d]: become new leader",
									rf.me, rf.CurrentTerm)
								go rf.heartBeats()
								return
							}
						}
					}(&reply)
				}
			}(i)
		}
	}
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

// should be called when hold the lock
func (rf *Raft) newAppendEntriesArgs(PrevLogIndex int) *AppendEntriesArgs {

	return &AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: PrevLogIndex,
		PrevLogTerm:  rf.Logs[rf.LogOffset(PrevLogIndex)].Term,
		Entries:      nil, // nil for heartbeats
		LeaderCommit: rf.commitIndex,
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.isShutDown() {
		DPrintf("Peer[%d]Term[%d] is shutdown - AppendEntries", rf.me, rf.CurrentTerm)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// get AppendEntries from old term leader
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
	}

	if rf.VotedFor != args.LeaderID {
		rf.VotedFor = args.LeaderID
	}

	// this is a Valid AppendEntries(including heartbeat)
	rf.resetElectionTimer <- struct{}{}

	// get old AppendEntries
	if args.PrevLogIndex < rf.snapshotIndex {
		reply.Term = rf.CurrentTerm
		reply.ConflictTerm = rf.CurrentTerm
		reply.ConflictIndex = rf.snapshotIndex
		reply.Success = false
		return
	}

	var preLogTerm, preLogIndex int
	if rf.LogIndex() >= args.PrevLogIndex {
		preLogIndex = args.PrevLogIndex
		preLogTerm = rf.Logs[rf.LogOffset(preLogIndex)].Term
	}

	// AppendEntries's log match local last log
	if preLogTerm == args.PrevLogTerm && preLogIndex == args.PrevLogIndex {
		reply.Success = true

		// get match logs, rebuild the log with new entries
		rf.Logs = rf.Logs[:rf.LogOffset(preLogIndex)+1]
		rf.Logs = append(rf.Logs, args.Entries...)

		// Leader left some log uncommitted, signal for commit
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.LogIndex())
			// signal possible update commit index
			go func() { rf.commitIndexCond.Broadcast() }()
		}
		// tell leader to update matched index
		reply.ConflictTerm = rf.Logs[len(rf.Logs)-1].Term
		reply.ConflictIndex = rf.LogIndex()

		if len(args.Entries) > 0 {
			DPrintf("Peer[%d]Term[%d]: Append Entries - Success From %d %d+%d logs, commit index: leader[%d], local[%d]",
				rf.me, rf.CurrentTerm, args.LeaderID, preLogIndex+1, len(args.Entries), args.LeaderCommit, rf.commitIndex)
		}
	} else {
		// mismatch, find out latest matching index
		// if leader knows about the conflicting term:
		// 		move nextIndex[i] back to leader's last entry for the conflicting term
		// else:
		// 		move nextIndex[i] back to follower's first index
		reply.Success = false

		// TODO
		consistentIndex := rf.snapshotIndex + 1
		reply.ConflictTerm = preLogTerm
		if reply.ConflictTerm == 0 {
			// leader has more logs or follower has empty log
			consistentIndex = len(rf.Logs) + rf.snapshotIndex
			reply.ConflictTerm = rf.Logs[0].Term
		} else {
			for i := preLogIndex - 1; i > rf.snapshotIndex; i-- {
				if rf.Logs[rf.LogOffset(i)].Term != preLogTerm {
					consistentIndex = i + 1
					break
				}
			}
		}
		// conflict start from consistentIndex
		reply.ConflictIndex = consistentIndex
		if len(rf.Logs) <= args.PrevLogIndex {
			DPrintf("Peer[%d]Term[%d]: Append Entries - leader[%d] has more logs (%d > %d), reply: Term[%d]Index[%d]", rf.me, rf, args.LeaderID, args.PrevLogIndex, len(rf.Logs)-1, reply.ConflictTerm, reply.ConflictIndex)
		} else {
			DPrintf("Peer[%d]Term[%d]: Append Entries - leader[%d], pre index/term mismatch (%d != %d, %d != %d)",
				rf.me, rf, args.LeaderID, args.PrevLogIndex, preLogIndex, args.PrevLogTerm, preLogTerm)
		}
	}
	rf.persist()
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
func (rf *Raft) Start(command interface{}) (index int, Term int, isLeader bool) {
	index = -1
	Term = 0
	isLeader = false

	// Your code here (2B).
	if rf.isShutDown() {
		DPrintf("Peer[%d]Term[%d] is shutdown - Start", rf.me, rf.CurrentTerm)
		return index, Term, isLeader
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Leader {
		isLeader = true
		newLog := LogEntry{
			Term:    rf.CurrentTerm,
			Command: command,
		}
		rf.Logs = append(rf.Logs, newLog)

		index = rf.LogIndex()
		Term = rf.CurrentTerm

		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1

		DPrintf("Peers[%d]Term[%d] client add log index[%d]:%v", rf.me, rf.CurrentTerm, index, command)

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
	close(rf.shutdown)
	rf.commitIndexCond.Broadcast()
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
	rf.turnTo(Follower)
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
	rf.shutdown = make(chan struct{})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.lastApplied = rf.snapshotIndex
	rf.commitIndex = rf.snapshotIndex

	// electionDaemon
	go func() {
		for {
			select {
			case <-rf.shutdown:
				DPrintf("Peer[%d]Term[%d] is shutdown - ElectionDaemon", rf.me, rf.CurrentTerm)
				return
			case <-rf.resetElectionTimer:
				if !rf.electionTimer.Stop() {
					<-rf.electionTimer.C
				}
				rf.electionTimer.Reset(rf.electionInterval)
			case <-rf.electionTimer.C:
				DPrintf("Peer[%d]Term[%d]: election timeout, open new election", rf.me, rf.CurrentTerm)
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
				if rf.isShutDown() {
					rf.mu.Unlock()
					DPrintf("Peer[%d]Term[%d] is shutdown - ApplyLogDaemon", rf.me, rf.CurrentTerm)
					close(rf.applyCh)
					return
				}
			}
			last, cur := rf.lastApplied, rf.commitIndex
			if last < cur {
				rf.lastApplied = rf.commitIndex
				logs = make([]LogEntry, cur-last)
				copy(logs, rf.Logs[rf.LogOffset(last)+1:rf.LogOffset(cur)+1])
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

func (rf *Raft) consistencyCheck(server int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	preLogIndex := rf.nextIndex[server] - 1
	if preLogIndex < rf.snapshotIndex {
		rf.sendSnapshot(server)
	} else {
		args := rf.newAppendEntriesArgs(preLogIndex)
		if rf.nextIndex[server] <= rf.LogIndex() {
			args.Entries = rf.Logs[rf.LogOffset(rf.nextIndex[server]):]
		}
		go func() {
			DPrintf("Peer[%d]Term[%d]: send AppendEntries to %d", rf.me, rf.CurrentTerm, server)
			var reply AppendEntriesReply
			if rf.sendAppendEntries(server, args, &reply) {
				func(Reply *AppendEntriesReply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if rf.state != Leader {
						return
					}

					if reply.Success {
						rf.matchIndex[server] = reply.ConflictIndex
						rf.nextIndex[server] = reply.ConflictIndex + 1
						// commit in leader and return to client
						rf.updateCommitIndex()
					} else {
						// find higher term's leader
						if reply.Term > rf.CurrentTerm {
							rf.turnTo(Follower)
							rf.persist()
							rf.resetElectionTimer <- struct{}{}
							DPrintf("Peer[%d]Term[%d]: AppendEntries - get higher Term's reply from Peer[%d]",
								rf.me, rf.CurrentTerm, reply.Term)
							return
						}
						// mismatch, find out latest matching index
						// if leader knows about the conflicting term:
						// 		move nextIndex[i] back to leader's last entry for the conflicting term
						// else:
						// 		move nextIndex[i] back to follower's first index

						rf.nextIndex[server] = reply.ConflictIndex
						if reply.ConflictTerm != 0 {
							for i := len(rf.Logs) - 1; i > 0; i-- {
								if rf.Logs[i].Term == reply.ConflictTerm {
									lastIndex := i + rf.snapshotIndex
									rf.nextIndex[server] = min(lastIndex, reply.ConflictIndex)
									break
								}
							}
						}

						// send snapshot
						if rf.snapshotIndex != 0 && rf.nextIndex[server] <= rf.snapshotIndex {
							DPrintf("Peer[%d]Term[%d]: AppendEntries - peer[%d] need snapshots nextIndex %d < snapshotIndex %d",
								rf.me, rf.CurrentTerm, server, rf.nextIndex[server], rf.snapshotIndex)
							rf.sendSnapshot(server)
						} else {
							// update snapshot:  snapshot + 1 <= rf.nextIndex[n] <= len(rf.Logs) + snapshot
							rf.nextIndex[server] = min(max(rf.nextIndex[server], 1+rf.snapshotIndex), len(rf.Logs)+rf.snapshotIndex)
							DPrintf("Peer[%d]Term[%d]: AppendEntries - peer[%d] nextIndex: %d snapshotID:%d",
								rf.me, rf.CurrentTerm, server, rf.nextIndex[server], rf.snapshotIndex)
						}
					}
				}(&reply)
			}
		}()
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
	if rf.commitIndex < target && rf.snapshotIndex < target {
		if rf.Logs[rf.LogOffset(target)].Term == rf.CurrentTerm {
			DPrintf("Peers[%d]Term[%d]: update commit index %d -> %d",
				rf.me, rf.CurrentTerm, rf.commitIndex, target)
			rf.commitIndex = target
			go func() { rf.commitIndexCond.Broadcast() }()
		} else {
			DPrintf("Peers[%d]Term[%d]: update commit index %d failed (log term %d != current Term %d)",
				rf.me, rf.CurrentTerm, rf.commitIndex, rf.commitIndex, rf.Logs[rf.LogOffset(target)].Term)
		}
	}
}

// must be called when hold lock
func (rf *Raft) resetIndex() {

	for i := 0; i < len(rf.Peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = rf.LogIndex() + 1
		if i == rf.me {
			rf.matchIndex[i] = rf.LogIndex()
		}
	}
}

func (rf *Raft) heartBeats() {
	for {

		if _, isLeader := rf.GetState(); !isLeader {
			return
		}

		rf.resetElectionTimer <- struct{}{}

		select {
		case <-rf.shutdown:
			DPrintf("Peer[%d]Term[%d] is shutdown - HeartBeats", rf.me, rf.CurrentTerm)
			return
		default:
			for i := range rf.Peers {
				if i != rf.me {
					go rf.consistencyCheck(i)
				}
			}
		}

		time.Sleep(rf.heartbeatInterval)
	}
}

// InstallSnapShot RPC
type InstallSnapshotArgs struct {
	Term              int // leader's term
	LeaderID          int // so follower can redirect clients
	LastIncludedIndex int // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int // term of lastIncludedIndex
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) NewSnapShot(index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// index input wrong
	if index <= rf.snapshotIndex || index > rf.commitIndex {
		DPrintf("[%d][%d]: new snapshot index error", rf.me, rf.CurrentTerm)
		return
	}

	// truncate logs before old snapshot
	rf.Logs = rf.Logs[rf.LogOffset(index):]

	rf.snapshotIndex = index
	rf.snapshotTerm = rf.Logs[0].Term

	DPrintf("[%d][%d]: new snapshot in index：%d Term %d",
		rf.me, rf.CurrentTerm, rf.snapshotIndex, rf.snapshotTerm)

	rf.persist()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrentTerm

	if rf.isShutDown() {
		DPrintf("Peer[%d]Term[%d] is shutdown - InstallSnapshot", rf.me, rf.CurrentTerm)
		return
	}

	if args.Term < rf.CurrentTerm {
		DPrintf("Peer[%d]Term[%d]: InstallSnapshot- args.term[%d] < rf.term[%d]",
			rf.me, rf.CurrentTerm, args.Term, rf.CurrentTerm)
		return
	}

	// may have duplicate snapshot
	if args.LastIncludedIndex <= rf.snapshotIndex {
		DPrintf("Peer[%d]Term[%d]: InstallSnapshot- args.LastIncludedIndex[%d] < rf.snapshotIndex[%d]",
			rf.me, rf.CurrentTerm, args.LastIncludedIndex, rf.snapshotIndex)
		return
	}

	rf.resetElectionTimer <- struct{}{}

	// snapshot have all logs
	if args.LastIncludedIndex >= rf.LogIndex() {
		DPrintf("Peer[%d]Term[%d]: InstallSnapshot- have all logs： LastIncludedIndex:%d LogIndex:%d",
			rf.me, rf.CurrentTerm, args.LastIncludedIndex, rf.LogIndex())

		rf.snapshotIndex = args.LastIncludedIndex
		rf.snapshotTerm = args.LastIncludedTerm
		rf.commitIndex = rf.snapshotIndex
		rf.lastApplied = rf.snapshotIndex
		rf.Logs = []LogEntry{{rf.snapshotTerm, nil}}

		rf.applyCh <- ApplyMsg{rf.snapshotIndex, nil, true, args.Snapshot}

		rf.persist()
		return
	}

	// snapshot contains part of logs
	DPrintf("Peer[%d]Term[%d]: InstallSnapshot- have part of logs： LastIncludedIndex:%d LogIndex:%d",
		rf.me, rf.CurrentTerm, args.LastIncludedIndex, rf.LogIndex())

	rf.Logs = rf.Logs[args.LastIncludedIndex-rf.snapshotIndex:]
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm
	rf.commitIndex = rf.snapshotIndex
	rf.lastApplied = rf.snapshotIndex

	rf.applyCh <- ApplyMsg{rf.snapshotIndex, nil, true, args.Snapshot}

	rf.persist()

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.Peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) newInstallSnapshotArgs() *InstallSnapshotArgs {
	return &InstallSnapshotArgs{
		rf.CurrentTerm,
		rf.me,
		rf.snapshotIndex,
		rf.snapshotTerm,
		rf.persister.ReadSnapshot(),
	}
}

// should be called when holding lock
func (rf *Raft) sendSnapshot(server int) {
	args := rf.newInstallSnapshotArgs()
	go func() {
		var reply InstallSnapshotReply
		if rf.sendInstallSnapshot(server, args, &reply) {
			func(server int, reply *InstallSnapshotReply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state == Leader {
					if reply.Term > rf.CurrentTerm {
						rf.CurrentTerm = reply.Term
						rf.turnTo(Follower)
						return
					}

					rf.matchIndex[server] = rf.snapshotIndex
					rf.nextIndex[server] = rf.snapshotIndex + 1
				}
			}(server, &reply)
		}
	}()
}

func (rf *Raft) lastLogInfo() (lastLogIndex, lastLogTerm int) {
	lastLogIndex = rf.snapshotIndex + len(rf.Logs) - 1 // logs[0] is placeholder
	// logs are compacted, need a offset
	lastLogTerm = rf.Logs[rf.LogOffset(lastLogIndex)].Term
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
		rf.CurrentTerm += 1
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

func (rf *Raft) LogOffset(nowIndex int) int {
	return nowIndex - rf.snapshotIndex
}

func (rf *Raft) LogIndex() int {
	return len(rf.Logs) + rf.snapshotIndex - 1
}

func (rf *Raft) isShutDown() bool {
	ok := false

	select {
	case <-rf.shutdown:
		ok = true
	default:
	}

	return ok
}
