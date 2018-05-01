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
