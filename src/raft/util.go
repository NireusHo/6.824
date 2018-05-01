package raft

import "log"

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
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
