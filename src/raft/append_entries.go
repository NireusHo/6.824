package raft

import (
	"sort"
	"time"
)

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
