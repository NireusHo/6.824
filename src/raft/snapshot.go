package raft

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
