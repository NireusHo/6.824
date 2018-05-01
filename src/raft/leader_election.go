package raft

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
