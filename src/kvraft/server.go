package raftkv

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	Op       string // "Get", "Put" or "Append"
	ClientID int64  // client clientID
	SeqNo    int    // request sequence number
}

type LatestReply struct {
	SeqNo int      // latest request
	Reply GetReply // latest reply
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	snapshotIndex int
	persist       *raft.Persister
	db            map[string]string

	notify    map[int]chan struct{}  // channel per log
	duplicate map[int64]*LatestReply // duplication detect

	shutdown chan struct{} // shutdown chan
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// not a leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}

	DPrintf("Peer[%d]: Get request for Key[%q]", kv.me, args.Key)

	kv.mu.Lock()
	// duplicate request check
	if latestReply, ok := kv.duplicate[args.ClientID]; ok {
		// old request, return
		if latestReply.SeqNo >= args.SeqNo {
			kv.mu.Unlock()
			reply.WrongLeader = false
			reply.Err = OK
			reply.Value = latestReply.Reply.Value
			return
		}
	}

	command := Op{
		Key:      args.Key,
		Op:       "Get",
		ClientID: args.ClientID,
		SeqNo:    args.SeqNo,
	}

	// term for check whether change a leader
	index, startTerm, _ := kv.rf.Start(command)
	notify := make(chan struct{})
	kv.notify[index] = notify

	kv.mu.Unlock()

	// wait for result
	select {
	case <-kv.shutdown:
		DPrintf("Peer[%d]: shutdown - Get", kv.me)
		return
	case <-notify:
		curTerm, isLeader := kv.rf.GetState()
		// lost leadership
		if !isLeader || startTerm != curTerm {
			reply.WrongLeader = true
			reply.Err = NOT_LEADER
			return
		}

		kv.mu.Lock()
		if value, ok := kv.db[args.Key]; ok {
			reply.Value = value
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
		kv.mu.Unlock()
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = NOT_LEADER
		return
	}

	DPrintf("Peer[%d]: Put/Append request for Op[%q] Key[%q] Value[%q] from client[%d]@%d",
		kv.me, args.Op, args.Key, args.Value, args.ClientID, args.SeqNo)

	kv.mu.Lock()
	// duplicate request check
	if latestReply, ok := kv.duplicate[args.ClientID]; ok {
		// old request, return
		if latestReply.SeqNo >= args.SeqNo {
			kv.mu.Unlock()
			reply.WrongLeader = false
			reply.Err = OK
			return
		}
	}

	command := Op{
		args.Key,
		args.Value,
		args.Op,
		args.ClientID,
		args.SeqNo,
	}
	index, startTerm, _ := kv.rf.Start(command)
	notify := make(chan struct{})
	kv.notify[index] = notify
	kv.mu.Unlock()

	// wait for result
	select {
	case <-kv.shutdown:
		DPrintf("Peer[%d]: shutdown - Get", kv.me)
		return
	case <-notify:
		curTerm, isLeader := kv.rf.GetState()
		// lost leadership
		if !isLeader || startTerm != curTerm {
			reply.WrongLeader = true
			reply.Err = NOT_LEADER
			return
		}
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.shutdown)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the SeqNo of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)

	// You may need initialization code here.
	kv.persist = persister
	kv.db = make(map[string]string)

	kv.notify = make(map[int]chan struct{})     // channel per log
	kv.duplicate = make(map[int64]*LatestReply) // duplication detect

	kv.shutdown = make(chan struct{}) // shutdown chan

	kv.readSnapshot(kv.persist.ReadSnapshot())
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applyDaemon()

	return kv
}

func (kv *RaftKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	kv.db = make(map[string]string)
	kv.duplicate = make(map[int64]*LatestReply)

	d.Decode(&kv.db)
	d.Decode(&kv.snapshotIndex)
	d.Decode(&kv.duplicate)
}

func (kv *RaftKV) snapshot(index int) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	kv.snapshotIndex = index

	e.Encode(kv.db)
	e.Encode(kv.snapshotIndex)
	e.Encode(kv.duplicate)

	data := w.Bytes()
	kv.persist.SaveSnapshot(data)
}

// get apply msg from raft, apply it to kv state machine
func (kv *RaftKV) applyDaemon() {
	for {
		select {
		case <-kv.shutdown:
			DPrintf("Peer[%d]: shutdown - applyDaemon", kv.me)
			return
		case msg, ok := <-kv.applyCh:
			if ok {
				// have snapshot to apply
				if msg.UseSnapshot {
					kv.mu.Lock()
					kv.readSnapshot(msg.Snapshot)
					kv.snapshot(msg.Index)
					kv.mu.Unlock()
					continue
				}

				if msg.Command != nil && msg.Index > kv.snapshotIndex {
					command := msg.Command.(Op)
					kv.mu.Lock()
					if dup, ok := kv.duplicate[command.ClientID]; !ok || command.SeqNo > dup.SeqNo {
						switch command.Op {
						case "Get":
							kv.duplicate[command.ClientID] = &LatestReply{
								command.SeqNo,
								GetReply{
									Value: kv.db[command.Key],
								},
							}
						case "Put":
							kv.db[command.Key] = command.Value
							kv.duplicate[command.ClientID] = &LatestReply{
								SeqNo: command.SeqNo,
							}
						case "Append":
							kv.db[command.Key] += command.Value
							kv.duplicate[command.ClientID] = &LatestReply{
								SeqNo: command.SeqNo,
							}
						default:
							DPrintf("Peer[%d]: receive unknown commad: %d",
								kv.me, command.Op)
						}
					}

					// db may change, snapshot need check
					if kv.needSnapshot() {
						DPrintf("Peer[%d]: need snapshot - maxraftstate %d raftstatesize %d index %d client %d",
							kv.me, kv.maxraftstate, kv.persist.RaftStateSize(), msg.Index, command.ClientID)
						kv.snapshot(msg.Index)
						kv.rf.NewSnapShot(msg.Index)
					}

					// notify
					if notify, ok := kv.notify[msg.Index]; ok && notify != nil {
						close(notify)
						delete(kv.notify, msg.Index)
					}
					kv.mu.Unlock()
				}
			}
		}
	}
}

func (kv *RaftKV) needSnapshot() bool {
	if kv.maxraftstate < 0 {
		return false
	}

	if kv.maxraftstate < kv.persist.RaftStateSize() {
		return true
	}

	if kv.maxraftstate-kv.persist.RaftStateSize() < kv.maxraftstate/10 {
		return true
	}

	return false
}
