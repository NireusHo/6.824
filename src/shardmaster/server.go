package shardmaster

import "raft"
import "labrpc"
import "sync"
import (
	"encoding/gob"
	"log"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	Join  = "Join"
	Leave = "Leave"
	Move  = "Move"
	Query = "Query"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs   []Config               // indexed by config num
	notify    map[int]chan struct{}  // per log entry
	shutdown  chan struct{}          // shutdown channel
	duplicate map[int64]*LatestReply // duplication detection table
}

type LatestReply struct {
	SeqNo int
}

type Op struct {
	// Your data here.
	SeqNo    int    // sequence Number
	Op       string // must be one of "Join", "Leave", "Move" and "Query"
	ClientID int64  // for duplicate request detection

	// args of "Join"
	Servers map[int][]string

	// args of "Leave"
	GIDs []int

	// args of "Move"
	Shard int
	GID   int

	// args of "Query"
	ConfigNum int
}

func (sm *ShardMaster) requestRaft(command *Op) bool {

	if _, isLeader := sm.rf.GetState(); !isLeader {
		return false
	}

	sm.mu.Lock()

	// duplicate request
	if dup, ok := sm.duplicate[command.ClientID]; ok {
		if command.SeqNo <= dup.SeqNo {
			sm.mu.Unlock()
			// append/put request success
			return true
		}
	}

	// notify raft to apply log
	index, term, _ := sm.rf.Start(command)

	// when raft complete apply, it will notify from channel
	ch := make(chan struct{})
	sm.notify[index] = ch
	sm.mu.Unlock()

	// wait for raft to apply log success
	select {
	case <-ch:
		curTerm, isLeader := sm.rf.GetState()

		if !isLeader || term != curTerm {
			return false
		}
	case <-sm.shutdown:

	}
	return false
}

func (sm *ShardMaster) copyConfig(index int, config *Config) {
	if index == -1 || index >= len(sm.configs) {
		index = len(sm.configs) - 1
	}
	config.Num = sm.configs[index].Num
	config.Shards = sm.configs[index].Shards
	config.Groups = make(map[int][]string)
	for k, v := range sm.configs[index].Groups {
		var servers = make([]string, len(v))
		copy(servers, v)
		config.Groups[k] = servers
	}
}

func (sm *ShardMaster) joinConfig(servers map[int][]string) {
	// 1. construct a new configuration
	config := Config{}
	sm.copyConfig(-1, &config)
	config.Num++

	// add or update new gid-servers
	for k, v := range servers {
		var servers = make([]string, len(v))
		copy(servers, v)
		config.Groups[k] = v
	}

	// 2. re-balance
	sm.rebalance(&config)
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) leaveConfig(gids []int) {
	// 1. construct a new configuration
	config := Config{}
	sm.copyConfig(-1, &config)
	config.Num++

	for _, k := range gids {
		delete(config.Groups, k)
	}

	// 2. re-balance
	sm.rebalance(&config)
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) moveConfig(shard int, gid int) {
	// 1. construct a new configuration
	config := Config{}
	sm.copyConfig(-1, &config)
	config.Num++
	config.Shards[shard] = gid

	// 2. no need to re-balance
	sm.configs = append(sm.configs, config)
}

type Status struct {
	group int
	count int
}

func (sm *ShardMaster) rebalance(config *Config) {
	groups := len(config.Groups)

	if groups == 0 {
		return
	}

	// shards per replica group
	average := len(config.Shards) / groups
	// remain slice
	left := len(config.Shards) % groups

	// groups > config.Shards
	if average == 0 {
		return
	}

	// gid -> shard
	var reverseShard = make(map[int][]int)
	for shard, gid := range config.Shards {
		reverseShard[gid] = append(reverseShard[gid], shard)
	}

	// detect a diff
	var extra []Status
	var lacking []Status

	count1, count2 := 0, 0
	// if some group is new
	for gid := range config.Groups {
		if _, ok := reverseShard[gid]; !ok {
			lacking = append(lacking, Status{gid, average})
			count1 += average
		}
	}

	// if some group is removed
	for gid, shard := range reverseShard {
		if _, ok := config.Groups[gid]; !ok {
			extra = append(extra, Status{gid, len(shard)})
			count2 += len(shard)
		} else {
			if len(shard) > average {
				if left == 0 {
					extra = append(extra, Status{gid, len(shard) - average})
					count2 += len(shard) - average
				} else if len(shard) == average+1 {
					left--
				} else if len(shard) > average+1 {
					extra = append(extra, Status{gid, len(shard) - average - 1})
					count2 += len(shard) - average - 1
					left--
				}
			} else if len(shard) < average {
				lacking = append(lacking, Status{gid, average - len(shard)})
				count1 += average - len(shard)
			}
		}
	}

	// compensation for lacking, diff for lacking log
	if diff := count2 - count1; diff > 0 {
		if len(lacking) < diff {
			cnt := diff - len(lacking)
			for gid := range config.Groups {
				if len(reverseShard[gid]) == average {
					lacking = append(lacking, Status{gid, 0})
					if cnt--; cnt == 0 {
						break
					}
				}
			}
		}
		for i := range lacking {
			lacking[i].count++
			if diff--; diff == 0 {
				break
			}
		}
	}

	// modify reverse
	for len(lacking) != 0 && len(extra) != 0 {
		e, l := extra[0], lacking[0]
		src, dst := e.group, l.group
		if e.count > l.count {
			// move l.count to l
			balance(reverseShard, src, dst, l.count)
			lacking = lacking[1:]
			extra[0].count -= l.count
		} else if e.count < l.count {
			// move e.count to l
			balance(reverseShard, src, dst, e.count)
			extra = extra[1:]
			lacking[0].count -= e.count
		} else {
			balance(reverseShard, src, dst, e.count)
			lacking = lacking[1:]
			extra = extra[1:]
		}
	}

	for gid, shards := range reverseShard {
		for _, s := range shards {
			config.Shards[s] = gid
		}
	}

}

// data : gid -> shard
// move src[:count] to dst
func balance(data map[int][]int, src, dst, count int) {
	s, d := data[src], data[dst]

	if count > len(s) {
		DPrintf("count > len(s): %d <-> %d\n", count, len(s))
		panic("count > len(s)")
	}

	e := s[:count]
	d = append(d, e...)
	s = s[count:]

	data[src] = s
	data[dst] = d
}

// common function
func (sm *ShardMaster) requestAgree(cmd *Op, fillReply func(success bool)) {
	// not leader ?
	if _, isLeader := sm.rf.GetState(); !isLeader {
		fillReply(false)
		return
	}

	sm.mu.Lock()
	// duplicate put/append request
	if dup, ok := sm.duplicate[cmd.ClientID]; ok {
		// filter duplicate
		if cmd.SeqNo <= dup.SeqNo {
			sm.mu.Unlock()
			fillReply(true)
			return
		}
	}
	// notify raft to agreement
	index, term, _ := sm.rf.Start(cmd)

	ch := make(chan struct{})
	sm.notify[index] = ch
	sm.mu.Unlock()

	// assume will succeed
	fillReply(true)

	// wait for Raft to complete agreement
	select {
	case <-ch:
		// lose leadership?
		curTerm, isLeader := sm.rf.GetState()

		// what if still leader, but different term? just let client retry
		if !isLeader || term != curTerm {
			fillReply(false)
			return
		}
	case <-sm.shutdown:
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	cmd := Op{ClientID: args.ClientID, SeqNo: args.SeqNo, Op: "Join", Servers: args.Servers}
	sm.requestAgree(&cmd, func(success bool) {
		if success {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
			reply.Err = ""
		}
	})
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	cmd := Op{ClientID: args.ClientID, SeqNo: args.SeqNo, Op: "Leave", GIDs: args.GIDs}
	sm.requestAgree(&cmd, func(success bool) {
		if success {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
			reply.Err = ""
		}
	})
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	cmd := Op{ClientID: args.ClientID, SeqNo: args.SeqNo, Op: "Move", Shard: args.Shard, GID: args.GID}
	sm.requestAgree(&cmd, func(success bool) {
		if success {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
			reply.Err = ""
		}
	})
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	cmd := Op{ClientID: args.ClientID, SeqNo: args.SeqNo, Op: "Query", ConfigNum: args.Num}
	sm.requestAgree(&cmd, func(success bool) {
		if success {
			reply.WrongLeader = false
			reply.Err = OK

			// if success, copy current or past Config
			sm.mu.Lock()
			sm.copyConfig(args.Num, &reply.Config)
			sm.mu.Unlock()
		} else {
			reply.WrongLeader = true
			reply.Err = ""
		}
	})
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	close(sm.shutdown)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(&Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.shutdown = make(chan struct{})
	sm.notify = make(map[int]chan struct{})
	sm.duplicate = make(map[int64]*LatestReply)

	// apply log Daemon(Recv applyMsg from Raft and apply to State machine)
	go func() {
		for {
			select {
			case <-sm.shutdown:
				return
			case msg, ok := <-sm.applyCh:
				if ok && msg.Command != nil {
					command := msg.Command.(*Op)
					sm.mu.Lock()
					if dup, ok := sm.duplicate[command.ClientID]; !ok || dup.SeqNo < command.SeqNo {
						// update to latest
						sm.duplicate[command.ClientID] = &LatestReply{command.SeqNo}
						switch command.Op {
						case "Join":
							sm.joinConfig(command.Servers)
						case "Leave":
							sm.leaveConfig(command.GIDs)
						case "Move":
							sm.moveConfig(command.Shard, command.GID)
						case "Query":

						default:
							panic("Unsupported Method")
							return
						}
					}
					// notify channel
					if notifyCh, ok := sm.notify[msg.Index]; ok && notifyCh != nil {
						close(notifyCh)
						delete(sm.notify, msg.Index)
					}
					sm.mu.Unlock()
				}
			}
		}
	}()
	return sm
}
