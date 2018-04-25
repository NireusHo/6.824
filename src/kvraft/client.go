package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"time"
)

var checkDupClientID = make(map[int64]bool)
var rpcTimeout = 200 // 200ms

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	lastLeader int   // last lastLeader
	SeqNo      int   // RPC SeqNo number
	id         int64 // client id
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func genrateID() int64 {
	for {
		x := nrand()
		if checkDupClientID[x] {
			continue
		}
		checkDupClientID[x] = true
		return x
	}
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastLeader = 0
	ck.SeqNo = 1
	ck.id = genrateID()

	DPrintf("New Clerk lastLeader:%d id:%d", ck.lastLeader, ck.id)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	for {
		args := &GetArgs{
			key,
			ck.id,
			ck.SeqNo,
		}
		reply := &GetReply{}

		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		done := make(chan bool, 1)

		go func() {
			ok := ck.servers[ck.lastLeader].Call("RaftKV.Get", args, reply)
			done <- ok
		}()

		select {
		case <-time.After(time.Duration(rpcTimeout) * time.Millisecond):
			continue // retry
		case ok := <-done:
			if ok && !reply.WrongLeader {
				ck.SeqNo++
				if reply.Err == OK {
					return reply.Value
				}
				return ""
			}
		}

	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	for {
		args := &PutAppendArgs{
			key,
			value,
			op,
			ck.id,
			ck.SeqNo,
		}
		reply := &PutAppendReply{}

		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		done := make(chan bool, 1)

		go func() {
			ok := ck.servers[ck.lastLeader].Call("RaftKV.PutAppend", args, reply)
			done <- ok
		}()

		select {
		case <-time.After(time.Duration(rpcTimeout) * time.Millisecond):
			continue // retry
		case ok := <-done:
			if ok && !reply.WrongLeader && reply.Err == OK {
				ck.SeqNo++
				return
			}
		}

	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
