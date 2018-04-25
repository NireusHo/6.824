package raftkv

const (
	OK         = "OK"
	ErrNoKey   = "ErrNoKey"
	NOT_LEADER = "Not a leader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	ClientID int64
	SeqNo    int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.

	ClientID int64
	SeqNo    int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
