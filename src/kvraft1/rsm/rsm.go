package rsm

import (
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	SeqNo    int
	Req      any
}

type Notification struct {
	Value any
	Id    int64 // 用于校验身份
	Term  int   // 用于校验任期
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	dead int32

	table map[int]chan Notification
}

func (rsm *RSM) Kill() {
	atomic.StoreInt32(&rsm.dead, 1)
}

func (rsm *RSM) killed() bool {
	z := atomic.LoadInt32(&rsm.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {

	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		table:        make(map[int]chan Notification),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	go rsm.Reader()

	return rsm
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	return bigx.Int64()
}

func (rsm *RSM) Reader() {
	for {
		ApplyMsg, ok := <-rsm.applyCh

		if !ok {
			return
		}

		op := ApplyMsg.Command

		req := op.(Op).Req

		res := rsm.sm.DoOp(req)

		noti := Notification{
			Value: res,
			Id:    op.(Op).ClientId,
			Term:  ApplyMsg.CommandTerm,
		}

		rsm.mu.Lock()
		ch, exists := rsm.table[ApplyMsg.CommandIndex]
		if exists {

			select {

			case ch <- noti:

			default:

			}

		}
		rsm.mu.Unlock()
	}
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here

	var op Op

	switch args := req.(type) {
	case *rpc.GetArgs:
		op.Req = args
		op.ClientId = nrand()
		op.SeqNo = args.SeqNo
	case *rpc.PutArgs:
		op.Req = args
		op.ClientId = nrand()
		op.SeqNo = args.SeqNo
	default:
		op = Op{
			Req:      req,
			ClientId: nrand(),
		}
	}

	if op.ClientId == 0 {
		op.ClientId = nrand()
	} // for A

	// fmt.Printf("%d\n", op.ClientId)
	index, term, isLeader := rsm.rf.Start(op)

	if !isLeader {
		return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	}

	ch := make(chan Notification, 1)

	rsm.mu.Lock()
	rsm.table[index] = ch
	rsm.mu.Unlock()

	defer func() {
		rsm.mu.Lock()
		delete(rsm.table, index)
		rsm.mu.Unlock()
	}()

	for {

		if rsm.killed() {
			return rpc.ErrWrongLeader, nil
		}

		select {
		case noti := <-ch:
			if noti.Id != op.ClientId {
				return rpc.ErrWrongLeader, nil
			}
			if noti.Term != term {
				return rpc.ErrWrongLeader, nil
			}
			return rpc.OK, noti.Value

		case <-time.After(50 * time.Millisecond):

			currentTerm, isLeader := rsm.rf.GetState()
			if !isLeader || currentTerm != term {
				return rpc.ErrWrongLeader, nil
			}
		}
	}

}
