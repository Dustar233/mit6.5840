package kvraft

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type data struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	mu        sync.Mutex
	lastSeqNo map[int64]int
	lastOp    map[int64]rpc.PutReply

	datas map[string]data

	// Your definitions here.
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15

func (kv *KVServer) DoOp(req any) any {
	// Your code here

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if ptr, ok := req.(*rpc.GetArgs); ok {
		req = *ptr
	} else if ptr, ok := req.(*rpc.PutArgs); ok {
		req = *ptr
	}

	switch args := req.(type) {

	case rpc.GetArgs:

		val, ok := kv.datas[args.Key]
		if !ok {
			rep := rpc.GetReply{
				Err: rpc.ErrNoKey,
			}

			return rep
		}

		rep := rpc.GetReply{
			Value:   val.Value,
			Version: val.Version,
			Err:     rpc.OK,
		}

		// kv.lastOp[args.ClientId] = rep
		// kv.lastSeqNo[args.ClientId] = args.SeqNo

		return rep

	case rpc.PutArgs:

		lastSeqNo, ok := kv.lastSeqNo[args.ClientId]

		if ok {
			if lastSeqNo >= args.SeqNo {
				return kv.lastOp[args.ClientId]
			}
		}

		d, exists := kv.datas[args.Key]

		var tmp data

		if exists {

			if args.Version == d.Version {

				tmp = data{
					Value:   args.Value,
					Version: args.Version + 1,
				}

			} else {

				rep := rpc.PutReply{
					Err: rpc.ErrVersion,
				}

				return rep
			}

		} else {

			tmp = data{
				Value:   args.Value,
				Version: args.Version + 1,
			}

		}

		kv.datas[args.Key] = tmp

		rep := rpc.PutReply{
			Err: rpc.OK,
		}

		kv.lastOp[args.ClientId] = rep
		kv.lastSeqNo[args.ClientId] = args.SeqNo

		return rep

	default:
		fmt.Printf("FATAL BUG: DoOp Unknown type: %T\n", req)
		return nil

	}

}

type SnapshotData struct {
	LastSeqNo map[int64]int
	Datas     map[string]data
	LastOp    map[int64]rpc.PutReply
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here

	kv.mu.Lock()
	defer kv.mu.Unlock()

	msg := SnapshotData{
		LastSeqNo: make(map[int64]int),
		Datas:     make(map[string]data),
		LastOp:    make(map[int64]rpc.PutReply),
	}

	for k, v := range kv.lastSeqNo {
		msg.LastSeqNo[k] = v
	}

	for k, v := range kv.datas {
		msg.Datas[k] = v
	}

	for k, v := range kv.lastOp {
		msg.LastOp[k] = v
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(msg)

	states := w.Bytes()

	// fmt.Printf("[DEBUG] Snapshot generated, length: %d\n", len(states))

	return states
}

func (kv *KVServer) Restore(data []byte) {

	// fmt.Printf("[DEBUG] Restore called with data length: %d\n", len(data))
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var msg SnapshotData
	err := d.Decode(&msg)
	if err != nil {
		fmt.Printf("Failed to readSnapshot, error details: %v\n", err)
	} else {

		kv.lastOp = msg.LastOp
		kv.lastSeqNo = msg.LastSeqNo
		kv.datas = msg.Datas
	}

}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)

	err, rep := kv.rsm.Submit(args)

	if err != rpc.OK {
		reply.Err = err
	}

	if rep != nil {
		actualReply := rep.(rpc.GetReply)
		reply.Value = actualReply.Value
		reply.Version = actualReply.Version
		reply.Err = actualReply.Err
	}
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)

	err, rep := kv.rsm.Submit(args)

	if err != rpc.OK {
		reply.Err = err
	}

	if rep != nil {
		actualReply := rep.(rpc.PutReply)
		reply.Err = actualReply.Err
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
	if kv.rsm != nil {
		kv.rsm.Kill()
	}
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(SnapshotData{})
	labgob.Register(data{})

	kv := &KVServer{me: me}

	kv.mu.Lock()
	kv.lastSeqNo = make(map[int64]int)
	kv.lastOp = make(map[int64]rpc.PutReply)
	kv.datas = make(map[string]data)
	kv.mu.Unlock()

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.

	return []tester.IService{kv, kv.rsm.Raft()}
}
