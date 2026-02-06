package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type data struct {
	Value   string
	Version rpc.Tversion
}
type KVServer struct {
	mu    sync.Mutex
	datas map[string]data
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		datas: make(map[string]data),
	}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	query, exists := kv.datas[args.Key]
	if !exists {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Err = rpc.OK
	reply.Value = query.Value
	reply.Version = query.Version
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	query, exists := kv.datas[args.Key]
	if !exists {
		if args.Version == 0 {
			kv.datas[args.Key] = data{
				Version: 1,
				Value:   args.Value,
			}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
		return
	}

	if args.Version != query.Version {
		reply.Err = rpc.ErrVersion
		return
	}
	kv.datas[args.Key] = data{
		Version: args.Version + 1,
		Value:   args.Value,
	}
	reply.Err = rpc.OK
	return
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
