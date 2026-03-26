package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	recentLeader int

	mu       sync.Mutex
	ClientId int64
	SeqNo    int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	return bigx.Int64()
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	ck.recentLeader = 0
	ck.ClientId = nrand()
	ck.SeqNo = 1
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {

	// You will have to modify this function.

	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := rpc.GetArgs{
		Key: key,

		ClientId: ck.ClientId,
		SeqNo:    ck.SeqNo,
	}
	var reply rpc.GetReply

	ck.SeqNo++

	for {

		reply = rpc.GetReply{}

		ok := ck.clnt.Call(ck.servers[ck.recentLeader], "KVServer.Get", &args, &reply)

		if !ok {
			ck.recentLeader = (ck.recentLeader + 1) % len(ck.servers)
			continue
		}

		if reply.Err == rpc.ErrNoKey {
			return "", 0, rpc.ErrNoKey
		}

		if reply.Err == rpc.ErrWrongGroup {
			ck.recentLeader = (ck.recentLeader + 1) % len(ck.servers)
			continue
		}

		if reply.Err == rpc.ErrWrongLeader {
			ck.recentLeader = (ck.recentLeader + 1) % len(ck.servers)
			continue
		}

		if reply.Err == rpc.OK {
			break
		}

	}

	return reply.Value, reply.Version, reply.Err
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.

	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,

		ClientId: ck.ClientId,
		SeqNo:    ck.SeqNo,
	}
	reply := rpc.PutReply{}

	ck.SeqNo++

	first_time := true

	for {

		reply = rpc.PutReply{}

		ok := ck.clnt.Call(ck.servers[ck.recentLeader], "KVServer.Put", &args, &reply)

		if !ok {
			ck.recentLeader = (ck.recentLeader + 1) % len(ck.servers)
			first_time = false
			time.Sleep(50 * time.Millisecond)
			continue
		}

		if reply.Err == rpc.ErrVersion {
			if first_time {
				return rpc.ErrVersion
			} else {
				return rpc.ErrMaybe
			}
		}

		if reply.Err == rpc.ErrWrongGroup {
			ck.recentLeader = (ck.recentLeader + 1) % len(ck.servers)
			continue
		}

		if reply.Err == rpc.ErrWrongLeader {
			ck.recentLeader = (ck.recentLeader + 1) % len(ck.servers)
			continue
		}

		if reply.Err == rpc.OK {
			break
		}

	}

	return reply.Err
}
