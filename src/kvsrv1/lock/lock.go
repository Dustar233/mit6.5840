package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck   kvtest.IKVClerk
	id   string
	name string
	// You may add code here

}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.id = kvtest.RandValue(8)
	lk.name = l
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		val, ver, err := lk.ck.Get(lk.name)

		if err == rpc.ErrMaybe {
			continue
		}

		if val == lk.id {
			return
		}

		if err == rpc.ErrNoKey {
			err := lk.ck.Put(lk.name, lk.id, 0)
			if err == rpc.ErrMaybe {
				continue
			}
			if err == rpc.OK {
				return
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}

		if val == "" {
			continue
		}

		if val == "unlock" && err == rpc.OK {
			err := lk.ck.Put(lk.name, lk.id, ver)
			if err == rpc.ErrMaybe {
				continue
			}
			if err == rpc.OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

}

func (lk *Lock) Release() {
	// Your code here
	for {
		val, ver, err := lk.ck.Get(lk.name)

		if err != rpc.OK {
			continue
		}

		if val == "" {
			continue
		}

		if val == lk.id {

			err = lk.ck.Put(lk.name, "unlock", ver)

			if err == rpc.ErrMaybe {
				continue
			}

			if err == rpc.OK {
				return
			}

		} else {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}
