package queue

import (
	"context"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/textileio/go-threads/core/thread"
	"sync"
	"testing"
)

func TestSyncOperationsDifferentThreadsCorrectOrder(t *testing.T) {
	closeCh1 := make(chan struct{})
	closeCh2 := make(chan struct{})
	q := NewSyncQueue(context.Background())
	m := sync.Mutex{}
	var res1 []int
	var res2 []int
	t1 := thread.NewIDV1(thread.Raw, 32)
	t2 := thread.NewIDV1(thread.Raw, 32)
	pid := peer.ID("Incorrect Id, but I am too lazy to create correct one :-)")
	totalOps := 100

	newOp := func(param int, totalOps int, res *[]int, ch chan struct{}) PeerCall {
		return func(ctx context.Context, _ peer.ID, _ thread.ID) error {
			m.Lock()
			defer m.Unlock()
			*res = append(*res, param)
			if len(*res) == totalOps {
				close(ch)
			}
			return nil
		}
	}

	for i := 0; i < totalOps; i++ {
		q.Schedule(pid, t1, newOp(i, totalOps, &res1, closeCh1))
		q.Schedule(pid, t2, newOp(i, totalOps, &res2, closeCh2))
	}
	<-closeCh1
	<-closeCh2
	for i := 0; i < totalOps; i++ {
		if res1[i] != i || res2[i] != i {
			t.Fatalf("ops were called in wrong order")
		}
	}
}
