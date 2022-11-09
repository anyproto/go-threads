package queue

import (
	"context"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/textileio/go-threads/core/thread"
	"sync"
	"testing"
)

func TestPushBackOperationsDifferentThreadsCorrectOrder(t *testing.T) {
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
		q.PushBack(pid, t1, newOp(i, totalOps, &res1, closeCh1))
		q.PushBack(pid, t2, newOp(i, totalOps, &res2, closeCh2))
	}
	<-closeCh1
	<-closeCh2
	for i := 0; i < totalOps; i++ {
		if res1[i] != i || res2[i] != i {
			t.Fatalf("ops were called in wrong order")
		}
	}
}

func TestPushFrontCorrectOrder(t *testing.T) {
	closeCh := make(chan struct{})
	startCh := make(chan struct{})
	isStarted := false
	q := NewSyncQueue(context.Background())
	var res []int
	tid := thread.NewIDV1(thread.Raw, 32)
	pid := peer.ID("Incorrect Id, but I am too lazy to create correct one :-)")
	totalOps := 100

	newOp := func(param int, totalOps int, res *[]int, ch chan struct{}) PeerCall {
		return func(ctx context.Context, _ peer.ID, _ thread.ID) error {
			<-startCh
			if !isStarted {
				// skip first number because it can be any
				isStarted = true
				return nil
			}
			*res = append(*res, param)
			if len(*res) == totalOps {
				close(ch)
			}
			return nil
		}
	}

	// we will drop one operation because the first called operation can be any
	for i := 0; i < totalOps+1; i++ {
		q.PushFront(pid, tid, newOp(i, totalOps, &res, closeCh))
	}
	close(startCh)
	<-closeCh
	for i := 1; i < totalOps; i++ {
		if res[i] > res[i-1] {
			t.Fatalf("ops were called in wrong order")
		}
	}
}

func TestSyncOperationsDifferentThreadsCorrectThreadIdInsideCall(t *testing.T) {
	closeCh := make(chan struct{})
	q := NewSyncQueue(context.Background())
	m := sync.Mutex{}
	isCorrectThread := true
	ops := 0
	t1 := thread.NewIDV1(thread.Raw, 32)
	t2 := thread.NewIDV1(thread.Raw, 32)
	pid := peer.ID("Incorrect Id, but I am too lazy to create correct one :-)")

	newOp := func(totalOps int, ch chan struct{}, tid thread.ID) PeerCall {
		return func(ctx context.Context, _ peer.ID, t thread.ID) error {
			m.Lock()
			defer m.Unlock()
			if t != tid {
				isCorrectThread = false
			}
			ops++
			if ops == totalOps {
				close(ch)
			}
			return nil
		}
	}

	q.PushBack(pid, t1, newOp(2, closeCh, t1))
	q.PushBack(pid, t2, newOp(2, closeCh, t2))

	<-closeCh
	if !isCorrectThread {
		t.Fatalf("got incorrect thead in call")
	}
}
