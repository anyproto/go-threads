package net

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	lstore "github.com/textileio/go-threads/core/logstore"
	"github.com/textileio/go-threads/core/thread"
)

func (n *net) recountAndCheck(ctx context.Context, tid thread.ID, lid peer.ID) error {
	heads, err := n.store.Heads(tid, lid)
	if err != nil {
		return err
	}
	if len(heads) == 0 {
		return nil
	}

	// our logs have only one head
	h := heads[0]
	isBroken := h.Counter == thread.CounterUndef && h.ID != cid.Undef
	ts := n.semaphores.Get(semaThreadUpdate(tid))
	ts.Acquire()
	defer ts.Release()
	counter, err := n.countRecords(ctx, tid, h.ID, cid.Undef)
	if err != nil {
		if isBroken {
			return nil
		} else {
			return fmt.Errorf("got problem with thread %s, log %s: broken vs not broken", tid.String(), lid.String())
		}
	}

	if counter != h.Counter {
		return fmt.Errorf("got problem with thread %s, log %s: incorrect counter, migrated %d, real %d", tid.String(), lid.String(), h.Counter, counter)
	}
	return nil
}

func (n *net) recountHeads(ctx context.Context) {
	threadIds, err := n.store.Threads()
	if err != nil {
		log.Errorf("could not get threads from logstore: %v", err)
		return
	}

	log.Info("checking for heads migration")
	completed, err := n.store.MigrationCompleted(lstore.MigrationVersion1)
	if err != nil {
		log.Errorf("error getting migration state: %v", err)
		return
	}
	if !completed {
		log.Info("cannot count while migrating")
		return
	}

	log.Info("starting recounting heads")

	isCorrectlyCounted := true
	for _, tid := range threadIds {
		tInfo, err := n.store.GetThread(tid)
		if err != nil {
			log.With("thread", tid.String()).
				Errorf("error getting thread: %v", err)
			continue
		}

		for _, l := range tInfo.Logs {
			err = n.recountAndCheck(ctx, tid, l.ID)
			if err != nil {
				log.With("thread", tid.String()).
					With("log", l.ID.String()).
					Error("error checking log: %v", err)
				isCorrectlyCounted = false
			}
		}
	}

	if isCorrectlyCounted {
		log.Info("finished counting without errors")
	} else {
		log.Error("there were errors in counting")
	}
}

