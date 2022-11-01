package mongods

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ipfs/go-datastore"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	ErrBatchAlreadyCommited = errors.New("batch already commited")
)

type mongoBatch struct {
	lock sync.Mutex

	commited bool
	deletes  map[datastore.Key]struct{}
	upserts  map[datastore.Key][]byte
	ds       *MongoDS
}

func (mb *mongoBatch) Put(ctx context.Context, key datastore.Key, val []byte) error {
	mb.lock.Lock()
	defer mb.lock.Unlock()
	if mb.commited {
		return ErrBatchAlreadyCommited
	}

	mb.upserts[key] = val
	delete(mb.deletes, key)
	return nil
}

func (mb *mongoBatch) Delete(ctx context.Context, key datastore.Key) error {
	mb.lock.Lock()
	defer mb.lock.Unlock()
	if mb.commited {
		return ErrBatchAlreadyCommited
	}

	mb.deletes[key] = struct{}{}
	delete(mb.upserts, key)
	return nil
}

func (mb *mongoBatch) Commit(ctx context.Context) error {
	mb.lock.Lock()
	defer mb.lock.Unlock()
	if mb.commited {
		return ErrBatchAlreadyCommited
	}

	operations := make([]mongo.WriteModel, 0, len(mb.deletes)+len(mb.upserts))
	if cap(operations) == 0 {
		mb.commited = true
		return nil
	}
	for k := range mb.deletes {
		delOp := mongo.NewDeleteOneModel()
		delOp.SetFilter(bson.M{"_id": k.String()})
		operations = append(operations, delOp)
	}
	for k, v := range mb.upserts {
		upsOp := mongo.NewUpdateOneModel()
		upsOp.SetUpsert(true)
		upsOp.SetFilter(bson.M{"_id": k.String()})
		upsOp.SetUpdate(bson.M{"$set": bson.M{"v": v}})
		operations = append(operations, upsOp)
	}

	bulkOption := options.BulkWriteOptions{}
	bulkOption.SetOrdered(false) // Will do things in parallel
	ctx, cls := context.WithTimeout(context.Background(), mb.ds.opTimeout*time.Duration(len(operations)))
	defer cls()
	if _, err := mb.ds.col.BulkWrite(ctx, operations, &bulkOption); err != nil {
		return fmt.Errorf("committing batch: %s", err)
	}

	mb.commited = true
	return nil
}
