package mongods

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	dsq "github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
	"github.com/jbenet/goprocess"
	dsextensions "github.com/textileio/go-datastore-extensions"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	ErrClosed = errors.New("datastore was closed")

	log = logging.Logger("mongods")
)

type MongoDS struct {
	m          *mongo.Client
	db         *mongo.Database
	col        *mongo.Collection
	opTimeout  time.Duration
	txnTimeout time.Duration

	lock   sync.RWMutex
	closed bool
}

var _ datastore.Datastore = (*MongoDS)(nil)
var _ datastore.Batching = (*MongoDS)(nil)
var _ datastore.TxnDatastore = (*MongoDS)(nil)
var _ dsextensions.DatastoreExtensions = (*MongoDS)(nil)

type keyValue struct {
	Key   string `bson:"_id"`
	Value []byte `bson:"v"`
}

func New(ctx context.Context, uri string, dbName string, opts ...Option) (*MongoDS, error) {
	m, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("connecting to MongoDB: %s", err)
	}
	config := defaultConfig
	for _, f := range opts {
		f(&config)
	}

	db := m.Database(dbName)

	_ = db.CreateCollection(ctx, config.collName)
	col := db.Collection(config.collName)

	return &MongoDS{
		m:          m,
		db:         db,
		col:        col,
		opTimeout:  config.opTimeout,
		txnTimeout: config.txnTimeout,
	}, nil
}

func (m *MongoDS) Batch(ctx context.Context) (datastore.Batch, error) {
	return &mongoBatch{
		ds:      m,
		deletes: map[datastore.Key]struct{}{},
		upserts: map[datastore.Key][]byte{},
	}, nil
}

func (m *MongoDS) Put(ctx context.Context, key datastore.Key, val []byte) error {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if m.closed {
		return ErrClosed
	}

	ctx, cls := context.WithTimeout(ctx, m.opTimeout)
	defer cls()
	return m.put(ctx, key, val)
}

func (m *MongoDS) Has(ctx context.Context, key datastore.Key) (bool, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if m.closed {
		return false, ErrClosed
	}

	ctx, cls := context.WithTimeout(ctx, m.opTimeout)
	defer cls()
	return m.has(ctx, key)
}

func (m *MongoDS) Sync(ctx context.Context, key datastore.Key) error {
	return nil
}

func (m *MongoDS) GetSize(ctx context.Context, key datastore.Key) (int, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if m.closed {
		return 0, ErrClosed
	}

	ctx, cls := context.WithTimeout(ctx, m.opTimeout)
	defer cls()
	return m.getSize(ctx, key)
}

func (m *MongoDS) Get(ctx context.Context, key datastore.Key) ([]byte, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if m.closed {
		return nil, ErrClosed
	}
	ctx, cls := context.WithTimeout(ctx, m.opTimeout)
	defer cls()
	return m.get(ctx, key)
}

func (m *MongoDS) Delete(ctx context.Context, key datastore.Key) error {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if m.closed {
		return ErrClosed
	}

	ctx, cls := context.WithTimeout(ctx, m.opTimeout)
	defer cls()
	return m.delete(ctx, key)
}

func (m *MongoDS) QueryExtended(q dsextensions.QueryExt) (query.Results, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if m.closed {
		return nil, ErrClosed
	}

	ctx, cls := context.WithTimeout(context.Background(), m.opTimeout)
	defer cls()

	return m.query(ctx, q)
}

func (m *MongoDS) Query(ctx context.Context, q query.Query) (query.Results, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if m.closed {
		return nil, ErrClosed
	}

	ctx, cls := context.WithTimeout(context.Background(), m.opTimeout)
	defer cls()

	qe := dsextensions.QueryExt{Query: q}

	return m.query(ctx, qe)
}

func (m *MongoDS) Close() error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.closed {
		return fmt.Errorf("mongods already closed")
	}
	ctx, cls := context.WithTimeout(context.Background(), m.opTimeout)
	defer cls()
	if err := m.m.Disconnect(ctx); err != nil {
		return fmt.Errorf("client disconnecting: %s", err)
	}
	m.closed = true
	return nil
}

func (m *MongoDS) get(ctx context.Context, key datastore.Key) ([]byte, error) {
	sr := m.col.FindOne(ctx, bson.M{"_id": key.String()})
	if sr.Err() == mongo.ErrNoDocuments {
		return nil, datastore.ErrNotFound
	}
	if sr.Err() != nil {
		return nil, fmt.Errorf("delete document: %s", sr.Err())
	}
	var kv keyValue
	if err := sr.Decode(&kv); err != nil {
		return nil, fmt.Errorf("decoding key-value: %s", err)
	}
	return kv.Value, nil
}

func (m *MongoDS) delete(ctx context.Context, key datastore.Key) error {
	_, err := m.col.DeleteOne(ctx, bson.M{"_id": key.String()})
	if err != nil {
		return fmt.Errorf("delete document: %s", err)
	}

	return nil
}

func (m *MongoDS) put(ctx context.Context, key datastore.Key, val []byte) error {
	_, err := m.col.UpdateOne(ctx, bson.M{"_id": key.String()}, bson.M{"$set": bson.M{"v": val}}, options.Update().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("inserting/updating key-value: %s", err)
	}
	return nil
}

func (m *MongoDS) has(ctx context.Context, key datastore.Key) (bool, error) {
	sr := m.col.FindOne(ctx, bson.M{"_id": key.String()})
	if sr.Err() == mongo.ErrNoDocuments {
		return false, nil
	}
	if sr.Err() != nil {
		return false, fmt.Errorf("finding key: %s", sr.Err())
	}
	return true, nil
}

func (m *MongoDS) getSize(ctx context.Context, key datastore.Key) (int, error) {
	v, err := m.get(ctx, key)
	if err == datastore.ErrNotFound {
		return -1, err
	}
	if err != nil {
		return 0, fmt.Errorf("getting value: %s", err)
	}
	return len(v), nil
}

func (m *MongoDS) query(ctx context.Context, q dsextensions.QueryExt) (query.Results, error) {
	opts := options.Find()

	// Handle ordering
	asc := true
	if len(q.Orders) > 0 {
		switch q.Orders[0].(type) {
		case dsq.OrderByKey, *dsq.OrderByKey:
			// We order by key by default.
		case dsq.OrderByKeyDescending, *dsq.OrderByKeyDescending:
			// Reverse order by key
			asc = false
		default:
			// Ok, we have a weird order we can't handle. Let's
			// perform the _base_ query (prefix, filter, etc.), then
			// handle sort/offset/limit later.

			// Skip the stuff we can't apply.
			baseQuery := q
			baseQuery.Limit = 0
			baseQuery.Offset = 0
			baseQuery.Orders = nil

			// perform the base query.
			res, err := m.query(ctx, baseQuery)
			if err != nil {
				return nil, err
			}

			// fix the query
			res = dsq.ResultsReplaceQuery(res, q.Query)

			// Remove the parts we've already applied.
			naiveQuery := q.Query
			naiveQuery.Prefix = ""
			naiveQuery.Filters = nil

			// Apply the rest of the query
			return dsq.NaiveQueryApply(naiveQuery, res), nil
		}
	}
	opts.SetSort(bson.M{"_id": 1})
	if !asc {
		opts.SetSort(bson.M{"_id": -1})
	}

	prefix := datastore.NewKey(q.Prefix).String()
	// Important to consider the '/' suffix to respect Prefix semantics
	// of returning strictly child keys.
	var filters bson.A
	if prefix != "/" {
		rgx := fmt.Sprintf("^%s/.*", regexp.QuoteMeta(prefix))
		filters = append(filters, bson.M{"_id": bson.M{"$regex": primitive.Regex{Pattern: rgx}}})
	}
	seekPrefix := datastore.NewKey(q.SeekPrefix).String()
	if seekPrefix != "/" {
		op := "$gte"
		if !asc {
			op = "$lte"
		}
		filters = append(filters, bson.M{"_id": bson.M{op: seekPrefix}})
	}
	fil := bson.M{}
	if len(filters) > 0 {
		fil = bson.M{"$and": filters}
	}

	// If we have no filters, then we can leverage Skip.
	// If that isn't the case, we should fetch all of them
	// and apply skipping later.
	if len(q.Filters) == 0 {
		opts.SetSkip(int64(q.Offset))
	}

	if q.KeysOnly {
		opts.SetProjection(bson.D{
			{Key: "v", Value: 0},
		})
	}

	it, err := m.col.Find(ctx, fil, opts)
	if err != nil {
		return nil, fmt.Errorf("finding key-values: %s", err)
	}

	qrb := dsq.NewResultBuilder(q.Query)
	qrb.Process.Go(func(worker goprocess.Process) {
		m.lock.RLock()
		closedEarly := false
		defer func() {
			m.lock.RUnlock()
			if closedEarly {
				select {
				case qrb.Output <- dsq.Result{
					Error: ErrClosed,
				}:
				case <-qrb.Process.Closing():
				}
			}

		}()
		if m.closed {
			closedEarly = true
			return
		}

		defer func() {
			if err := it.Close(context.Background()); err != nil {
				log.Errorf("closing iterator: %s", err)
			}
		}()

		if len(q.Filters) > 0 {
			// skip to the offset
			skipped := 0
			for skipped < q.Offset {
				ctx, cls := context.WithTimeout(context.Background(), m.opTimeout)
				if !it.Next(ctx) {
					cls()
					break
				}
				cls()

				var item keyValue
				err = it.Decode(&item)
				if err != nil {
					select {
					case qrb.Output <- dsq.Result{Error: err}:
					case <-worker.Closing(): // client told us to close early
						return
					}
				}

				matches := true
				check := func(value []byte) error {
					e := dsq.Entry{
						Key:   item.Key,
						Value: value,
						Size:  len(value), // this function is basically free
					}

					matches = filter(q.Filters, e)
					return nil
				}

				var err error
				if q.KeysOnly {
					err = check(nil)
				} else {
					err = check(item.Value)
				}

				if err != nil {
					select {
					case qrb.Output <- dsq.Result{Error: err}:
					case <-worker.Closing(): // client told us to close early
						return
					}
				}
				if !matches {
					skipped++
				}

			}
			if it.Err() != nil {
				select {
				case qrb.Output <- dsq.Result{Error: it.Err()}:
				case <-worker.Closing(): // client told us to close early
					return
				}
			}
		}

		sent := 0
		for q.Limit <= 0 || sent < q.Limit {
			ctx, cls := context.WithTimeout(context.Background(), m.opTimeout)
			if !it.Next(ctx) {
				cls()
				break

			}
			cls()

			var item keyValue
			err = it.Decode(&item)
			if err != nil {
				select {
				case qrb.Output <- dsq.Result{Error: err}:
				case <-worker.Closing(): // client told us to close early
					return
				}
			}

			e := dsq.Entry{
				Key:   item.Key,
				Value: item.Value,
				Size:  len(item.Value),
			}
			result := dsq.Result{Entry: e}

			// Finally, filter it (unless we're dealing with an error).
			if result.Error == nil && filter(q.Filters, e) {
				continue
			}

			select {
			case qrb.Output <- result:
				sent++
			case <-worker.Closing(): // client told us to close early
				return
			}
		}
		if it.Err() != nil {
			select {
			case qrb.Output <- dsq.Result{Error: it.Err()}:
			case <-worker.Closing(): // client told us to close early
				return
			}
		}
	})

	go qrb.Process.CloseAfterChildren() //nolint

	return qrb.Results(), nil
}

// filter returns _true_ if we should filter (skip) the entry
func filter(filters []dsq.Filter, entry dsq.Entry) bool {
	for _, f := range filters {
		if !f.Filter(entry) {
			return true
		}
	}
	return false
}
