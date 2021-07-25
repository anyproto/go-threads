package lstoreds

import (
	"fmt"

	ds "github.com/ipfs/go-datastore"
	core "github.com/textileio/go-threads/core/logstore"
)

type dsMigrationBook struct {
	ds ds.Datastore
}

var (
	migrationKey                    = ds.NewKey("/migration")
	_            core.MigrationBook = (*dsMigrationBook)(nil)
)

// NewMigrationBook returns a new MigrationBook backed by a datastore.
func NewMigrationBook(ds ds.Datastore) core.MigrationBook {
	return &dsMigrationBook{ds: ds}
}

func (mb *dsMigrationBook) SetMigrationCompleted(version core.MigrationVersion) error {
	key := migrationKey.ChildString(string(version))
	bs := []byte{1}
	if err := mb.ds.Put(key, bs); err != nil {
		return fmt.Errorf("error when putting key %v in datastore: %w", key, err)
	}
	return nil
}

func (mb *dsMigrationBook) MigrationCompleted(version core.MigrationVersion) (bool, error) {
	key := migrationKey.ChildString(string(version))
	_, err := mb.ds.Get(key)
	if err == ds.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("error when getting key %s from store: %v", key, err)
	}
	return true, nil
}

func (mb *dsMigrationBook) DumpMigrations() (core.DumpMigrationBook, error) {
	data := make(map[string]struct{})
	// when there will be more migrations we can have more sophisticated logic :-)
	firstMigrationCompleted, err := mb.MigrationCompleted(core.MigrationVersion1)
	if err != nil {
		return core.DumpMigrationBook{Migrations: data}, err
	}
	if firstMigrationCompleted {
		data[string(core.MigrationVersion1)] = struct{}{}
	}

	return core.DumpMigrationBook{Migrations: data}, err
}

func (mb *dsMigrationBook) RestoreMigrations(dump core.DumpMigrationBook) error {
	// when there will be more migrations we can have more sophisticated logic :-)
	_, firstMigrationCompleted := dump.Migrations[string(core.MigrationVersion1)]
	if firstMigrationCompleted {
		return mb.SetMigrationCompleted(core.MigrationVersion1)
	}

	return nil
}
