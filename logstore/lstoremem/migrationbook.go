package lstoremem

import core "github.com/textileio/go-threads/core/logstore"

type memoryMigrationBook struct {
	migrations map[string]struct{}
}

var _ core.MigrationBook = (*memoryMigrationBook)(nil)

func NewMigrationBook() core.MigrationBook {
	return &memoryMigrationBook{migrations: make(map[string]struct{})}
}

func (mb *memoryMigrationBook) SetMigrationCompleted(version core.MigrationVersion) error {
	mb.migrations[string(version)] = struct{}{}
	return nil
}

func (mb *memoryMigrationBook) MigrationCompleted(version core.MigrationVersion) (bool, error) {
	_, ok := mb.migrations[string(version)]
	return ok, nil
}
