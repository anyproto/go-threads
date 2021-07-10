package net

import (
	"encoding/json"
	"io/ioutil"
)

type MigrationData struct {
	Threads map[string]ThreadData
}

type ThreadData struct {
	LogHeads map[string]HeadData
}

type HeadData struct {
	Head    string
	Counter int64
}

type MigrationConfigPathKey struct{}

type MigrationConfig struct {
	Path          string
	ShouldMigrate bool
}

func readMigrationData(path string) (*MigrationData, error) {
	var data MigrationData
	file, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(file, &data)
	if err != nil {
		return nil, err
	}

	return &data, nil
}
