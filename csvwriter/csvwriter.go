package csvwriter

import (
	"encoding/csv"
	"io"
	"os"
	"strconv"
	"sync"
	"time"
)

type CSVRecord interface {
	FieldValues() []string
	FieldNames() []string
}

const (
	Push    = 1
	PubSub  = 2
	GetLogs = 4
)

type UpdateCSVRecord struct {
	RecordId  string
	ThreadId  string
	LogId     string
	Timestamp time.Time
	Type      int
}

func (u UpdateCSVRecord) FieldValues() []string {
	return []string{
		strconv.FormatInt(u.Timestamp.UnixNano(), 10),
		u.ThreadId,
		u.LogId,
		u.RecordId,
		strconv.FormatInt(int64(u.Type), 10),
	}
}

func (u UpdateCSVRecord) FieldNames() []string {
	return []string{
		"Timestamp",
		"ThreadId",
		"LogId",
		"RecordId",
		"Type",
	}
}

type CSVWriter struct {
	Name   string
	exists bool
	mx     sync.Mutex
}

func NewCSVWriter(name string) (*CSVWriter, error) {
	exists := true
	if _, err := os.Stat(name); os.IsNotExist(err) {
		exists = false
	}

	return &CSVWriter{Name: name, exists: exists}, nil
}

func (w *CSVWriter) Write(model CSVRecord) error {
	w.mx.Lock()
	defer w.mx.Unlock()

	csvFile, err := os.OpenFile(w.Name, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return err
	}
	defer csvFile.Close()

	csvFile.Seek(0, io.SeekEnd)
	writer := csv.NewWriter(csvFile)

	if !w.exists {
		err = writer.Write(model.FieldNames())
		if err != nil {
			return err
		}
		w.exists = true
	}

	err = writer.Write(model.FieldValues())
	if err != nil {
		return err
	}

	writer.Flush()

	return nil
}
