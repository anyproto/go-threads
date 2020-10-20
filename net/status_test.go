package net

import (
	"testing"

	"github.com/textileio/go-threads/core/thread"
)

func TestThreadStatusRegistry(t *testing.T) {
	var (
		reg = NewThreadStatusRegistry("")
		tid = thread.NewIDV1(thread.Raw, 24)
	)

	tests := []struct {
		desc     string
		event    ThreadStatusEvent
		expected ThreadStatus
	}{
		{
			desc:     "first uninitialized request",
			expected: ThreadStatus{Initialized: false},
		},
		{
			desc:     "start uploading",
			event:    ThreadStatusUploadStarted,
			expected: ThreadStatus{Initialized: true, UploadInProgress: true},
		},
		{
			desc:     "start simultaneous download",
			event:    ThreadStatusDownloadStarted,
			expected: ThreadStatus{Initialized: true, UploadInProgress: true, DownloadInProgress: true},
		},
		{
			desc:     "upload failed",
			event:    ThreadStatusUploadFailed,
			expected: ThreadStatus{Initialized: true, UploadSuccess: false, DownloadInProgress: true},
		},
		{
			desc:     "download succeeded",
			event:    ThreadStatusDownloadDone,
			expected: ThreadStatus{Initialized: true, DownloadInProgress: false, DownloadSuccess: true},
		},
		{
			desc:     "uploading again",
			event:    ThreadStatusUploadStarted,
			expected: ThreadStatus{Initialized: true, UploadInProgress: true, DownloadSuccess: true},
		},
		{
			desc:     "finally uploaded",
			event:    ThreadStatusUploadDone,
			expected: ThreadStatus{Initialized: true, UploadSuccess: true, DownloadSuccess: true},
		},
	}

	for _, tt := range tests {
		if tt.event > 0 {
			reg.Apply(tid, tt.event)
		}

		if actual := reg.Get(tid); actual != tt.expected {
			t.Errorf("%s, expected: %+v, actual: %+v", tt.desc, tt.expected, actual)
		}
	}

	if total := reg.Total(); total != 1 {
		t.Errorf("expected 1 thread to be tracked, got %d", total)
	}
}
