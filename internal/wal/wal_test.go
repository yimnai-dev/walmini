package wal

import (
	"path/filepath"
	"reflect"
	"testing"
)

func testWAL(t *testing.T, cfg WALConfig) *WAL {
	t.Helper()
	if cfg.RootDir == "" {
		cfg.RootDir = t.TempDir()
	}
	if cfg.MaxSegmentSize == 0 {
		cfg.MaxSegmentSize = 2 << 20
	}
	var w WAL
	w.Init(cfg)
	return &w
}

func TestAppendReadRoundTrip(t *testing.T) {
	w := testWAL(t, WALConfig{})
	const msg = "hello-wal"
	w.CollectIncomingRecords(msg)
	records, err := w.ReadNext(10, 0)
	if err != nil {
		t.Fatalf("ReadNext: %v", err)
	}
	if len(records) != 1 || records[0] != msg {
		t.Fatalf("got %#v, want one record %q", records, msg)
	}
}

func TestReadBatchesAdvanceCheckpoint(t *testing.T) {
	w := testWAL(t, WALConfig{})
	for _, s := range []string{"a", "b", "c", "d", "e"} {
		w.CollectIncomingRecords(s)
	}
	first, err := w.ReadNext(2, 0)
	if err != nil {
		t.Fatalf("ReadNext: %v", err)
	}
	if !reflect.DeepEqual(first, []string{"a", "b"}) {
		t.Fatalf("first batch: got %#v", first)
	}
	second, err := w.ReadNext(2, 0)
	if err != nil {
		t.Fatalf("ReadNext: %v", err)
	}
	if !reflect.DeepEqual(second, []string{"c", "d"}) {
		t.Fatalf("second batch: got %#v", second)
	}
	third, err := w.ReadNext(2, 0)
	if err != nil {
		t.Fatalf("ReadNext: %v", err)
	}
	if !reflect.DeepEqual(third, []string{"e"}) {
		t.Fatalf("third batch: got %#v", third)
	}
}

func TestSeekNonZero(t *testing.T) {
	w := testWAL(t, WALConfig{})
	for range 5 {
		w.CollectIncomingRecords("x")
	}
	records, err := w.ReadNext(2, 2)
	if err != nil {
		t.Fatalf("ReadNext(2, 2): %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("got %d records: %#v", len(records), records)
	}
}

func TestSeekZeroIsError(t *testing.T) {
	w := testWAL(t, WALConfig{})
	w.CollectIncomingRecords("a")
	if err := w.Seek(0); err == nil {
		t.Fatal("Seek(0) should error")
	}
}

func TestRotatesToSecondSegment(t *testing.T) {
	const small = 256
	w := testWAL(t, WALConfig{MaxSegmentSize: small})
	payloadLen := small - 4 - 1
	large := make([]byte, payloadLen)
	for i := range large {
		large[i] = 'x'
	}
	w.CollectIncomingRecords(string(large))
	w.CollectIncomingRecords("y")
	matches, err := filepath.Glob(filepath.Join(w.FilePathPrefix, "*."+w.SegExt))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) < 2 {
		t.Fatalf("expected at least 2 segment files, got %d: %v", len(matches), matches)
	}
}
