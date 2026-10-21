package wal

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/kabilaymen/quorum/raft"
)

func ent(term, index uint64, data string) raft.Entry {
	return raft.Entry{Term: term, Index: index, Data: []byte(data)}
}

func open(t *testing.T, dir string) (*WAL, raft.HardState, []raft.Entry) {
	t.Helper()
	w, hs, es, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	return w, hs, es
}

func TestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w, hs, es := open(t, dir)
	if hs != (raft.HardState{}) || len(es) != 0 {
		t.Fatalf("fresh dir: %+v %+v", hs, es)
	}
	if err := w.Save(&raft.HardState{Term: 3, Vote: 2}, []raft.Entry{ent(1, 1, ""), ent(3, 2, "a")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Save(nil, []raft.Entry{ent(3, 3, "b")}); err != nil {
		t.Fatal(err)
	}
	w.Close()

	w, hs, es = open(t, dir)
	defer w.Close()
	if hs != (raft.HardState{Term: 3, Vote: 2}) {
		t.Fatalf("hardstate = %+v", hs)
	}
	if len(es) != 3 || string(es[2].Data) != "b" || es[1].Term != 3 {
		t.Fatalf("entries = %+v", es)
	}
}

func TestOverwriteSuffix(t *testing.T) {
	dir := t.TempDir()
	w, _, _ := open(t, dir)
	w.Save(nil, []raft.Entry{ent(1, 1, "a"), ent(1, 2, "b"), ent(1, 3, "c")})
	// a new leader rewrites from index 2
	w.Save(nil, []raft.Entry{ent(2, 2, "x")})
	w.Save(nil, []raft.Entry{ent(2, 3, "y"), ent(2, 4, "z")})
	w.Close()

	w, _, es := open(t, dir)
	defer w.Close()
	got := ""
	for _, e := range es {
		got += string(e.Data)
	}
	if got != "axyz" {
		t.Fatalf("replayed log = %q, want axyz", got)
	}
}

func TestTornTailIsDiscarded(t *testing.T) {
	dir := t.TempDir()
	w, _, _ := open(t, dir)
	w.Save(nil, []raft.Entry{ent(1, 1, "a"), ent(1, 2, "bbbbbbbb")})
	w.Close()

	// Simulate a crash in the middle of the last write.
	path := filepath.Join(dir, "log")
	st, _ := os.Stat(path)
	os.Truncate(path, st.Size()-3)

	w, _, es := open(t, dir)
	if len(es) != 1 || string(es[0].Data) != "a" {
		t.Fatalf("after torn write: %+v", es)
	}
	// New appends must land right after the last valid record.
	w.Save(nil, []raft.Entry{ent(1, 2, "c")})
	w.Close()
	w, _, es = open(t, dir)
	defer w.Close()
	if len(es) != 2 || string(es[1].Data) != "c" {
		t.Fatalf("after recovery: %+v", es)
	}
}

func TestCorruptedRecordIsDiscarded(t *testing.T) {
	dir := t.TempDir()
	w, _, _ := open(t, dir)
	w.Save(nil, []raft.Entry{ent(1, 1, "a"), ent(1, 2, "b")})
	w.Close()
	path := filepath.Join(dir, "log")
	b, _ := os.ReadFile(path)
	b[len(b)-1] ^= 0xff // flip a bit in the last payload
	os.WriteFile(path, b, 0o644)

	w, _, es := open(t, dir)
	defer w.Close()
	if len(es) != 1 {
		t.Fatalf("checksum mismatch must drop the record, got %+v", es)
	}
}

func TestGapIsRejected(t *testing.T) {
	w, _, _ := open(t, t.TempDir())
	defer w.Close()
	if err := w.Save(nil, []raft.Entry{ent(1, 5, "x")}); err == nil {
		t.Fatal("appending index 5 to an empty log must fail")
	}
}

func entries(term uint64, from, to uint64) []raft.Entry {
	var es []raft.Entry
	for i := from; i <= to; i++ {
		es = append(es, ent(term, i, "x"))
	}
	return es
}

func TestSnapshotCompactsTheLog(t *testing.T) {
	dir := t.TempDir()
	w, _, _ := open(t, dir)
	if err := w.Save(&raft.HardState{Term: 1}, entries(1, 1, 10)); err != nil {
		t.Fatal(err)
	}
	before, _ := os.Stat(filepath.Join(dir, "log"))
	if err := w.SaveSnapshot(raft.Snapshot{Index: 7, Term: 1, Data: []byte("state")}); err != nil {
		t.Fatal(err)
	}
	after, _ := os.Stat(filepath.Join(dir, "log"))
	if after.Size() >= before.Size() {
		t.Fatalf("log did not shrink: %d -> %d bytes", before.Size(), after.Size())
	}
	// Appending continues after the kept suffix.
	if err := w.Save(nil, entries(1, 11, 12)); err != nil {
		t.Fatal(err)
	}
	w.Close()

	w, hs, es := open(t, dir)
	defer w.Close()
	if s := w.Snapshot(); s.Index != 7 || s.Term != 1 || string(s.Data) != "state" {
		t.Fatalf("snapshot = %+v", s)
	}
	if hs.Term != 1 || len(es) != 5 || es[0].Index != 8 || es[4].Index != 12 {
		t.Fatalf("hs %+v, entries %d..%d (%d)", hs, es[0].Index, es[len(es)-1].Index, len(es))
	}
}

// A snapshot from a leader whose log differs at its last index replaces the
// whole log: the conflicting suffix must not come back after a restart.
func TestConflictingSnapshotDropsTheLog(t *testing.T) {
	dir := t.TempDir()
	w, _, _ := open(t, dir)
	w.Save(&raft.HardState{Term: 1}, entries(1, 1, 5))
	if err := w.SaveSnapshot(raft.Snapshot{Index: 3, Term: 2}); err != nil {
		t.Fatal(err)
	}
	if err := w.Save(nil, entries(2, 4, 4)); err != nil {
		t.Fatal(err)
	}
	w.Close()
	w, _, es := open(t, dir)
	defer w.Close()
	if len(es) != 1 || es[0].Index != 4 || es[0].Term != 2 {
		t.Fatalf("entries = %+v, want only entry 4 of term 2", es)
	}
}

// Crash after the snapshot was written but before the log was rewritten: the
// directory holds a snapshot at 4 next to the full log 1..6.
func TestCrashBetweenSnapshotAndLogRewrite(t *testing.T) {
	dir := t.TempDir()
	w, _, _ := open(t, dir)
	w.Save(&raft.HardState{Term: 1}, entries(1, 1, 6))
	w.Close()
	snap := make([]byte, 20)
	binary.LittleEndian.PutUint64(snap[0:8], 4)
	binary.LittleEndian.PutUint64(snap[8:16], 1)
	binary.LittleEndian.PutUint32(snap[16:20], crc32.Checksum(nil, castagnoli))
	if err := os.WriteFile(filepath.Join(dir, "snapshot"), snap, 0o644); err != nil {
		t.Fatal(err)
	}

	w, _, es := open(t, dir)
	defer w.Close()
	if w.Snapshot().Index != 4 || len(es) != 2 || es[0].Index != 5 {
		t.Fatalf("snapshot %+v, entries %+v: want entries 5..6", w.Snapshot(), es)
	}
	if err := w.Save(nil, entries(1, 7, 7)); err != nil {
		t.Fatalf("appending after recovery: %v", err)
	}
}

func TestCorruptedSnapshotIsRefused(t *testing.T) {
	dir := t.TempDir()
	w, _, _ := open(t, dir)
	w.Save(&raft.HardState{Term: 1}, entries(1, 1, 3))
	w.SaveSnapshot(raft.Snapshot{Index: 2, Term: 1, Data: []byte("state")})
	w.Close()
	b, _ := os.ReadFile(filepath.Join(dir, "snapshot"))
	b[len(b)-1] ^= 0xff
	os.WriteFile(filepath.Join(dir, "snapshot"), b, 0o644)
	if _, _, _, err := Open(dir, Options{}); err == nil {
		t.Fatal("a corrupted snapshot must be refused, not ignored")
	}
}
