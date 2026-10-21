package wal

import (
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
