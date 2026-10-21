package kv

import "testing"

func apply(t *testing.T, s *Store, i uint64, op Op) Result {
	t.Helper()
	_, r, err := s.Apply(i, Encode(op))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestPutGetDelete(t *testing.T) {
	s := NewStore()
	apply(t, s, 1, Op{Type: Put, Key: "a", Value: "1"})
	if r := apply(t, s, 2, Op{Type: Get, Key: "a"}); !r.Found || r.Value != "1" || r.Version != 1 {
		t.Fatalf("get = %+v", r)
	}
	if r := apply(t, s, 3, Op{Type: Delete, Key: "a"}); !r.Found {
		t.Fatalf("delete = %+v", r)
	}
	if r := apply(t, s, 4, Op{Type: Get, Key: "a"}); r.Found {
		t.Fatalf("get after delete = %+v", r)
	}
}

func TestDuplicateIsAppliedOnce(t *testing.T) {
	s := NewStore()
	apply(t, s, 1, Op{Type: Put, Key: "x", Value: "first", Client: "c1", Seq: 1})
	apply(t, s, 2, Op{Type: Put, Key: "x", Value: "other", Client: "c2", Seq: 1})
	// c1 retries its first command (the reply was lost): it must not overwrite c2's write.
	if r := apply(t, s, 3, Op{Type: Put, Key: "x", Value: "first", Client: "c1", Seq: 1}); r.Version != 1 {
		t.Fatalf("duplicate returned %+v, want the original result (version 1)", r)
	}
	if r := apply(t, s, 4, Op{Type: Get, Key: "x"}); r.Value != "other" {
		t.Fatalf("value = %q, want other", r.Value)
	}
}

func TestVersionTracksLastWrite(t *testing.T) {
	s := NewStore()
	apply(t, s, 5, Op{Type: Put, Key: "k", Value: "v1"})
	apply(t, s, 6, Op{Type: Put, Key: "other", Value: "z"})
	apply(t, s, 9, Op{Type: Put, Key: "k", Value: "v2"})
	if r := apply(t, s, 10, Op{Type: Get, Key: "k"}); r.Version != 9 {
		t.Fatalf("version = %d, want 9", r.Version)
	}
}

func TestRejectsReapply(t *testing.T) {
	s := NewStore()
	apply(t, s, 1, Op{Type: Put, Key: "a", Value: "1"})
	if _, _, err := s.Apply(1, Encode(Op{Type: Put, Key: "a", Value: "2"})); err == nil {
		t.Fatal("applying the same index twice must fail")
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	a := NewStore()
	a.Apply(1, Encode(Op{Type: Put, Key: "k", Value: "v1", Client: "c", Seq: 1}))
	a.Apply(2, Encode(Op{Type: Put, Key: "j", Value: "w", Client: "c", Seq: 2}))
	a.Apply(3, Encode(Op{Type: Delete, Key: "j", Client: "d", Seq: 1}))

	b := NewStore()
	if err := b.Restore(a.Snapshot()); err != nil {
		t.Fatal(err)
	}
	if b.AppliedIndex() != 3 || b.Len() != 1 {
		t.Fatalf("applied %d, %d keys", b.AppliedIndex(), b.Len())
	}
	_, r, _ := b.Apply(4, Encode(Op{Type: Get, Key: "k", Client: "e", Seq: 1}))
	if r.Value != "v1" || r.Version != 1 {
		t.Fatalf("get after restore = %+v (the version must survive too)", r)
	}
	// A retry of client c's last write, arriving after the snapshot, must be
	// answered from the dedup table, not applied again.
	_, r, _ = b.Apply(5, Encode(Op{Type: Put, Key: "j", Value: "w", Client: "c", Seq: 2}))
	if b.Len() != 1 || r.Version != 2 {
		t.Fatalf("retry re-applied after restore: %d keys, %+v", b.Len(), r)
	}
}
