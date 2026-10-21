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
