// Package kv is the replicated state machine: a key-value map driven by the
// committed Raft log. Every replica applies the same commands in the same
// order, so every replica ends up with the same map.
package kv

import (
	"encoding/json"
	"fmt"
)

type OpType string

const (
	Put    OpType = "put"
	Get    OpType = "get" // reads go through the log too: linearizable by construction
	Delete OpType = "delete"
)

// Op is a client command. (Client, Seq) identifies it so that a retried
// command is applied at most once, even if the first attempt did commit.
type Op struct {
	Type   OpType `json:"t"`
	Key    string `json:"k"`
	Value  string `json:"v,omitempty"`
	Client string `json:"c"`
	Seq    uint64 `json:"s"`
}

type Result struct {
	Value string `json:"value,omitempty"`
	Found bool   `json:"found"`
	// Version is the log index of the last write to the key. Clients use it
	// to invalidate cached values (a newer version means the cache is stale).
	Version uint64 `json:"version"`
}

func Encode(op Op) []byte {
	b, _ := json.Marshal(op)
	return b
}

func Decode(b []byte) (Op, error) {
	var op Op
	err := json.Unmarshal(b, &op)
	return op, err
}

type item struct {
	value   string
	version uint64
}

type session struct {
	seq    uint64
	result Result
}

type Store struct {
	data     map[string]item
	sessions map[string]session
	applied  uint64
}

func NewStore() *Store {
	return &Store{data: map[string]item{}, sessions: map[string]session{}}
}

func (s *Store) AppliedIndex() uint64 { return s.applied }

// Apply executes the command stored at log index `index`.
func (s *Store) Apply(index uint64, data []byte) (Op, Result, error) {
	if index <= s.applied {
		return Op{}, Result{}, fmt.Errorf("kv: index %d already applied (at %d)", index, s.applied)
	}
	s.applied = index
	if len(data) == 0 { // leader no-op entry
		return Op{}, Result{}, nil
	}
	op, err := Decode(data)
	if err != nil {
		return Op{}, Result{}, err
	}
	if op.Client != "" {
		if sess, ok := s.sessions[op.Client]; ok && op.Seq <= sess.seq {
			// Duplicate of a command already applied: return its original result.
			// A client keeps one command in flight, so only the last result is needed.
			return op, sess.result, nil
		}
	}
	var res Result
	switch op.Type {
	case Put:
		s.data[op.Key] = item{value: op.Value, version: index}
		res = Result{Value: op.Value, Found: true, Version: index}
	case Delete:
		_, found := s.data[op.Key]
		delete(s.data, op.Key)
		res = Result{Found: found, Version: index}
	case Get:
		it, found := s.data[op.Key]
		res = Result{Value: it.value, Found: found, Version: it.version}
	default:
		return op, Result{}, fmt.Errorf("kv: unknown op %q", op.Type)
	}
	if op.Client != "" {
		s.sessions[op.Client] = session{seq: op.Seq, result: res}
	}
	return op, res, nil
}

func (s *Store) Len() int { return len(s.data) }

// snapshotState is the serialized form of a Store. The dedup table is part of
// it: a retry arriving after a snapshot must still be recognised.
type snapshotState struct {
	Applied  uint64                 `json:"applied"`
	Data     map[string]snapItem    `json:"data"`
	Sessions map[string]snapSession `json:"sessions"`
}

type snapItem struct {
	Value   string `json:"v"`
	Version uint64 `json:"ver"`
}

type snapSession struct {
	Seq    uint64 `json:"s"`
	Result Result `json:"r"`
}

// Snapshot serializes the whole state, as of AppliedIndex.
func (s *Store) Snapshot() []byte {
	st := snapshotState{Applied: s.applied, Data: make(map[string]snapItem, len(s.data)), Sessions: make(map[string]snapSession, len(s.sessions))}
	for k, it := range s.data {
		st.Data[k] = snapItem{Value: it.value, Version: it.version}
	}
	for c, sess := range s.sessions {
		st.Sessions[c] = snapSession{Seq: sess.seq, Result: sess.result}
	}
	b, _ := json.Marshal(st)
	return b
}

// Restore replaces the whole state with a snapshot taken by Snapshot.
func (s *Store) Restore(b []byte) error {
	var st snapshotState
	if err := json.Unmarshal(b, &st); err != nil {
		return fmt.Errorf("kv: bad snapshot: %w", err)
	}
	s.applied = st.Applied
	s.data = make(map[string]item, len(st.Data))
	for k, it := range st.Data {
		s.data[k] = item{value: it.Value, version: it.Version}
	}
	s.sessions = make(map[string]session, len(st.Sessions))
	for c, sess := range st.Sessions {
		s.sessions[c] = session{seq: sess.Seq, result: sess.Result}
	}
	return nil
}
