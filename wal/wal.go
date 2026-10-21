// Package wal persists a raft node's HardState, latest snapshot and log.
//
// Layout of the data directory:
//
//	hardstate   term + vote, replaced atomically (write temp file, fsync, rename)
//	snapshot    [index u64][term u64][crc32c u32][state machine bytes], replaced atomically
//	log         append-only sequence of records: [len u32][crc32c u32][payload]
//
// A log record holds one entry. Truncation needs no special record: an entry
// whose index is not past the current end implicitly discards everything from
// that index on, which is exactly what Raft does when a follower's log
// conflicts with the leader's. On open, a torn or corrupted tail (crash in the
// middle of a write) is detected by its length or checksum and cut off.
//
// SaveSnapshot writes the snapshot first, then rewrites the log without the
// entries it covers. A crash between the two steps leaves an old log next to a
// newer snapshot; Open applies the same filter, so the state is the same.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/kabilaymen/quorum/raft"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

const maxRecord = 64 << 20

type WAL struct {
	dir     string
	log     *os.File
	w       *bufio.Writer
	lastIdx uint64
	snap    raft.Snapshot
	noSync  bool
}

type Options struct {
	// NoSync skips fsync (tests and benchmarks only: a crash can lose writes).
	NoSync bool
}

// Open loads the persisted state from dir (created if missing) and returns a
// WAL positioned to append after it. The entries returned are those after the
// snapshot (see Snapshot).
func Open(dir string, opts Options) (*WAL, raft.HardState, []raft.Entry, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, raft.HardState{}, nil, err
	}
	hs, err := readHardState(filepath.Join(dir, "hardstate"))
	if err != nil {
		return nil, raft.HardState{}, nil, err
	}
	snap, err := readSnapshot(filepath.Join(dir, "snapshot"))
	if err != nil {
		return nil, raft.HardState{}, nil, err
	}
	w := &WAL{dir: dir, snap: snap, noSync: opts.NoSync}
	entries, err := w.openLog()
	if err != nil {
		return nil, raft.HardState{}, nil, err
	}
	return w, hs, entries, nil
}

// Snapshot returns the latest snapshot saved (zero if none).
func (w *WAL) Snapshot() raft.Snapshot { return w.snap }

// openLog opens the log file, cuts a torn tail, and returns the entries after
// the snapshot.
func (w *WAL) openLog() ([]raft.Entry, error) {
	f, err := os.OpenFile(filepath.Join(w.dir, "log"), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	entries, good, err := replay(f)
	if err == nil {
		entries, err = afterSnapshot(entries, w.snap)
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	// Cut off a torn tail so that new records follow the last valid one.
	if err := f.Truncate(good); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(good, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	w.log, w.w = f, bufio.NewWriterSize(f, 64<<10)
	w.lastIdx = w.snap.Index
	if len(entries) > 0 {
		w.lastIdx = entries[len(entries)-1].Index
	}
	return entries, nil
}

// replay reads all valid records and rebuilds the log, which starts at the
// index of the first record (1, or just after a snapshot once the log has been
// rewritten). It returns the offset just past the last valid record.
func replay(f *os.File) ([]raft.Entry, int64, error) {
	r := bufio.NewReader(f)
	var entries []raft.Entry
	var off int64
	var hdr [8]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return entries, off, nil // clean EOF or torn header
		}
		n := binary.LittleEndian.Uint32(hdr[0:4])
		sum := binary.LittleEndian.Uint32(hdr[4:8])
		if n > maxRecord {
			return entries, off, nil
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			return entries, off, nil // torn payload
		}
		if crc32.Checksum(payload, castagnoli) != sum {
			return entries, off, nil // corrupted tail
		}
		e, err := decodeEntry(payload)
		if err != nil {
			return nil, 0, fmt.Errorf("wal: record at offset %d: %w", off, err)
		}
		if len(entries) == 0 {
			entries = append(entries, e)
		} else {
			first := entries[0].Index
			if e.Index < first || e.Index > first+uint64(len(entries)) {
				return nil, 0, fmt.Errorf("wal: entry %d outside log [%d, %d] (offset %d)", e.Index, first, first+uint64(len(entries))-1, off)
			}
			entries = append(entries[:e.Index-first], e)
		}
		off += int64(8 + n)
	}
}

// afterSnapshot keeps the entries that follow snap. Like a raft follower, it
// keeps them only if the log holds the snapshot's last entry with the same
// term; otherwise the log had diverged and the snapshot replaces all of it.
func afterSnapshot(entries []raft.Entry, snap raft.Snapshot) ([]raft.Entry, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	first := entries[0].Index
	switch {
	case snap.Index == 0 && first != 1:
		return nil, fmt.Errorf("wal: log starts at %d without a snapshot", first)
	case first > snap.Index+1:
		return nil, fmt.Errorf("wal: log starts at %d, snapshot ends at %d", first, snap.Index)
	case first == snap.Index+1:
		return entries, nil // already rewritten after this snapshot
	}
	last := first + uint64(len(entries)) - 1
	if snap.Index > last || entries[snap.Index-first].Term != snap.Term {
		return nil, nil
	}
	return append([]raft.Entry(nil), entries[snap.Index-first+1:]...), nil
}

// Save persists a raft.Ready's HardState and Entries. It returns only once
// the data is on stable storage (unless NoSync).
func (w *WAL) Save(hs *raft.HardState, entries []raft.Entry) error {
	if len(entries) > 0 {
		if first := entries[0].Index; first > w.lastIdx+1 || first <= w.snap.Index {
			return fmt.Errorf("wal: appending %d after %d (snapshot at %d)", first, w.lastIdx, w.snap.Index)
		}
		for _, e := range entries {
			w.writeRecord(e)
		}
		w.lastIdx = entries[len(entries)-1].Index
		if err := w.w.Flush(); err != nil {
			return err
		}
		if !w.noSync {
			if err := w.log.Sync(); err != nil {
				return err
			}
		}
	}
	if hs != nil {
		return w.writeHardState(*hs)
	}
	return nil
}

func (w *WAL) writeRecord(e raft.Entry) {
	payload := encodeEntry(e)
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[4:8], crc32.Checksum(payload, castagnoli))
	w.w.Write(hdr[:])
	w.w.Write(payload)
}

// SaveSnapshot persists snap, then rewrites the log without the entries it
// covers (or, for a snapshot that conflicts with the log, without any entry).
func (w *WAL) SaveSnapshot(snap raft.Snapshot) error {
	if snap.Index <= w.snap.Index {
		return nil
	}
	b := make([]byte, 20+len(snap.Data))
	binary.LittleEndian.PutUint64(b[0:8], snap.Index)
	binary.LittleEndian.PutUint64(b[8:16], snap.Term)
	binary.LittleEndian.PutUint32(b[16:20], crc32.Checksum(snap.Data, castagnoli))
	copy(b[20:], snap.Data)
	if err := w.replaceFile("snapshot", b); err != nil {
		return err
	}
	w.snap = snap

	// Rewrite the log. The open file must be closed first: Windows cannot
	// rename over an open file.
	if err := w.w.Flush(); err != nil {
		return err
	}
	if _, err := w.log.Seek(0, io.SeekStart); err != nil {
		return err
	}
	entries, _, err := replay(w.log)
	if err == nil {
		entries, err = afterSnapshot(entries, snap)
	}
	if err != nil {
		return err
	}
	if err := w.log.Close(); err != nil {
		return err
	}
	tmp := filepath.Join(w.dir, "log.tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w.w = bufio.NewWriterSize(f, 64<<10)
	for _, e := range entries {
		w.writeRecord(e)
	}
	if err := w.w.Flush(); err != nil {
		f.Close()
		return err
	}
	if !w.noSync {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(w.dir, "log")); err != nil {
		return err
	}
	_, err = w.openLog()
	return err
}

func (w *WAL) Close() error {
	w.w.Flush()
	return w.log.Close()
}

func (w *WAL) writeHardState(hs raft.HardState) error {
	var b [16]byte
	binary.LittleEndian.PutUint64(b[0:8], hs.Term)
	binary.LittleEndian.PutUint64(b[8:16], uint64(hs.Vote))
	return w.replaceFile("hardstate", b[:])
}

// replaceFile writes data to name atomically: after a crash, the directory
// holds either the old content or the new one.
func (w *WAL) replaceFile(name string, data []byte) error {
	tmp := filepath.Join(w.dir, name+".tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if !w.noSync {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(w.dir, name))
}

func readHardState(path string) (raft.HardState, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return raft.HardState{}, nil
	}
	if err != nil {
		return raft.HardState{}, err
	}
	if len(b) != 16 {
		return raft.HardState{}, fmt.Errorf("wal: hardstate has %d bytes", len(b))
	}
	return raft.HardState{
		Term: binary.LittleEndian.Uint64(b[0:8]),
		Vote: raft.NodeID(binary.LittleEndian.Uint64(b[8:16])),
	}, nil
}

func readSnapshot(path string) (raft.Snapshot, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return raft.Snapshot{}, nil
	}
	if err != nil {
		return raft.Snapshot{}, err
	}
	// The file is replaced atomically, so a bad checksum is real corruption,
	// not a torn write: refuse to start rather than silently lose state.
	if len(b) < 20 || crc32.Checksum(b[20:], castagnoli) != binary.LittleEndian.Uint32(b[16:20]) {
		return raft.Snapshot{}, fmt.Errorf("wal: snapshot file is corrupted")
	}
	return raft.Snapshot{
		Index: binary.LittleEndian.Uint64(b[0:8]),
		Term:  binary.LittleEndian.Uint64(b[8:16]),
		Data:  append([]byte(nil), b[20:]...),
	}, nil
}

func encodeEntry(e raft.Entry) []byte {
	b := make([]byte, 16+len(e.Data))
	binary.LittleEndian.PutUint64(b[0:8], e.Term)
	binary.LittleEndian.PutUint64(b[8:16], e.Index)
	copy(b[16:], e.Data)
	return b
}

func decodeEntry(b []byte) (raft.Entry, error) {
	if len(b) < 16 {
		return raft.Entry{}, errors.New("short entry")
	}
	e := raft.Entry{Term: binary.LittleEndian.Uint64(b[0:8]), Index: binary.LittleEndian.Uint64(b[8:16])}
	if len(b) > 16 {
		e.Data = append([]byte(nil), b[16:]...)
	}
	return e, nil
}
