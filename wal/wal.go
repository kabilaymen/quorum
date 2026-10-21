// Package wal persists a raft node's HardState and log.
//
// Layout of the data directory:
//
//	hardstate   term + vote, replaced atomically (write temp file, fsync, rename)
//	log         append-only sequence of records: [len u32][crc32c u32][payload]
//
// A log record holds one entry. Truncation needs no special record: an entry
// whose index is not past the current end implicitly discards everything from
// that index on, which is exactly what Raft does when a follower's log
// conflicts with the leader's. On open, a torn or corrupted tail (crash in the
// middle of a write) is detected by its length or checksum and cut off.
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
	noSync  bool
}

type Options struct {
	// NoSync skips fsync (tests and benchmarks only: a crash can lose writes).
	NoSync bool
}

// Open loads the persisted state from dir (created if missing) and returns a
// WAL positioned to append after it.
func Open(dir string, opts Options) (*WAL, raft.HardState, []raft.Entry, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, raft.HardState{}, nil, err
	}
	hs, err := readHardState(filepath.Join(dir, "hardstate"))
	if err != nil {
		return nil, raft.HardState{}, nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "log"), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, raft.HardState{}, nil, err
	}
	entries, good, err := replay(f)
	if err != nil {
		f.Close()
		return nil, raft.HardState{}, nil, err
	}
	// Cut off a torn tail so that new records follow the last valid one.
	if err := f.Truncate(good); err != nil {
		f.Close()
		return nil, raft.HardState{}, nil, err
	}
	if _, err := f.Seek(good, io.SeekStart); err != nil {
		f.Close()
		return nil, raft.HardState{}, nil, err
	}
	w := &WAL{dir: dir, log: f, w: bufio.NewWriterSize(f, 64<<10), lastIdx: uint64(len(entries)), noSync: opts.NoSync}
	return w, hs, entries, nil
}

// replay reads all valid records and rebuilds the log. It returns the offset
// just past the last valid record.
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
		if e.Index == 0 || e.Index > uint64(len(entries))+1 {
			return nil, 0, fmt.Errorf("wal: entry %d after log end %d (offset %d)", e.Index, len(entries), off)
		}
		entries = append(entries[:e.Index-1], e)
		off += int64(8 + n)
	}
}

// Save persists a raft.Ready's HardState and Entries. It returns only once
// the data is on stable storage (unless NoSync).
func (w *WAL) Save(hs *raft.HardState, entries []raft.Entry) error {
	if len(entries) > 0 {
		if first := entries[0].Index; first > w.lastIdx+1 {
			return fmt.Errorf("wal: gap: appending %d after %d", first, w.lastIdx)
		}
		for _, e := range entries {
			payload := encodeEntry(e)
			var hdr [8]byte
			binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload)))
			binary.LittleEndian.PutUint32(hdr[4:8], crc32.Checksum(payload, castagnoli))
			w.w.Write(hdr[:])
			w.w.Write(payload)
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

func (w *WAL) Close() error {
	w.w.Flush()
	return w.log.Close()
}

func (w *WAL) writeHardState(hs raft.HardState) error {
	var b [16]byte
	binary.LittleEndian.PutUint64(b[0:8], hs.Term)
	binary.LittleEndian.PutUint64(b[8:16], uint64(hs.Vote))
	tmp := filepath.Join(w.dir, "hardstate.tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(b[:]); err != nil {
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
	// rename is atomic: after a crash we see either the old or the new state.
	return os.Rename(tmp, filepath.Join(w.dir, "hardstate"))
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
