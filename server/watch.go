package server

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// Event is pushed to watchers on GET /watch, one JSON object per line.
//
//	{"type":"inv","key":"k","version":42}     a write to k was applied at log index 42
//	{"type":"progress","applied":57,"healthy":true}
//
// Progress events are sent every 100 ms. healthy=false means this node does
// not currently know a leader (partitioned, or election in progress): it may
// be missing writes, so clients must stop trusting their cache.
type Event struct {
	Type    string `json:"type"`
	Key     string `json:"key,omitempty"`
	Version uint64 `json:"version,omitempty"`
	Applied uint64 `json:"applied,omitempty"`
	Healthy bool   `json:"healthy,omitempty"`
}

type watchers struct {
	mu   sync.Mutex
	subs map[chan Event]struct{}
	m    *metrics
}

func (w *watchers) add() chan Event {
	ch := make(chan Event, 1024)
	w.mu.Lock()
	if w.subs == nil {
		w.subs = map[chan Event]struct{}{}
	}
	w.subs[ch] = struct{}{}
	w.m.watchers.Set(float64(len(w.subs)))
	w.mu.Unlock()
	return ch
}

func (w *watchers) remove(ch chan Event) {
	w.mu.Lock()
	if _, ok := w.subs[ch]; ok {
		delete(w.subs, ch)
		close(ch)
	}
	w.m.watchers.Set(float64(len(w.subs)))
	w.mu.Unlock()
}

// publish never blocks the raft loop: a watcher that cannot keep up is
// disconnected, and its client drops its cache and resubscribes. Silently
// skipping an invalidation instead would leave stale entries forever.
func (w *watchers) publish(ev Event) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for ch := range w.subs {
		select {
		case ch <- ev:
		default:
			delete(w.subs, ch)
			close(ch)
			w.m.dropped.Inc()
			w.m.watchers.Set(float64(len(w.subs)))
		}
	}
}

// closeAll disconnects every watcher; their clients drop their caches.
func (w *watchers) closeAll() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for ch := range w.subs {
		delete(w.subs, ch)
		close(ch)
	}
	w.m.watchers.Set(0)
}

func watchHandler(s *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		ch := s.watch.add()
		defer s.watch.remove(ch)
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Header().Set("Cache-Control", "no-store")
		enc := json.NewEncoder(w)
		progress := func() error {
			st := s.Status()
			return enc.Encode(Event{Type: "progress", Applied: st.Applied, Healthy: st.Leader != 0})
		}
		if progress() != nil {
			return
		}
		flusher.Flush()
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case ev, ok := <-ch:
				if !ok {
					return // too slow: dropped by publish
				}
				if enc.Encode(ev) != nil {
					return
				}
			case <-tick.C:
				if progress() != nil {
					return
				}
			}
			flusher.Flush()
		}
	}
}
