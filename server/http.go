package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/kabilaymen/quorum/kv"
	"github.com/kabilaymen/quorum/raft"
)

// Handler exposes the client API:
//
//	GET    /kv/{key}        linearizable read
//	PUT    /kv/{key}        body = value
//	DELETE /kv/{key}
//	GET    /status
//	GET    /watch           stream of invalidations (see Event)
//	GET    /metrics         Prometheus metrics
//
// Requests may carry X-Client-Id / X-Seq so that retries are applied once.
// A follower answers 421 with the leader id in X-Leader; clients redirect.
func Handler(s *Server, timeout time.Duration) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /watch", watchHandler(s))
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.m.reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, s.Status())
	})
	do := func(w http.ResponseWriter, r *http.Request, op kv.Op) {
		op.Key = r.PathValue("key")
		op.Client = r.Header.Get("X-Client-Id")
		op.Seq, _ = strconv.ParseUint(r.Header.Get("X-Seq"), 10, 64)
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		res, err := s.Do(ctx, op)
		switch {
		case err == nil:
			writeJSON(w, http.StatusOK, res)
		case errors.Is(err, ErrNotLeader):
			w.Header().Set("X-Leader", strconv.FormatUint(uint64(s.Status().Leader), 10))
			http.Error(w, err.Error(), http.StatusMisdirectedRequest)
		case errors.Is(err, ErrLost), errors.Is(err, ErrUnknown), errors.Is(err, context.DeadlineExceeded):
			// Outcome unknown or not applied: the client retries with the same Seq.
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
	mux.HandleFunc("GET /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		do(w, r, kv.Op{Type: kv.Get})
	})
	mux.HandleFunc("PUT /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		do(w, r, kv.Op{Type: kv.Put, Value: string(body)})
	})
	mux.HandleFunc("DELETE /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		do(w, r, kv.Op{Type: kv.Delete})
	})
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

// LeaderHint parses the X-Leader header of a 421 response.
func LeaderHint(h http.Header) raft.NodeID {
	id, _ := strconv.ParseUint(h.Get("X-Leader"), 10, 64)
	return raft.NodeID(id)
}
