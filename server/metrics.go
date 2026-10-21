package server

import (
	"github.com/prometheus/client_golang/prometheus"
)

// metrics are registered on a per-server registry so that several nodes can
// live in one process (tests, benchmarks).
type metrics struct {
	reg        *prometheus.Registry
	term       prometheus.Gauge
	isLeader   prometheus.Gauge
	commit     prometheus.Gauge
	applied    prometheus.Gauge
	elections  prometheus.Counter
	walSync    prometheus.Histogram
	requests   *prometheus.HistogramVec
	watchers   prometheus.Gauge
	dropped    prometheus.Counter
	msgsSent   prometheus.Counter
	msgsRecv   prometheus.Counter
	lastLeader bool
}

func newMetrics() *metrics {
	m := &metrics{reg: prometheus.NewRegistry()}
	f := func(c prometheus.Collector) { m.reg.MustRegister(c) }
	m.term = prometheus.NewGauge(prometheus.GaugeOpts{Name: "quorum_raft_term", Help: "Current raft term."})
	m.isLeader = prometheus.NewGauge(prometheus.GaugeOpts{Name: "quorum_raft_is_leader", Help: "1 if this node is the leader."})
	m.commit = prometheus.NewGauge(prometheus.GaugeOpts{Name: "quorum_raft_commit_index", Help: "Highest committed log index."})
	m.applied = prometheus.NewGauge(prometheus.GaugeOpts{Name: "quorum_raft_applied_index", Help: "Highest applied log index."})
	m.elections = prometheus.NewCounter(prometheus.CounterOpts{Name: "quorum_raft_leader_changes_total", Help: "Times this node became leader."})
	m.walSync = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "quorum_wal_save_seconds", Help: "Duration of a WAL save (write + fsync).",
		Buckets: prometheus.ExponentialBuckets(0.00005, 2, 16),
	})
	m.requests = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "quorum_request_seconds", Help: "Client request latency on this node, by operation and outcome.",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 16),
	}, []string{"op", "outcome"})
	m.watchers = prometheus.NewGauge(prometheus.GaugeOpts{Name: "quorum_watch_streams", Help: "Open invalidation streams."})
	m.dropped = prometheus.NewCounter(prometheus.CounterOpts{Name: "quorum_watch_dropped_total", Help: "Watch streams closed because the client was too slow."})
	m.msgsSent = prometheus.NewCounter(prometheus.CounterOpts{Name: "quorum_raft_messages_sent_total", Help: "Raft messages handed to the transport."})
	m.msgsRecv = prometheus.NewCounter(prometheus.CounterOpts{Name: "quorum_raft_messages_received_total", Help: "Raft messages stepped into the node."})
	for _, c := range []prometheus.Collector{m.term, m.isLeader, m.commit, m.applied, m.elections, m.walSync,
		m.requests, m.watchers, m.dropped, m.msgsSent, m.msgsRecv} {
		f(c)
	}
	return m
}

func (m *metrics) observeStatus(st Status) {
	m.term.Set(float64(st.Term))
	m.commit.Set(float64(st.CommitIndex))
	m.applied.Set(float64(st.Applied))
	leader := st.State == "leader"
	if leader {
		m.isLeader.Set(1)
		if !m.lastLeader {
			m.elections.Inc()
		}
	} else {
		m.isLeader.Set(0)
	}
	m.lastLeader = leader
}
