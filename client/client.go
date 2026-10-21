// Package client talks to a Quorum cluster over HTTP. It finds the leader,
// follows redirects, and retries failed attempts with the same (client, seq)
// so that a command is applied at most once even when a reply is lost.
package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kabilaymen/quorum/kv"
	"github.com/kabilaymen/quorum/raft"
)

type Client struct {
	endpoints map[raft.NodeID]string // node id -> base URL
	order     []raft.NodeID
	http      *http.Client
	id        string

	mu     sync.Mutex // one command in flight per client (required by the dedup table)
	seq    uint64
	leader raft.NodeID
}

func New(endpoints map[raft.NodeID]string) *Client {
	c := &Client{endpoints: endpoints, http: &http.Client{Timeout: 2 * time.Second, Transport: sharedTransport}, id: randomID()}
	for id := range endpoints {
		c.order = append(c.order, id)
	}
	return c
}

func randomID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func (c *Client) Get(ctx context.Context, key string) (kv.Result, error) {
	return c.do(ctx, http.MethodGet, key, "")
}

func (c *Client) Put(ctx context.Context, key, value string) (kv.Result, error) {
	return c.do(ctx, http.MethodPut, key, value)
}

func (c *Client) Delete(ctx context.Context, key string) (kv.Result, error) {
	return c.do(ctx, http.MethodDelete, key, "")
}

func (c *Client) do(ctx context.Context, method, key, value string) (kv.Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	seq := c.seq
	backoff := 10 * time.Millisecond
	tried := 0
	for {
		target := c.leader
		if target == 0 {
			target = c.order[tried%len(c.order)]
		}
		tried++
		res, hint, err := c.attempt(ctx, target, method, key, value, seq)
		if err == nil {
			c.leader = target
			return res, nil
		}
		c.leader = hint // 0 if unknown: next attempt tries the next node
		if ctx.Err() != nil {
			return kv.Result{}, fmt.Errorf("%s %s: %w (last error: %v)", method, key, ctx.Err(), err)
		}
		if hint == 0 {
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
			}
			backoff = min(2*backoff, 200*time.Millisecond)
		}
	}
}

var errRetry = errors.New("retry")

// sharedTransport keeps connections alive across all clients of the process.
// The default keeps only 2 idle connections per host, so concurrent clients
// would reopen a TCP connection for most requests.
var sharedTransport = func() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 512
	t.MaxIdleConnsPerHost = 256
	return t
}()

func (c *Client) attempt(ctx context.Context, node raft.NodeID, method, key, value string, seq uint64) (kv.Result, raft.NodeID, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.endpoints[node]+"/kv/"+key, strings.NewReader(value))
	if err != nil {
		return kv.Result{}, 0, err
	}
	req.Header.Set("X-Client-Id", c.id)
	req.Header.Set("X-Seq", strconv.FormatUint(seq, 10))
	resp, err := c.http.Do(req)
	if err != nil {
		return kv.Result{}, 0, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		var res kv.Result
		err := json.NewDecoder(resp.Body).Decode(&res)
		return res, 0, err
	case http.StatusMisdirectedRequest:
		io.Copy(io.Discard, resp.Body)
		hint, _ := strconv.ParseUint(resp.Header.Get("X-Leader"), 10, 64)
		if hint == uint64(node) {
			hint = 0
		}
		return kv.Result{}, raft.NodeID(hint), errRetry
	default:
		body, _ := io.ReadAll(resp.Body)
		return kv.Result{}, 0, fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
}
