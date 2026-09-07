package client

import (
	"sync"
	"time"
)

const (
	maxAttempts = 10
	retryDelay  = 50 * time.Millisecond
	callTimeout = 300 * time.Millisecond
)

// Client is an EXTERNAL caller's view of the cluster — it is not itself a
// cluster member. It knows every node's address up front (static membership;
// dynamic membership is a later phase) and keeps a running guess at who the
// leader is, updating that guess as it follows redirects or discovers a node
// is unreachable.
type Client struct {
	mu      sync.Mutex
	addrs   map[int]string
	ids     []int
	leader  int // -1 = unknown
	rrIndex int
}

func New(addrs map[int]string) *Client {
	ids := make([]int, 0, len(addrs))
	for id := range addrs {
		ids = append(ids, id)
	}
	return &Client{addrs: addrs, ids: ids, leader: -1}
}

// currentTarget returns who to try next: the current leader guess if there is
// one, otherwise the next id in round-robin order.
func (c *Client) currentTarget() (id int, addr string, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leader != -1 {
		if a, exists := c.addrs[c.leader]; exists {
			return c.leader, a, true
		}
	}
	if len(c.ids) == 0 {
		return 0, "", false
	}
	id = c.ids[c.rrIndex%len(c.ids)]
	return id, c.addrs[id], true
}

// advance moves the round-robin cursor forward and drops the leader guess —
// call this after the current target turns out unreachable.
func (c *Client) advance() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rrIndex++
	c.leader = -1
}

// setLeaderHint updates our guess from a KVService reply. A hint of -1 means
// the node we asked doesn't know either (e.g. an election is in progress) —
// assigning it directly is enough: the next currentTarget() call correctly
// falls back to round-robin instead of retrying the same uninformative node.
func (c *Client) setLeaderHint(hint int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.leader = hint
}

// noteWorking records that id successfully handled a request — worth
// remembering as our new leader guess so the next call goes straight there.
func (c *Client) noteWorking(id int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.leader = id
}
