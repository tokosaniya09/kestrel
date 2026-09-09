package client

import (
	"fmt"
	"time"

	"kestrel/internal/rpc"
)

// Put replicates a write through the cluster, chasing the leader: it retries
// against the current guess, follows redirects when a node says "not me",
// and falls back to round-robin when a node is unreachable.
func (c *Client) Put(key, value []byte) error {
	args := rpc.PutArgs{Key: key, Value: value}
	for attempt := 0; attempt < maxAttempts; attempt++ {
		id, addr, ok := c.currentTarget()
		if !ok {
			return fmt.Errorf("no known cluster nodes")
		}

		var reply rpc.PutReply
		if !tryCall(addr, "KVService.Put", args, &reply) {
			c.advance() // unreachable — try someone else next time
			time.Sleep(retryDelay)
			continue
		}
		if reply.Success {
			c.noteWorking(id)
			return nil
		}
		c.setLeaderHint(reply.LeaderHint) // follow the redirect
		time.Sleep(retryDelay)
	}
	return fmt.Errorf("put failed after %d attempts", maxAttempts)
}

// Delete mirrors Put exactly — same retry/redirect shape, different RPC.
func (c *Client) Delete(key []byte) error {
	args := rpc.DeleteArgs{Key: key}
	for attempt := 0; attempt < maxAttempts; attempt++ {
		id, addr, ok := c.currentTarget()
		if !ok {
			return fmt.Errorf("no known cluster nodes")
		}

		var reply rpc.DeleteReply
		if !tryCall(addr, "KVService.Delete", args, &reply) {
			c.advance()
			time.Sleep(retryDelay)
			continue
		}
		if reply.Success {
			c.noteWorking(id)
			return nil
		}
		c.setLeaderHint(reply.LeaderHint)
		time.Sleep(retryDelay)
	}
	return fmt.Errorf("delete failed after %d attempts", maxAttempts)
}

// Get does not chase the leader: any node answers a read from its own local
// state, so there is no "wrong node" to be redirected away from and the only
// failure to handle is an unreachable one. The tradeoff is that the result may
// be slightly stale — use LinearizableGet when that isn't acceptable.
func (c *Client) Get(key []byte) ([]byte, bool, error) {
	args := rpc.GetArgs{Key: key}
	for attempt := 0; attempt < maxAttempts; attempt++ {
		id, addr, ok := c.currentTarget()
		if !ok {
			return nil, false, fmt.Errorf("no known cluster nodes")
		}

		var reply rpc.GetReply
		if tryCall(addr, "KVService.Get", args, &reply) {
			c.noteWorking(id)
			return reply.Value, reply.Found, nil
		}
		c.advance()
		time.Sleep(retryDelay)
	}
	return nil, false, fmt.Errorf("get failed after %d attempts", maxAttempts)
}