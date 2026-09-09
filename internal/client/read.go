package client

import (
	"fmt"
	"time"

	"kestrel/internal/rpc"
)

// LinearizableGet reads with the strong guarantee: the result reflects every
// write that completed before this call started.
//
// Provided — it's the same chase-the-leader loop as Put/Delete (Phase 10),
// because unlike a plain Get, a linearizable read CAN be redirected: only the
// leader can serve one.
//
// The cost is real: a plain Get is answered immediately by whichever node you
// happen to reach, while this one requires the leader to complete a heartbeat
// round with a majority before it can answer. Strong consistency is not free —
// use Get when a slightly stale read is acceptable, and this when it isn't.
func (c *Client) LinearizableGet(key []byte) ([]byte, bool, error) {
	args := rpc.GetArgs{Key: key, Linearizable: true}
	for attempt := 0; attempt < maxAttempts; attempt++ {
		id, addr, ok := c.currentTarget()
		if !ok {
			return nil, false, fmt.Errorf("no known cluster nodes")
		}

		var reply rpc.GetReply
		if !tryCall(addr, "KVService.Get", args, &reply) {
			c.advance()
			time.Sleep(retryDelay)
			continue
		}
		if reply.Success {
			c.noteWorking(id)
			return reply.Value, reply.Found, nil
		}
		c.setLeaderHint(reply.LeaderHint)
		time.Sleep(retryDelay)
	}
	return nil, false, fmt.Errorf("linearizable get failed after %d attempts", maxAttempts)
}
