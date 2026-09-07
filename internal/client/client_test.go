package client
import (
	"fmt" 
	"kestrel/internal/rpc"
	"time"
)

// This is your Phase 10 implementation file: three methods. Put and Delete
// share an identical shape (chase the leader, following redirects until one
// succeeds). Get does NOT need that shape at all — see PHASE10.md for why
// that asymmetry is correct, not an oversight, and directly follows from a
// decision made back in Phase 8.
//
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
