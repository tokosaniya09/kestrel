package client

import (
	"net"
	"net/rpc"
)

// tryCall makes ONE RPC attempt against address — dial with a timeout,
// invoke, report success. The same mechanical pattern as Phase 9's
// RPCTransport.call, provided since it isn't new material.
func tryCall(address, serviceMethod string, args, reply interface{}) bool {
	conn, err := net.DialTimeout("tcp", address, callTimeout)
	if err != nil {
		return false
	}
	c := rpc.NewClient(conn)
	defer c.Close()
	return c.Call(serviceMethod, args, reply) == nil
}