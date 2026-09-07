package config

import (
	"fmt"
	"strconv"
	"strings"
)

// ParsePeers turns "0=127.0.0.1:9000,1=127.0.0.1:9001" into
// {0: "127.0.0.1:9000", 1: "127.0.0.1:9001"}.
//
// Lives here rather than in either cmd/ binary because both kestrel-server
// and kestrel-cli need it, and two `package main`s can't import each other.
func ParsePeers(s string) (map[int]string, error) {
	addrs := map[int]string{}
	for _, entry := range strings.Split(s, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		idStr, addr, found := strings.Cut(entry, "=")
		if !found {
			return nil, fmt.Errorf("entry %q is not of the form id=host:port", entry)
		}
		peerID, err := strconv.Atoi(strings.TrimSpace(idStr))
		if err != nil {
			return nil, fmt.Errorf("entry %q has a non-numeric id: %v", entry, err)
		}
		addr = strings.TrimSpace(addr)
		if addr == "" {
			return nil, fmt.Errorf("entry %q has an empty address", entry)
		}
		if _, dup := addrs[peerID]; dup {
			return nil, fmt.Errorf("id %d appears more than once", peerID)
		}
		addrs[peerID] = addr
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no peers given")
	}
	return addrs, nil
}
