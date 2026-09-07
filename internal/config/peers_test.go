package config

import "testing"

func TestParsePeersValid(t *testing.T) {
	addrs, err := ParsePeers("0=127.0.0.1:9000,1=127.0.0.1:9001, 2=127.0.0.1:9002")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(addrs) != 3 {
		t.Fatalf("got %d peers, want 3", len(addrs))
	}
	if addrs[2] != "127.0.0.1:9002" {
		t.Fatalf("peer 2 = %q, want 127.0.0.1:9002 (whitespace should be trimmed)", addrs[2])
	}
}

func TestParsePeersRejectsBadInput(t *testing.T) {
	cases := map[string]string{
		"missing equals": "0-127.0.0.1:9000",
		"non-numeric id": "a=127.0.0.1:9000",
		"empty address":  "0=",
		"duplicate id":   "0=127.0.0.1:9000,0=127.0.0.1:9001",
		"empty string":   "",
	}
	for name, input := range cases {
		if _, err := ParsePeers(input); err == nil {
			t.Errorf("%s: expected an error for %q, got none", name, input)
		}
	}
}
