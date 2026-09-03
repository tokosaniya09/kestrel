package raft

import (
	"bytes"
	"encoding/gob"
	"os"
	"sync"
)

// Persister is how a Raft node durably saves and reloads currentTerm, votedFor,
// and its log. Two implementations are provided: MemoryPersister (used by
// tests — fast, no real disk I/O, but still round-trips through real encoding)
// and FilePersister (real disk, atomic write via temp-file + rename, the same
// crash-safety pattern your Layer 1 SSTable writer uses).
type Persister interface {
	Save(state []byte) error
	Load() ([]byte, error) // returns (nil, nil) if nothing has been saved yet
}

// --- MemoryPersister: in-memory, for tests ---

type MemoryPersister struct {
	mu    sync.Mutex
	state []byte
}

func NewMemoryPersister() *MemoryPersister { return &MemoryPersister{} }

func (p *MemoryPersister) Save(state []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state = append([]byte(nil), state...) // copy: never alias the caller's slice
	return nil
}

func (p *MemoryPersister) Load() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.state) == 0 {
		return nil, nil
	}
	return append([]byte(nil), p.state...), nil
}

// --- FilePersister: real disk, for actual deployment ---

type FilePersister struct {
	mu   sync.Mutex
	path string
}

func NewFilePersister(path string) *FilePersister { return &FilePersister{path: path} }

func (p *FilePersister) Save(state []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	tmp := p.path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(state); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, p.path)
}

func (p *FilePersister) Load() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	data, err := os.ReadFile(p.path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	return data, err
}

// --- Encoding: currentTerm + votedFor + log, via Go's gob encoder ---
//
// Layer 1's WAL/SSTable formats were hand-rolled on purpose, to see exactly how
// the bytes work. Here we reach for encoding/gob instead, because LogEntry.Command
// is `interface{}` — its concrete type varies (a string in our tests today; a
// real command struct once Phase 8 wires this to the KV store). Hand-rolling a
// format for an open-ended set of types is real effort with little extra
// learning value; gob handles it, and it's what real Go Raft implementations
// (including the reference MIT 6.824 solution) actually use for exactly this.
//
// gob.Register tells the encoder which concrete type an interface{} value holds.
// Forget to register a type and encoding/decoding it panics — a genuine gotcha,
// and the reason this init() exists. When Phase 8 introduces a real Command
// type, register it here too.
func init() {
	gob.Register("")
}

type persistentState struct {
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
}

func encodeState(term, votedFor int, log []LogEntry) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(persistentState{
		CurrentTerm: term,
		VotedFor:    votedFor,
		Log:         log,
	}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeState(data []byte) (term int, votedFor int, log []LogEntry, err error) {
	var ps persistentState
	if err = gob.NewDecoder(bytes.NewReader(data)).Decode(&ps); err != nil {
		return 0, -1, nil, err
	}
	return ps.CurrentTerm, ps.VotedFor, ps.Log, nil
}
