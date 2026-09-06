package raft

import (
	"bytes"
	"encoding/gob"
	"os"
	"sync"
)

type Persister interface {
	Save(state []byte) error
	Load() ([]byte, error)
}

type MemoryPersister struct {
	mu    sync.Mutex
	state []byte
}

func NewMemoryPersister() *MemoryPersister { return &MemoryPersister{} }

func (p *MemoryPersister) Save(state []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state = append([]byte(nil), state...)
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

func init() {
	gob.Register("")
}

type persistentState struct {
	CurrentTerm   int
	VotedFor      int
	Log           []LogEntry
	SnapshotIndex int
	SnapshotTerm  int
	SnapshotData  []byte
}

func encodeState(term, votedFor int, log []LogEntry, snapshotIndex, snapshotTerm int, snapshotData []byte) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(persistentState{
		CurrentTerm:   term,
		VotedFor:      votedFor,
		Log:           log,
		SnapshotIndex: snapshotIndex,
		SnapshotTerm:  snapshotTerm,
		SnapshotData:  snapshotData,
	}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeState(data []byte) (term int, votedFor int, log []LogEntry, snapshotIndex int, snapshotTerm int, snapshotData []byte, err error) {
	var ps persistentState
	if err = gob.NewDecoder(bytes.NewReader(data)).Decode(&ps); err != nil {
		return 0, -1, nil, 0, 0, nil, err
	}
	return ps.CurrentTerm, ps.VotedFor, ps.Log, ps.SnapshotIndex, ps.SnapshotTerm, ps.SnapshotData, nil
}
