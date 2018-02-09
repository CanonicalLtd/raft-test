// Copied from freeekanayaka:fix-inmem-snapshot-open-twice to fix #269

package core

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"sync"
	"time"

	"github.com/hashicorp/go-msgpack/codec"
	"github.com/hashicorp/raft"
)

// InmemSnapshotStore implements the SnapshotStore interface and
// retains only the most recent snapshot
type InmemSnapshotStore struct {
	latest      *InmemSnapshotSink
	hasSnapshot bool
	sync.RWMutex
}

// InmemSnapshotSink implements SnapshotSink in memory
type InmemSnapshotSink struct {
	meta     raft.SnapshotMeta
	contents *bytes.Buffer
}

// NewInmemSnapshotStore creates a blank new InmemSnapshotStore
func NewInmemSnapshotStore() *InmemSnapshotStore {
	return &InmemSnapshotStore{
		latest: &InmemSnapshotSink{
			contents: &bytes.Buffer{},
		},
	}
}

// Create replaces the stored snapshot with a new one using the given args
func (m *InmemSnapshotStore) Create(version raft.SnapshotVersion, index, term uint64,
	configuration raft.Configuration, configurationIndex uint64, trans raft.Transport) (raft.SnapshotSink, error) {
	// We only support version 1 snapshots at this time.
	if version != 1 {
		return nil, fmt.Errorf("unsupported snapshot version %d", version)
	}

	name := snapshotName(term, index)

	m.Lock()
	defer m.Unlock()

	sink := &InmemSnapshotSink{
		meta: raft.SnapshotMeta{
			Version:            version,
			ID:                 name,
			Index:              index,
			Term:               term,
			Peers:              encodePeers(configuration, trans),
			Configuration:      configuration,
			ConfigurationIndex: configurationIndex,
		},
		contents: &bytes.Buffer{},
	}
	m.hasSnapshot = true
	m.latest = sink

	return sink, nil
}

// List returns the latest snapshot taken
func (m *InmemSnapshotStore) List() ([]*raft.SnapshotMeta, error) {
	m.RLock()
	defer m.RUnlock()

	if !m.hasSnapshot {
		return []*raft.SnapshotMeta{}, nil
	}
	return []*raft.SnapshotMeta{&m.latest.meta}, nil
}

// Open wraps an io.ReadCloser around the snapshot contents
func (m *InmemSnapshotStore) Open(id string) (*raft.SnapshotMeta, io.ReadCloser, error) {
	m.RLock()
	defer m.RUnlock()

	if m.latest.meta.ID != id {
		return nil, nil, fmt.Errorf("[ERR] snapshot: failed to open snapshot id: %s", id)
	}

	// Make a copy of the contents, since a bytes.Buffer can only be read
	// once.
	contents := bytes.NewBuffer(m.latest.contents.Bytes())
	return &m.latest.meta, ioutil.NopCloser(contents), nil
}

// Write appends the given bytes to the snapshot contents
func (s *InmemSnapshotSink) Write(p []byte) (n int, err error) {
	written, err := io.Copy(s.contents, bytes.NewReader(p))
	s.meta.Size += written
	return int(written), err
}

// Close updates the Size and is otherwise a no-op
func (s *InmemSnapshotSink) Close() error {
	return nil
}

func (s *InmemSnapshotSink) ID() string {
	return s.meta.ID
}

func (s *InmemSnapshotSink) Cancel() error {
	return nil
}

// encodePeers is used to serialize a Configuration into the old peers format.
// This is here for backwards compatibility when operating with a mix of old
// servers and should be removed once we deprecate support for protocol version 1.
func encodePeers(configuration raft.Configuration, trans raft.Transport) []byte {
	// Gather up all the voters, other suffrage types are not supported by
	// this data format.
	var encPeers [][]byte
	for _, server := range configuration.Servers {
		if server.Suffrage == raft.Voter {
			encPeers = append(encPeers, trans.EncodePeer(server.ID, server.Address))
		}
	}

	// Encode the entire array.
	buf, err := encodeMsgPack(encPeers)
	if err != nil {
		panic(fmt.Errorf("failed to encode peers: %v", err))
	}

	return buf.Bytes()
}

// Encode writes an encoded object to a new bytes buffer.
func encodeMsgPack(in interface{}) (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(nil)
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(buf, &hd)
	err := enc.Encode(in)
	return buf, err
}

// snapshotName generates a name for the snapshot.
func snapshotName(term, index uint64) string {
	now := time.Now()
	msec := now.UnixNano() / int64(time.Millisecond)
	return fmt.Sprintf("%d-%d-%d", term, index, msec)
}
