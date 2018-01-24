// Copyright 2017 Canonical Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rafttest

import (
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// FSM create a dummy FSMs.
func FSM() raft.FSM {
	return &fsm{}
}

// FSMs creates the given number of dummy FSMs.
func FSMs(n int) []raft.FSM {
	fsms := make([]raft.FSM, n)
	for i := range fsms {
		fsms[i] = FSM()
	}
	return fsms
}

// FSMWatcher creates watchers for the given FSMs.
func FSMWatcher(t testing.TB, fsms []raft.FSM) *FSMWatcherAPI {
	api := &FSMWatcherAPI{
		t:        t,
		wrappers: make([]*fsmWrapper, len(fsms)),
	}
	for i, fsm := range fsms {
		wrapper := &fsmWrapper{t: t, fsm: fsm}
		api.wrappers[i] = wrapper
		fsms[i] = wrapper
	}
	return api
}

// FSMWatcherAPI implements methods to wait for the underlying FSMs to reach
// certain states.
type FSMWatcherAPI struct {
	t        testing.TB
	wrappers []*fsmWrapper
}

// LastIndex returns the last index applied by the FSM with the given
// index.
func (w *FSMWatcherAPI) LastIndex(i int) uint64 {
	return w.wrappers[i].index
}

// LastSnapshot returns the last snapshot performed by the FSM with the given
// index.
func (w *FSMWatcherAPI) LastSnapshot(i int) uint64 {
	return w.wrappers[i].snapshots
}

// LastRestore returns the last restore performed by the FSM with the given
// index.
func (w *FSMWatcherAPI) LastRestore(i int) uint64 {
	return w.wrappers[i].restores
}

// WaitIndex blocks until the FSM with the given index has reached at least the
// given log.
//
// If the timeout expires, test will fail.
func (w *FSMWatcherAPI) WaitIndex(i int, index uint64, timeout time.Duration) {
	helper, ok := w.t.(testingHelper)
	if ok {
		helper.Helper()
	}

	wrapper := w.wrappers[i]
	check := func() bool {
		wrapper.mu.Lock()
		defer wrapper.mu.Unlock()
		return wrapper.index >= index
	}

	message := fmt.Sprintf("fsm %d did not reach index %d", i, index)
	wait(w.t, check, 25*time.Millisecond, timeout, message)
}

// WaitSnapshot blocks until the FSM with the given index has reached at least the
// given snapshot number.
//
// If the timeout expires, test will fail.
func (w *FSMWatcherAPI) WaitSnapshot(i int, n uint64, timeout time.Duration) {
	helper, ok := w.t.(testingHelper)
	if ok {
		helper.Helper()
	}

	w.t.Logf("%d: fsm: wait for snapshot %d", i, n)
	wrapper := w.wrappers[i]
	check := func() bool {
		wrapper.mu.Lock()
		defer wrapper.mu.Unlock()
		return wrapper.snapshots >= n
	}

	message := fmt.Sprintf("fsm %d did not reach snapshot %d", i, n)
	wait(w.t, check, 25*time.Millisecond, timeout, message)
}

// WaitRestore blocks until the FSM with the given index has reached at least the
// given number of snapshot restores.
//
// If the timeout expires, test will fail.
func (w *FSMWatcherAPI) WaitRestore(i int, n uint64, timeout time.Duration) {
	helper, ok := w.t.(testingHelper)
	if ok {
		helper.Helper()
	}

	wrapper := w.wrappers[i]
	check := func() bool {
		wrapper.mu.Lock()
		defer wrapper.mu.Unlock()
		return wrapper.restores >= n
	}

	message := fmt.Sprintf("fsm %d did not reach restore %d", i, n)
	wait(w.t, check, 25*time.Millisecond, timeout, message)
}

// fsm is a dummy raft finite state machine that does nothing and
// always no-ops.
type fsm struct{}

// Apply always return a nil error without doing anything.
func (f *fsm) Apply(*raft.Log) interface{} { return nil }

// Snapshot always return a dummy snapshot and no error without doing
// anything.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) { return &fsmSnapshot{}, nil }

// Restore always return a nil error without reading anything from
// the reader.
func (f *fsm) Restore(io.ReadCloser) error { return nil }

// fsmSnapshot a dummy implementation of an fsm snapshot.
type fsmSnapshot struct{}

// Persist always return a nil error without writing anything
// to the sink.
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error { return nil }

// Release is a no-op.
func (s *fsmSnapshot) Release() {}

// Wraps a raft.FSM, tracking logs, snapshots and restores.
type fsmWrapper struct {
	t         testing.TB
	fsm       raft.FSM
	index     uint64
	snapshots uint64
	restores  uint64
	mu        sync.Mutex
}

// Apply always return a nil error without doing anything.
func (f *fsmWrapper) Apply(log *raft.Log) interface{} {
	f.mu.Lock()
	f.index = log.Index
	f.mu.Unlock()

	return f.fsm.Apply(log)
}

// Snapshot always return a dummy snapshot and no error without doing
// anything.
func (f *fsmWrapper) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	f.snapshots++
	f.mu.Unlock()

	return f.fsm.Snapshot()
}

// Restore always return a nil error without reading anything from
// the reader.
func (f *fsmWrapper) Restore(reader io.ReadCloser) error {
	f.mu.Lock()
	f.restores++
	f.mu.Unlock()

	return f.fsm.Restore(reader)
}
