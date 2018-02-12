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
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

// fsmsWatcher implements methods to wait for the underlying set of FSMs to
// reach certain states.
type fsmsWatcher struct {
	wrappers []*fsmWrapper
}

// Creates a new fsmsWatcher for watching the given FSMs.
func newFSMsWatcher(fsms []raft.FSM) *fsmsWatcher {
	watcher := &fsmsWatcher{
		wrappers: make([]*fsmWrapper, len(fsms)),
	}

	for i, fsm := range fsms {
		watcher.wrappers[i] = newFSMWrapper(fsm)
	}

	return watcher
}

// FSMs converts the internal wrappers to a slice of raft.FSM interfaces.
func (w *fsmsWatcher) FSMs() []raft.FSM {
	fsms := make([]raft.FSM, len(w.wrappers))
	for i, wrapper := range w.wrappers {
		fsms[i] = wrapper
	}
	return fsms
}

// ApplyHook sets a hook that gets invoked when the FSM with the given index is
// about to apply a new log entry.
func (w *fsmsWatcher) ApplyHook(i int, hook func(uint64)) {
	wrapper := w.wrappers[i]

	wrapper.mu.Lock()
	defer wrapper.mu.Unlock()

	wrapper.applyHook = hook
}

// ApplyIndex returns the index of the last log applied by the given FSM.
func (w *fsmsWatcher) ApplyIndex(i int) uint64 {
	wrapper := w.wrappers[i]

	wrapper.mu.RLock()
	defer wrapper.mu.RUnlock()

	return wrapper.applyIndex
}

// SnapshotHook sets a hook that gets invoked when the FSM with the given index
// has performed a snapshot and has finished to persist it.
func (w *fsmsWatcher) SnapshotHook(i int, hook func()) {
	wrapper := w.wrappers[i]

	wrapper.mu.Lock()
	defer wrapper.mu.Unlock()

	wrapper.snapshotHook = hook
}

// SnapshotCount returns the number of snapshots performed by the given FSM.
func (w *fsmsWatcher) SnapshotCount(i int) int {
	wrapper := w.wrappers[i]

	wrapper.mu.RLock()
	defer wrapper.mu.RUnlock()

	return wrapper.snapshotCount
}

// RestoreHook sets a hook that gets invoked when the FSM with the given index
// has restored a snapshot.
func (w *fsmsWatcher) RestoreHook(i int, hook func()) {
	wrapper := w.wrappers[i]

	wrapper.mu.Lock()
	defer wrapper.mu.Unlock()

	wrapper.restoreHook = hook
}

// RestoreCount returns the number of restores performed by the given FSM.
func (w *fsmsWatcher) RestoreCount(i int) int {
	wrapper := w.wrappers[i]

	wrapper.mu.RLock()
	defer wrapper.mu.RUnlock()

	return wrapper.restoreCount
}

// Wraps a raft.FSM, adding notifications for logs, snapshots and restores.
type fsmWrapper struct {
	fsm raft.FSM // Wrapped FSM

	// Hooks that are invoked when an FSM operation is about to be
	// performed.
	applyHook    func(uint64)
	snapshotHook func()
	restoreHook  func()

	// Trackers for the operations performed so far by the FSM.
	applyIndex    uint64
	snapshotCount int
	restoreCount  int

	// Serialize access to internal state.
	mu sync.RWMutex
}

func newFSMWrapper(fsm raft.FSM) *fsmWrapper {
	return &fsmWrapper{
		fsm: fsm,
	}
}

func (f *fsmWrapper) Apply(log *raft.Log) interface{} {
	f.mu.Lock()
	f.applyIndex = log.Index
	hook := f.applyHook
	f.mu.Unlock()

	if hook != nil {
		hook(log.Index)
	}

	return f.fsm.Apply(log)
}

// Snapshot always return a dummy snapshot and no error without doing
// anything.
func (f *fsmWrapper) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	hook := f.snapshotHook
	f.mu.RUnlock()

	snapshot, err := f.fsm.Snapshot()

	if snapshot != nil {
		snapshot = &fsmSnapshotWrapper{
			wrapper:  f,
			snapshot: snapshot,
			hook:     hook,
		}
	}

	return snapshot, err
}

// Restore always return a nil error without reading anything from
// the reader.
func (f *fsmWrapper) Restore(reader io.ReadCloser) error {
	f.mu.Lock()
	f.restoreCount++
	hook := f.restoreHook
	f.mu.Unlock()

	err := f.fsm.Restore(reader)

	if hook != nil {
		hook()
	}

	return err
}

type fsmSnapshotWrapper struct {
	wrapper  *fsmWrapper
	snapshot raft.FSMSnapshot
	hook     func()
}

func (s *fsmSnapshotWrapper) Persist(sink raft.SnapshotSink) error {
	err := s.snapshot.Persist(sink)

	s.wrapper.mu.Lock()
	s.wrapper.snapshotCount++
	s.wrapper.mu.Unlock()

	if s.hook != nil {
		s.hook()
	}
	return err
}

func (s *fsmSnapshotWrapper) Release() {}
