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

// BeforeApply sets a hook that gets invoked when the FSM with the given index is
// about to apply the log command with the given index.
func (w *fsmsWatcher) BeforeApply(i int, index uint64, hook func()) {
	wrapper := w.wrappers[i]

	wrapper.mu.Lock()
	defer wrapper.mu.Unlock()

	wrapper.beforeApply[index] = hook
}

// AfterApply sets a hook that gets invoked when the FSM with the given index has
// applied a new log entry.
func (w *fsmsWatcher) AfterApply(i int, index uint64, hook func()) {
	wrapper := w.wrappers[i]

	wrapper.mu.Lock()
	defer wrapper.mu.Unlock()

	wrapper.afterApply[index] = hook
}

// AppliedIndex returns the index of the last log applied by the given FSM.
func (w *fsmsWatcher) AppliedIndex(i int) uint64 {
	wrapper := w.wrappers[i]

	wrapper.mu.RLock()
	defer wrapper.mu.RUnlock()

	return wrapper.applyIndex
}

// BeforeSnapshot sets a hook that gets invoked when the FSM with the given
// index is about to perform the n'th snapshot.
func (w *fsmsWatcher) BeforeSnapshot(i int, n int, hook func()) {
	wrapper := w.wrappers[i]

	wrapper.mu.Lock()
	defer wrapper.mu.Unlock()

	wrapper.beforeSnapshot[n] = hook
}

// AfterSnapshot sets a hook that gets invoked when the FSM with the given
// index has performed the n'th snapshot and has finished to persist it.
func (w *fsmsWatcher) AfterSnapshot(i int, n int, hook func()) {
	wrapper := w.wrappers[i]

	wrapper.mu.Lock()
	defer wrapper.mu.Unlock()

	wrapper.afterSnapshot[n] = hook
}

// SnapshotCount returns the number of snapshots performed by the given FSM.
func (w *fsmsWatcher) SnapshotCount(i int) int {
	wrapper := w.wrappers[i]

	wrapper.mu.RLock()

	defer wrapper.mu.RUnlock()

	return wrapper.snapshotCount
}

// BeforeRestore sets a hook that gets invoked when the FSM with the given index is
// about to restore the n'th snapshot.
func (w *fsmsWatcher) BeforeRestore(i int, n int, hook func()) {
	wrapper := w.wrappers[i]

	wrapper.mu.Lock()
	defer wrapper.mu.Unlock()

	wrapper.beforeRestore[n] = hook
}

// AfterRestore sets a hook that gets invoked when the FSM with the given is
// has restored the n'th snapshot.
func (w *fsmsWatcher) AfterRestore(i int, n int, hook func()) {
	wrapper := w.wrappers[i]

	wrapper.mu.Lock()
	defer wrapper.mu.Unlock()

	wrapper.afterRestore[n] = hook
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

	// Hooks that are invoked upon FSM operations.
	beforeApply    map[uint64]func()
	afterApply     map[uint64]func()
	beforeSnapshot map[int]func()
	afterSnapshot  map[int]func()
	beforeRestore  map[int]func()
	afterRestore   map[int]func()

	// Trackers for the operations performed so far by the FSM.
	applyIndex    uint64
	snapshotCount int
	restoreCount  int

	// Serialize access to internal state.
	mu sync.RWMutex
}

func newFSMWrapper(fsm raft.FSM) *fsmWrapper {
	return &fsmWrapper{
		fsm:            fsm,
		beforeApply:    make(map[uint64]func()),
		afterApply:     make(map[uint64]func()),
		beforeSnapshot: make(map[int]func()),
		afterSnapshot:  make(map[int]func()),
		beforeRestore:  make(map[int]func()),
		afterRestore:   make(map[int]func()),
	}
}

func (f *fsmWrapper) Apply(log *raft.Log) interface{} {
	f.mu.RLock()
	before := f.beforeApply[log.Index]
	f.mu.RUnlock()

	if before != nil {
		before()
	}

	result := f.fsm.Apply(log)

	f.mu.Lock()
	f.applyIndex = log.Index
	after := f.afterApply[log.Index]
	f.mu.Unlock()

	if after != nil {
		after()
	}

	return result
}

// Snapshot always return a dummy snapshot and no error without doing
// anything.
func (f *fsmWrapper) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	n := f.snapshotCount + 1
	before := f.beforeSnapshot[n]
	f.mu.RUnlock()

	if before != nil {
		before()
	}

	snapshot, err := f.fsm.Snapshot()

	if snapshot != nil {
		snapshot = &fsmSnapshotWrapper{
			wrapper:  f,
			snapshot: snapshot,
		}
	}

	return snapshot, err
}

// Restore always return a nil error without reading anything from
// the reader.
func (f *fsmWrapper) Restore(reader io.ReadCloser) error {
	f.mu.RLock()
	n := f.restoreCount + 1
	before := f.beforeRestore[n]
	f.mu.RUnlock()

	if before != nil {
		before()
	}

	err := f.fsm.Restore(reader)

	f.mu.Lock()
	f.restoreCount = n
	after := f.afterRestore[n]
	f.mu.Unlock()

	if after != nil {
		after()
	}

	return err
}

type fsmSnapshotWrapper struct {
	wrapper  *fsmWrapper
	snapshot raft.FSMSnapshot
}

func (s *fsmSnapshotWrapper) Persist(sink raft.SnapshotSink) error {
	err := s.snapshot.Persist(sink)

	s.wrapper.mu.Lock()
	n := s.wrapper.snapshotCount + 1
	after := s.wrapper.afterSnapshot[n]
	s.wrapper.snapshotCount = n
	s.wrapper.mu.Unlock()

	if after != nil {
		after()
	}

	return err
}

func (s *fsmSnapshotWrapper) Release() {}
