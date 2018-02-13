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
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Convert watchers to a slice of raft.FSM interfaces
func TestFSMsWatcher_FSMs(t *testing.T) {
	fsms := FSMs(3)

	watcher := newFSMsWatcher(fsms)

	assert.Len(t, watcher.FSMs(), 3)
}

// The apply hook is invoked when a log is applied.
func TestFSMsWatcher_BeforeApply(t *testing.T) {
	fsms := FSMs(3)

	watcher := newFSMsWatcher(fsms)

	triggered := false
	watcher.BeforeApply(1, 1, func() {
		assert.Equal(t, uint64(0), watcher.AppliedIndex(1))
		triggered = true
	})

	watcher.FSMs()[1].Apply(newLog(1))

	assert.True(t, triggered)

	assert.Equal(t, uint64(0), watcher.AppliedIndex(0))
	assert.Equal(t, uint64(1), watcher.AppliedIndex(1))
	assert.Equal(t, uint64(0), watcher.AppliedIndex(2))
}

// The snapshot hook is invoked when a log is applied.
func TestFSMsWatcher_BeforeSnapshot(t *testing.T) {
	fsms := FSMs(3)

	watcher := newFSMsWatcher(fsms)

	triggered := false

	watcher.BeforeSnapshot(0, 1, func() {
		assert.Equal(t, 0, watcher.SnapshotCount(0))
		triggered = true
	})

	snapshot, err := watcher.FSMs()[0].Snapshot()
	require.NoError(t, err)
	snapshot.Persist(nil)

	assert.True(t, triggered, "hook not triggered")

	assert.Equal(t, 1, watcher.SnapshotCount(0))
	assert.Equal(t, 0, watcher.SnapshotCount(1))
	assert.Equal(t, 0, watcher.SnapshotCount(2))
}

// The restore hook is invoked when a log is applied.
func TestFSMsWatcher_BeforeRestore(t *testing.T) {
	fsms := FSMs(3)

	watcher := newFSMsWatcher(fsms)

	triggered := false

	watcher.BeforeRestore(2, 1, func() {
		assert.Equal(t, 0, watcher.RestoreCount(2))
		triggered = true
	})

	watcher.FSMs()[2].Restore(nil)

	assert.True(t, triggered, "hook not triggered")

	assert.Equal(t, 0, watcher.RestoreCount(0))
	assert.Equal(t, 0, watcher.RestoreCount(1))
	assert.Equal(t, 1, watcher.RestoreCount(2))
}

// If an apply hook is set, it's invoked afer applying a new log.
func TestFSMWrapper_AfterApply(t *testing.T) {
	fsm := FSM()

	wrapper := newFSMWrapper(fsm)

	triggered := false

	wrapper.afterApply[1] = func() {
		triggered = true
		assert.Equal(t, uint64(1), wrapper.applyIndex)
	}

	wrapper.Apply(newLog(1))
	assert.True(t, triggered)
}

// If a snapshot hook is set, it's invoked before performing a snapshot.
func TestFSMWrapper_AfterSnapshot(t *testing.T) {
	fsm := FSM()

	wrapper := newFSMWrapper(fsm)

	triggered := false

	wrapper.afterSnapshot[1] = func() {
		triggered = true
		assert.Equal(t, 1, wrapper.snapshotCount)
	}

	snapshot, err := wrapper.Snapshot()
	require.NoError(t, err)
	snapshot.Persist(nil)

	assert.True(t, triggered, "hook not fired")
}

// If an after restore hook is set, it's invoked after restoring the snapshot.
func TestFSMWrapper_AfterRestore(t *testing.T) {
	fsm := FSM()

	wrapper := newFSMWrapper(fsm)

	triggered := false

	wrapper.afterRestore[1] = func() {
		triggered = true
		assert.Equal(t, 1, wrapper.restoreCount)
	}

	wrapper.Restore(nil)
	assert.True(t, triggered)
}

func newLog(index uint64) *raft.Log {
	return &raft.Log{Index: index}
}
