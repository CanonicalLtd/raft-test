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

package rafttest_test

import (
	"sync"
	"testing"
	"time"

	"github.com/CanonicalLtd/raft-test"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFSM_Restore(t *testing.T) {
	fsm := rafttest.FSM()
	if err := fsm.Restore(nil); err != nil {
		t.Fatal(err)
	}
}

func TestFSMWatcher_WaitIndex(t *testing.T) {
	fsms := rafttest.FSMs(2)
	watcher := rafttest.FSMWatcher(t, fsms)

	go func() {
		fsms[0].Apply(&raft.Log{Index: 1})
		fsms[0].Apply(&raft.Log{Index: 2})
	}()

	go func() {
		fsms[1].Apply(&raft.Log{Index: 1})
		fsms[1].Apply(&raft.Log{Index: 2})
		fsms[1].Apply(&raft.Log{Index: 3})
	}()

	watcher.WaitIndex(0, 2, time.Second)
	watcher.WaitIndex(1, 3, time.Second)

	assert.Equal(t, uint64(2), watcher.LastIndex(0))
	assert.Equal(t, uint64(3), watcher.LastIndex(1))
}

func TestFSMWatcher_WaitIndexTimeout(t *testing.T) {
	fsms := rafttest.FSMs(2)

	testingT := &testing.T{}
	watcher := rafttest.FSMWatcher(testingT, fsms)

	succeeded := false

	fsms[0].Apply(&raft.Log{Index: 1})
	fsms[0].Apply(&raft.Log{Index: 2})

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		watcher.WaitIndex(0, 3, time.Microsecond)
		succeeded = true
	}()

	wg.Wait()

	assert.False(t, succeeded)
}

func TestFSMWatcher_WaitSnapshot(t *testing.T) {
	fsms := rafttest.FSMs(2)
	watcher := rafttest.FSMWatcher(t, fsms)

	go func() {
		_, err := fsms[0].Snapshot()
		require.NoError(t, err)
	}()

	go func() {
		for i := 0; i < 2; i++ {
			_, err := fsms[1].Snapshot()
			require.NoError(t, err)
		}
	}()

	watcher.WaitSnapshot(0, 1, time.Second)
	watcher.WaitSnapshot(1, 2, time.Second)

	assert.Equal(t, uint64(1), watcher.LastSnapshot(0))
	assert.Equal(t, uint64(2), watcher.LastSnapshot(1))
}

func TestFSMWatcher_WaitRestore(t *testing.T) {
	fsms := rafttest.FSMs(2)
	watcher := rafttest.FSMWatcher(t, fsms)

	go func() {
		require.NoError(t, fsms[0].Restore(nil))
	}()

	go func() {
		for i := 0; i < 2; i++ {
			require.NoError(t, fsms[1].Restore(nil))
		}
	}()

	watcher.WaitRestore(0, 1, time.Second)
	watcher.WaitRestore(1, 2, time.Second)

	assert.Equal(t, uint64(1), watcher.LastRestore(0))
	assert.Equal(t, uint64(2), watcher.LastRestore(1))
}

func TestFSM_HookIndex(t *testing.T) {
	fsms := rafttest.FSMs(2)

	ch := make(chan struct{})

	watcher := rafttest.FSMWatcher(t, fsms)
	watcher.HookIndex(0, 2, func() {
		ch <- struct{}{}
	})

	go func() {
		fsms[0].Apply(&raft.Log{Index: 1})
		fsms[0].Apply(&raft.Log{Index: 2})
	}()

	select {
	case <-ch:
	case <-time.After(500 * time.Millisecond):
		t.Error("hook was not invoked")
	}

}
