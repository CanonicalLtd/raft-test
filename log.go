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
	"sync"

	"github.com/hashicorp/raft"
)

// LogStore can be used to create custom log stores.
//
// The given function takes a node index as argument and returns the LogStore
// that the node should use.
func LogStore(factory func(int) raft.LogStore) Knob {
	return func(nodes []*node) {
		for i, node := range nodes {
			node.Logs = factory(i)
		}
	}
}

// logsWatcher implements methods to observe for the underlying set of log
// stores.
type logsWatcher struct {
	wrappers []*logWrapper
}

// Creates a new logsWatcher for watching the given FSMs.
func newLogsWatcher(wrappers []*logWrapper) *logsWatcher {
	return &logsWatcher{
		wrappers: wrappers,
	}
}

// BeforeStoreLog registers a hook to be executed before the log store with the
// given index stores the log with the given index.
func (w *logsWatcher) BeforeStoreLog(i int, index uint64, hook func()) {
	wrapper := w.wrappers[i]

	wrapper.mu.Lock()
	defer wrapper.mu.Unlock()

	wrapper.beforeStoreLog[index] = hook
}

// Wrap a log store and catch events on it.
type logWrapper struct {
	store          raft.LogStore     // The store being wrapped
	beforeStoreLog map[uint64]func() // Invoke f before storing the log with the given index.

	// Serialize access to internal state.
	mu sync.RWMutex
}

func newLogWrapper(store raft.LogStore) *logWrapper {
	return &logWrapper{
		store:          store,
		beforeStoreLog: map[uint64]func(){},
	}
}

func (l *logWrapper) GetLog(idx uint64, log *raft.Log) error {
	return l.store.GetLog(idx, log)
}

func (l *logWrapper) StoreLog(log *raft.Log) error {
	return l.StoreLogs([]*raft.Log{log})
}

func (l *logWrapper) StoreLogs(logs []*raft.Log) error {
	// Insert the logs into the ring buffer
	hooks := make([]func(), 0)
	l.mu.RLock()
	for _, log := range logs {
		hook := l.beforeStoreLog[log.Index]
		if hook != nil {
			hooks = append(hooks, hook)
		}
	}
	l.mu.RUnlock()

	for _, hook := range hooks {
		hook()
	}

	return l.store.StoreLogs(logs)
}

func (l *logWrapper) FirstIndex() (uint64, error) {
	return l.store.FirstIndex()
}

func (l *logWrapper) LastIndex() (uint64, error) {
	return l.store.LastIndex()
}

func (l *logWrapper) DeleteRange(min, max uint64) error {
	return l.store.DeleteRange(min, max)
}
