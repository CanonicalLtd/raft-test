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

// The before store-logs hook is invoked before a log is stored.
func TestLogsWatcher_BeforeStoreLog(t *testing.T) {
	store := raft.NewInmemStore()
	wrapper := newLogWrapper(store)
	watcher := newLogsWatcher([]*logWrapper{wrapper})

	triggered := false
	watcher.BeforeStoreLog(0, 1, func() {
		index, err := store.FirstIndex()
		require.NoError(t, err)
		assert.Equal(t, uint64(0), index)
		triggered = true
	})

	wrapper.StoreLog(&raft.Log{Index: 1})

	assert.True(t, triggered)

	index, err := store.FirstIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), index)
}
