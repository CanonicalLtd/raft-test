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
	"fmt"
	"testing"
	"time"

	"github.com/CanonicalLtd/raft-test"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func Example() {
	t := &testing.T{}

	// Create 3 raft FSMs and wrap them with a watcher.
	fsms := rafttest.FSMs(3)
	watcher := rafttest.FSMWatcher(t, fsms)

	// Create test cluster knobs.
	notify := rafttest.Notify()
	network := rafttest.Network()
	config := rafttest.Config(func(n int, config *raft.Config) {
		config.SnapshotInterval = 5 * time.Millisecond
		config.SnapshotThreshold = 4
		config.TrailingLogs = 10
	})

	// Create a cluster of raft instances.
	rafts, cleanup := rafttest.Cluster(t, fsms, notify, network, config)
	defer cleanup()

	// Get the index of the first raft instance to acquiring leadership.
	i := notify.NextAcquired(time.Second)
	r := rafts[i]

	// Get the index of one of the two follower raft instances.
	j := rafttest.Other(rafts, i)

	// Apply a log and wait for for all FSMs to apply it.
	require.NoError(t, r.Apply([]byte{}, time.Second).Error())
	for i := range fsms {
		watcher.WaitIndex(i, 3, time.Second)
	}

	// Simulate a network disconnection of raft instance j.
	network.Disconnect(j)

	// Apply another few logs, leaving raft instance j behind.
	for i := 0; i < 100; i++ {
		require.NoError(t, r.Apply([]byte{}, time.Second).Error())
	}
	watcher.WaitIndex(i, 103, time.Second)

	// Make sure a snapshot is taken by the leader i.
	watcher.WaitSnapshot(i, 1, time.Second)

	// Reconnect raft instance j.
	network.Reconnect(j)

	// Wait for raft instance j to use a snapshot shipped by the leader to
	// catch up with logs.
	watcher.WaitRestore(j, 1, time.Second)

	// Output:
	// 103
	fmt.Println(rafts[j].AppliedIndex())
}
