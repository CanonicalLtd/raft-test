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
	"log"
	"testing"
	"time"

	"github.com/CanonicalLtd/raft-test"
	"github.com/hashicorp/raft"
)

func Example() {
	t := &testing.T{}

	// Create 3 raft FSMs and wrap them with a watcher.
	fsms := rafttest.FSMs(3)
	watcher := rafttest.FSMWatcher(t, fsms)

	// Create a cluster knob to get notified of leadership changes.
	notify := rafttest.Notify()

	// Create a cluster knob to control network connectivity and be able to
	// simulate transport disconnections.
	network := rafttest.Network()

	// Create a cluster knob to tweak the raft configuration to have a very
	// aggressive snapshot cadence.
	config := rafttest.Config(func(n int, config *raft.Config) {
		config.SnapshotInterval = 20 * time.Millisecond
		config.SnapshotThreshold = 4
		config.TrailingLogs = 2
	})

	// Create a cluster of raft instances, setup with the above knobs.
	rafts, cleanup := rafttest.Cluster(t, fsms, notify, network, config)
	defer cleanup()

	// Get the index of the first raft instance to acquiring leadership.
	i := notify.NextAcquired(time.Second)
	r := rafts[i]

	// Apply a log and wait for for all FSMs to apply it.
	err := r.Apply([]byte{}, time.Second).Error()
	if err != nil {
		log.Fatal(err)
	}
	for i := range fsms {
		watcher.WaitIndex(i, 3, time.Second)
	}

	// Get the index of one of the two follower raft instances.
	j := rafttest.Other(rafts, i)

	// Simulate a network disconnection of raft instance j.
	network.Disconnect(j)

	// Get the index of the other two follower raft instance.
	k := rafttest.Other(rafts, i, j)

	// Keep track of the last snapshot performed on instance i and j.
	snapshotI := watcher.LastSnapshot(i)
	snapshotK := watcher.LastSnapshot(k)

	// Apply another few logs, leaving raft instance j behind.
	for i := 0; i < 10; i++ {
		err := r.Apply([]byte{}, time.Second).Error()
		if err != nil {
			log.Fatal(err)
		}
	}

	// Wait for the FSMs of the two connected raft instances to apply the logs.
	watcher.WaitIndex(i, 13, time.Second)
	watcher.WaitIndex(k, 13, time.Second)

	// Make sure a further snapshot is taken by the leader and the follower.
	watcher.WaitSnapshot(i, snapshotI+1, time.Second)
	watcher.WaitSnapshot(k, snapshotK+1, time.Second)

	// Reconnect raft instance j.
	network.Reconnect(j)

	// Wait for raft instance j to use a snapshot shipped by the leader to
	// catch up with logs.
	watcher.WaitRestore(j, 1, 3*time.Second)

	// Apply another log an check that the disconnected node has caught
	// up. It might be that node i lost leadership, in that case we retry
	// with the next leader.
	for n := 0; n < 10; i++ {
		err = r.Apply([]byte{}, time.Second).Error()
		if err == nil {
			break
		}
		if err == raft.ErrNotLeader {
			i = notify.NextAcquired(time.Second)
			r = rafts[i]
			continue
		}
		break
	}
	if err != nil {
		log.Fatal(err)
	}

	watcher.WaitIndex(j, 14, time.Second)

	// Output:
	// true
	fmt.Println(rafts[j].AppliedIndex() >= 14)
}
