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

	// Create 3 dummy raft FSMs.
	fsms := rafttest.FSMs(3)

	// Create a cluster knob to tweak the raft configuration to perform
	// a snapshot after about 100 millisecond.
	config := rafttest.Config(func(n int, config *raft.Config) {
		config.SnapshotInterval = 100 * time.Millisecond
		config.SnapshotThreshold = 8
		config.TrailingLogs = 1
		config.ElectionTimeout = 300 * time.Millisecond
		config.HeartbeatTimeout = 250 * time.Millisecond
		config.LeaderLeaseTimeout = 250 * time.Millisecond
	})

	// Create a cluster of raft instances, setup with the above knob.
	rafts, control := rafttest.Cluster(t, fsms, config)
	defer control.Close()

	// Get the first raft instance to acquiring leadership.
	raft1 := control.LeadershipAcquired(time.Second)

	// Apply a log and wait for for all FSMs to apply it.
	err := raft1.Apply([]byte{}, time.Second).Error()
	if err != nil {
		log.Fatal(err)
	}
	if raft1.AppliedIndex() != 3 {
		log.Fatal("expected for applied index to be 3")
	}
	for _, raft := range rafts {
		control.WaitIndex(raft, raft1.AppliedIndex(), time.Second)
	}

	// Get one of the two follower raft instances.
	raft2 := control.Other(raft1)

	// Simulate a network disconnection of the follower.
	control.Disconnect(raft2)

	// Get the other follower raft instance.
	raft3 := control.Other(raft1, raft2)

	// Apply another 5 logs, leaving raft instance raft2 behind.
	for i := 0; i < 5; i++ {
		err := raft1.Apply([]byte{}, time.Second).Error()
		if err != nil {
			log.Fatal(err)
		}
	}

	// Wait for the FSMs of the two connected raft instances to apply the logs.
	control.WaitIndex(raft1, 8, time.Second)
	control.WaitIndex(raft3, 8, time.Second)

	// Make sure a snapshot is taken by the leader and the follower.
	control.WaitSnapshot(raft1, 1, time.Second)
	control.WaitSnapshot(raft3, 1, time.Second)

	// Reconnect the disconnected follower.
	control.Reconnect(raft2)

	// Wait for the reconnected follower to use the snapshot shipped by the
	// leader to catch up with logs.
	control.WaitRestore(raft2, 1, time.Second)

	// Apply other logs an check that the disconnected node has caught
	// up.
	for i := 0; i < 5; i++ {
		err := raft1.Apply([]byte{}, time.Second).Error()
		if err != nil {
			log.Fatal(err)
		}
	}

	control.WaitIndex(raft2, 13, time.Second)

	// Output:
	// true
	fmt.Println(raft2.AppliedIndex() == 13)
}
