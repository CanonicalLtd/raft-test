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
	"testing"
	"time"

	"github.com/CanonicalLtd/raft-test"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Get a node other than the leader.
func TestOther(t *testing.T) {
	_, control := rafttest.Cluster(t, rafttest.FSMs(3))
	defer control.Close()

	r1 := control.LeadershipAcquired(time.Second)
	r2 := control.Other(r1)

	assert.NotEqual(t, r1, r2)
	assert.NotEqual(t, raft.Leader, r2.State())
}

// Get a node other than the leader and a certain follower.
func TestOther_Multi(t *testing.T) {
	_, control := rafttest.Cluster(t, rafttest.FSMs(3))
	defer control.Close()

	r1 := control.LeadershipAcquired(time.Second)
	r2 := control.Other(r1)
	r3 := control.Other(r1, r2)

	assert.NotEqual(t, r1, r2)
	assert.NotEqual(t, r2, r3)
	assert.NotEqual(t, r3, r1)
	assert.NotEqual(t, raft.Leader, r3.State())
}

// If there's only a single node, Other returns nil
func TestOther_SingleNode(t *testing.T) {
	rafts, control := rafttest.Cluster(t, rafttest.FSMs(1))
	defer control.Close()

	assert.Nil(t, control.Other(rafts[0]))
}

func TestControl_LeadershipAcquired(t *testing.T) {
	rafts, control := rafttest.Cluster(t, rafttest.FSMs(3))
	defer control.Close()

	r := control.LeadershipAcquired(time.Second)

	found := false
	for _, raft := range rafts {
		if raft == r {
			found = true
			break
		}
	}
	assert.True(t, found, "returned raft instance is not part of the cluster")

	assert.Equal(t, raft.Leader, r.State())
}

func TestControl_LeadershipLost(t *testing.T) {
	_, control := rafttest.Cluster(t, rafttest.FSMs(3))
	defer control.Close()

	r := control.LeadershipAcquired(time.Second)
	control.Disconnect(r)

	control.LeadershipLost(r, time.Second)

	assert.NotEqual(t, raft.Leader, r.State())
}

func TestControl_Reconnect(t *testing.T) {
	_, control := rafttest.Cluster(t, rafttest.FSMs(3), rafttest.DiscardLogger())
	defer control.Close()

	r1 := control.LeadershipAcquired(time.Second)
	control.Disconnect(r1)
	control.LeadershipLost(r1, time.Second)

	r2 := control.LeadershipAcquired(time.Second)
	future := r2.Apply([]byte{}, time.Second)
	require.NoError(t, future.Error())

	control.Reconnect(r1)

	control.WaitIndex(r1, 3, time.Second)
}

func TestControl_WaitSnapshot(t *testing.T) {
	// Create a cluster knob to tweak the raft configuration to perform
	// a snapshot after about 100 millisecond.
	config := rafttest.Config(func(n int, config *raft.Config) {
		config.SnapshotInterval = 100 * time.Millisecond
		config.SnapshotThreshold = 4
		config.TrailingLogs = 1

		// Prevent the disconnected node from restarting election
		config.ElectionTimeout = 300 * time.Millisecond
		config.HeartbeatTimeout = 250 * time.Millisecond
		config.LeaderLeaseTimeout = 250 * time.Millisecond
	})

	_, control := rafttest.Cluster(t, rafttest.FSMs(3), config)
	defer control.Close()

	r1 := control.LeadershipAcquired(time.Second)
	r2 := control.Other(r1)
	r3 := control.Other(r1, r2)

	// Disconnect the first follower.
	control.Disconnect(r2)

	// Apply five logs, which should be enough to trigger a snapshot.
	for i := 0; i < 5; i++ {
		err := r1.Apply([]byte{}, time.Second).Error()
		require.NoError(t, err)
	}

	// Wait for r1 and r3 to perform a snapshot.
	control.WaitSnapshot(r1, 1, time.Second)
	control.WaitSnapshot(r3, 1, time.Second)

	// Reconnect r2.
	control.Reconnect(r2)

	// Wait for r2 to use a snapshot shipped by the leader to catch up with
	// logs.
	control.WaitRestore(r2, 1, time.Second)

	err := r1.Apply([]byte{}, time.Second).Error()
	require.NoError(t, err)

	control.WaitIndex(r2, 8, time.Second)
	assert.Equal(t, uint64(8), control.AppliedIndex(r2))
}
