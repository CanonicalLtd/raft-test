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

// The test fails if trying to get the index of a raft instance which is not in
// the cluster.
func TestIndexl_Invalid(t *testing.T) {
	testingT := &testing.T{}
	_, control := rafttest.Cluster(testingT, rafttest.FSMs(1))
	defer control.Close()

	ch := make(chan struct{})
	go func() {
		defer func() {
			ch <- struct{}{}
		}()
		control.Index(&raft.Raft{})
	}()
	<-ch

	assert.True(t, testingT.Failed())
}

// Get a node other than the leader.
func TestControl_Other(t *testing.T) {
	_, control := rafttest.Cluster(t, rafttest.FSMs(3))
	defer control.Close()

	r1 := control.LeadershipAcquired(time.Second)
	r2 := control.Other(r1)

	assert.NotEqual(t, r1, r2)
	assert.NotEqual(t, raft.Leader, r2.State())
}

// Get a node other than the leader and a certain follower.
func TestControl_Other_Multi(t *testing.T) {
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
func TestControl_Other_SingleNode(t *testing.T) {
	rafts, control := rafttest.Cluster(t, rafttest.FSMs(1))
	defer control.Close()

	assert.Nil(t, control.Other(rafts[0]))
}

// Wait for a non-leader node to acquire leadership.
func TestControl_LeadershipAcquired(t *testing.T) {
	_, control := rafttest.Cluster(t, rafttest.FSMs(3))
	defer control.Close()

	r := control.LeadershipAcquired(time.Second)
	control.Index(r) // Will fail if r is not part of the cluster.

	assert.Equal(t, raft.Leader, r.State())
}

// The test fails if leadership is not acquired within the given timeout.
func TestControl_LeadershipAcquired_Timeout(t *testing.T) {
	testingT := &testing.T{}
	_, control := rafttest.Cluster(testingT, rafttest.FSMs(3))
	defer control.Close()

	ch := make(chan struct{})
	go func() {
		defer func() {
			ch <- struct{}{}
		}()
		control.LeadershipAcquired(time.Nanosecond)
	}()
	<-ch

	assert.True(t, testingT.Failed())
}

// The test fails if a node loses leadership instead of acquiring it.
func TestControl_LeadershipAcquired_UnexpectedLeadershipLost(t *testing.T) {
	testingT := &testing.T{}
	_, control := rafttest.Cluster(testingT, rafttest.FSMs(3))
	defer control.Close()

	r := control.LeadershipAcquired(time.Second)
	control.Disconnect(r)

	ch := make(chan struct{})
	go func() {
		defer func() {
			ch <- struct{}{}
		}()
		control.LeadershipAcquired(time.Second)
	}()
	<-ch

	assert.True(t, testingT.Failed())
}

// Wait for a leader node to lose leadership.
func TestControl_LeadershipLost(t *testing.T) {
	_, control := rafttest.Cluster(t, rafttest.FSMs(3))
	defer control.Close()

	r := control.LeadershipAcquired(time.Second)
	control.Disconnect(r)

	control.LeadershipLost(r, time.Second)

	assert.NotEqual(t, raft.Leader, r.State())
}

// The test fails if leadership is not lost within the given timeout.
func TestControl_LeadershipLost_Timeout(t *testing.T) {
	testingT := &testing.T{}
	_, control := rafttest.Cluster(testingT, rafttest.FSMs(3))
	defer control.Close()

	r := control.LeadershipAcquired(time.Second)
	control.Disconnect(r)

	ch := make(chan struct{})
	go func() {
		defer func() {
			ch <- struct{}{}
		}()
		control.LeadershipLost(r, time.Nanosecond)
	}()
	<-ch

	assert.True(t, testingT.Failed())
}

// The test fails if a node acquires leadership instead losing it.
func TestControl_LeadershipLost_UnexpectedLeadershipAcquired(t *testing.T) {
	testingT := &testing.T{}
	rafts, control := rafttest.Cluster(testingT, rafttest.FSMs(3))
	defer control.Close()

	ch := make(chan struct{})
	go func() {
		defer func() {
			ch <- struct{}{}
		}()
		control.LeadershipLost(rafts[0], time.Second)
	}()
	<-ch

	assert.True(t, testingT.Failed())
}

// When disconnecting a leader just after an apply future was created, the
// future fails with ErrLeadershipLost, and the log is not committed to the
// followers.
func TestControl_LeadershipLostWhileCommittingLogNoCommit(t *testing.T) {
	_, control := rafttest.Cluster(t, rafttest.FSMs(3))
	defer control.Close()

	r1 := control.LeadershipAcquired(time.Second)
	control.Disconnect(r1)

	future := r1.Apply([]byte{}, time.Second)

	control.LeadershipLost(r1, time.Second)

	require.Equal(t, raft.ErrLeadershipLost, future.Error())
	assert.Equal(t, uint64(3), r1.LastIndex())

	// The new leader did not apply the entry that the old leader tried to
	// append while disconnected.
	r2 := control.LeadershipAcquired(time.Second)
	require.NoError(t, r2.Barrier(time.Second).Error())
	assert.Equal(t, uint64(0), control.AppliedIndex(r2))
}

// If log stores on followers are slow enough in applying a log that the
// heartbeat timeout is hit, a log future fails with ErrLeadershipLost, but
// the log will be committed to the followers.
func TestControl_LeadershipLostWhileCommittingLogsSuccessfulAppend(t *testing.T) {
	_, control := rafttest.Cluster(t, rafttest.FSMs(3))
	defer control.Close()

	r1 := control.LeadershipAcquired(time.Second)

	slowdown := func() {
		// Sleep for longer than heartbeat timeout
		time.Sleep(rafttest.Duration(40 * time.Millisecond))
	}

	r2 := control.Other(r1)
	r3 := control.Other(r1, r2)
	control.BeforeStoreLog(r2, 3, slowdown)
	control.BeforeStoreLog(r3, 3, slowdown)

	future := r1.Apply([]byte("hi"), time.Second)

	control.LeadershipLost(r1, time.Second)
	control.Disconnect(r1)
	require.Equal(t, raft.ErrLeadershipLost, future.Error())

	// The new leader applied the entry that the old leader appended
	// despite losing leadership inbetween.
	r2 = control.LeadershipAcquired(time.Second)
	assert.NoError(t, r2.Barrier(time.Second).Error())
	control.WaitIndex(r2, 3, time.Second)
}

// If log stores on followers are slow enough in applying a log that the
// heartbeat timeout is hit, a log future fails with ErrLeadershipLost, but
// the log will be committed to the followers.
func TestControl_LeadershipLostWhileCommittingLogsSuccessfulAppendSameLeader(t *testing.T) {
	_, control := rafttest.Cluster(t, rafttest.FSMs(3))
	defer control.Close()

	r1 := control.LeadershipAcquired(time.Second)

	slowdown := func() {
		// Sleep for longer than heartbeat timeout
		time.Sleep(rafttest.Duration(40 * time.Millisecond))
	}

	r2 := control.Other(r1)
	r3 := control.Other(r1, r2)
	control.BeforeStoreLog(r2, 3, slowdown)
	control.BeforeStoreLog(r3, 3, slowdown)

	future := r1.Apply([]byte("hi"), time.Second)

	control.LeadershipLost(r1, time.Second)
	require.Equal(t, raft.ErrLeadershipLost, future.Error())

	// The new leader applied the entry that the old leader appended
	// despite losing leadership inbetween.
	r2 = control.LeadershipAcquired(time.Second)
	assert.NoError(t, r2.Barrier(time.Second).Error())
	control.WaitIndex(r2, 3, time.Second)
}

// Reconnecting a deposed leader makes it catch up with logs.
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

// Wait for a snapshot to be created and then restored.
func TestControl_WaitSnapshot(t *testing.T) {
	// Create a cluster knob to tweak the raft configuration to perform
	// a snapshot after about 100 millisecond.
	config := rafttest.Config(func(n int, config *raft.Config) {
		config.SnapshotInterval = 100 * time.Millisecond
		config.SnapshotThreshold = 5
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

	// Disconnect the first follower.
	control.Disconnect(r2)

	// Apply five logs, which is enough to trigger a snapshot, since the
	// threshold is 5.
	for i := 0; i < 5; i++ {
		err := r1.Apply([]byte{}, time.Second).Error()
		require.NoError(t, err)
	}

	// Wait for r1 (the leader) to perform a snapshot.
	control.WaitSnapshot(r1, 1, time.Second)

	// Reconnect r2.
	control.Reconnect(r2)

	// Wait for r2 to use a snapshot shipped by the leader to catch up with
	// logs.
	control.WaitRestore(r2, 1, time.Second)

	// Apply another log and wait for the follower to catch up with it as
	// well.
	err := r1.Apply([]byte{}, time.Second).Error()
	require.NoError(t, err)

	control.WaitIndex(r2, 8, time.Second)
	assert.Equal(t, uint64(8), control.AppliedIndex(r2))
}
