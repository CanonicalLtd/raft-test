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

	rafttest "github.com/CanonicalLtd/raft-test"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A scenario always expects a cluster with 3 raft instances.
func TestScenario_ClusterHasNotThreeNodes(t *testing.T) {
	testingT := &testing.T{}

	// Create a cluster with just two nodes.
	_, control := rafttest.Cluster(testingT, rafttest.FSMs(2))
	defer control.Close()

	ch := make(chan struct{})
	go func() {
		defer func() {
			ch <- struct{}{}
		}()
		scenario := rafttest.ReplicationScenario()
		scenario(control)
	}()
	<-ch

	assert.True(t, testingT.Failed())

}

// Each scenario expects a specific numner of stages.
func TestScenario_WrongNumberOfStages(t *testing.T) {
	testingT := &testing.T{}
	_, control := rafttest.Cluster(testingT, rafttest.FSMs(3))
	defer control.Close()

	ch := make(chan struct{})
	go func() {
		defer func() {
			ch <- struct{}{}
		}()

		scenario := rafttest.ReplicationScenario()

		// Pass only one stage.
		scenario(control, func(*raft.Raft) {})
	}()
	<-ch

	assert.True(t, testingT.Failed())

}

// The three stages are executed as expected.
func TestReplicationScenario(t *testing.T) {
	_, control := rafttest.Cluster(t, rafttest.FSMs(3))
	defer control.Close()

	executed := make([]int, 0)

	stage1 := func(r *raft.Raft) {
		assert.Equal(t, raft.Leader, r.State())
		err := r.Apply([]byte{}, rafttest.Duration(time.Second)).Error()
		require.NoError(t, err)
		executed = append(executed, 1)
	}
	stage2 := func(r *raft.Raft) {
		assert.Equal(t, raft.Follower, r.State())
		assert.Equal(t, uint64(3), r.AppliedIndex())
		executed = append(executed, 2)
	}
	stage3 := func(r *raft.Raft) {
		assert.Equal(t, raft.Follower, r.State())
		assert.Equal(t, uint64(3), r.AppliedIndex())
		executed = append(executed, 3)
	}

	scenario := rafttest.ReplicationScenario()
	scenario(control, stage1, stage2, stage3)

	assert.Equal(t, []int{1, 2, 3}, executed)
}

// The five stages are executed as expected.
func TestNotLeaderScenario(t *testing.T) {
	_, control := rafttest.Cluster(t, rafttest.FSMs(3))
	defer control.Close()

	executed := make([]int, 0)

	stage1 := func(r *raft.Raft) {
		assert.Equal(t, raft.Leader, r.State())
		err := r.Apply([]byte{}, rafttest.Duration(time.Second)).Error()
		require.NoError(t, err)
		executed = append(executed, 1)
	}
	stage2 := func(r *raft.Raft) {
		err := r.Apply([]byte{}, rafttest.Duration(time.Second)).Error()
		assert.Equal(t, raft.ErrNotLeader, err)
		executed = append(executed, 2)
	}
	stage3 := func(r *raft.Raft) {
		assert.Equal(t, raft.Leader, r.State())
		err := r.Apply([]byte{}, rafttest.Duration(time.Second)).Error()
		require.NoError(t, err)
		executed = append(executed, 3)
	}
	stage4 := func(r *raft.Raft) {
		assert.Equal(t, raft.Follower, r.State())
		assert.Equal(t, uint64(5), r.AppliedIndex())
		executed = append(executed, 4)
	}
	stage5 := func(r *raft.Raft) {
		assert.Equal(t, raft.Follower, r.State())
		assert.Equal(t, uint64(5), r.AppliedIndex())
		executed = append(executed, 5)
	}

	scenario := rafttest.NotLeaderScenario(0)
	scenario(control, stage1, stage2, stage3, stage4, stage5)

	assert.Equal(t, []int{1, 2, 3, 4, 5}, executed)
}

// The five stages are executed as expected, and the leader is disconnected
// only at the given index.
func TestNotLeaderScenario_WithIndex(t *testing.T) {
	_, control := rafttest.Cluster(t, rafttest.FSMs(3))
	defer control.Close()

	executed := make([]int, 0)

	stage1 := func(r *raft.Raft) {
		assert.Equal(t, raft.Leader, r.State())
		err := r.Apply([]byte{}, rafttest.Duration(time.Second)).Error()
		require.NoError(t, err)
		err = r.Apply([]byte{}, rafttest.Duration(time.Second)).Error()
		require.NoError(t, err)
		executed = append(executed, 1)
	}
	stage2 := func(r *raft.Raft) {
		err := r.Apply([]byte{}, rafttest.Duration(time.Second)).Error()
		assert.Equal(t, raft.ErrNotLeader, err)
		executed = append(executed, 2)
	}
	stage3 := func(r *raft.Raft) {
		assert.Equal(t, raft.Leader, r.State())
		err := r.Apply([]byte{}, rafttest.Duration(time.Second)).Error()
		require.NoError(t, err)
		assert.Equal(t, uint64(6), r.AppliedIndex())
		executed = append(executed, 3)
	}
	stage4 := func(r *raft.Raft) {
		assert.Equal(t, raft.Follower, r.State())
		assert.Equal(t, uint64(6), r.AppliedIndex())
		executed = append(executed, 4)
	}
	stage5 := func(r *raft.Raft) {
		assert.Equal(t, raft.Follower, r.State())
		assert.Equal(t, uint64(6), r.AppliedIndex())
		executed = append(executed, 5)
	}

	scenario := rafttest.NotLeaderScenario(4)
	scenario(control, stage1, stage2, stage3, stage4, stage5)

	assert.Equal(t, []int{1, 2, 3, 4, 5}, executed)
}

// The five stages are executed as expected.
func TestLeadershipLostScenario(t *testing.T) {
	_, control := rafttest.Cluster(t, rafttest.FSMs(3))
	defer control.Close()

	executed := make([]int, 0)

	stage1 := func(r *raft.Raft) {
		assert.Equal(t, raft.Leader, r.State())
		err := r.Apply([]byte{}, rafttest.Duration(time.Second)).Error()
		require.NoError(t, err)
		executed = append(executed, 1)
	}
	stage2 := func(r *raft.Raft) {
		err := r.Apply([]byte{}, rafttest.Duration(time.Second)).Error()
		assert.Equal(t, raft.ErrLeadershipLost, err)
		executed = append(executed, 2)
	}
	stage3 := func(r *raft.Raft) {
		assert.Equal(t, raft.Leader, r.State())
		err := r.Apply([]byte{}, rafttest.Duration(time.Second)).Error()
		require.NoError(t, err)
		executed = append(executed, 3)
	}
	stage4 := func(r *raft.Raft) {
		assert.Equal(t, raft.Follower, r.State())
		assert.Equal(t, uint64(5), r.AppliedIndex())
		executed = append(executed, 4)
	}
	stage5 := func(r *raft.Raft) {
		assert.Equal(t, raft.Follower, r.State())
		assert.Equal(t, uint64(5), r.AppliedIndex())
		executed = append(executed, 5)
	}

	scenario := rafttest.LeadershipLostScenario()
	scenario(control, stage1, stage2, stage3, stage4, stage5)

	assert.Equal(t, []int{1, 2, 3, 4, 5}, executed)
}

// The five stages are executed as expected.
func TestLeadershipLostQuorumSameLeaderScenario(t *testing.T) {
	_, control := rafttest.Cluster(t, rafttest.FSMs(3))
	defer control.Close()

	executed := make([]int, 0)

	stage1 := func(r *raft.Raft) {
		assert.Equal(t, raft.Leader, r.State())
		err := r.Apply([]byte{}, rafttest.Duration(time.Second)).Error()
		require.NoError(t, err)
		executed = append(executed, 1)
	}
	stage2 := func(r *raft.Raft) {
		err := r.Apply([]byte{}, rafttest.Duration(time.Second)).Error()
		assert.Equal(t, raft.ErrLeadershipLost, err)
		executed = append(executed, 2)
	}
	stage3 := func(r *raft.Raft) {
		assert.Equal(t, raft.Leader, r.State())
		err := r.Apply([]byte{}, rafttest.Duration(time.Second)).Error()
		require.NoError(t, err)
		executed = append(executed, 3)
	}
	stage4 := func(r *raft.Raft) {
		assert.Equal(t, raft.Follower, r.State())
		assert.Equal(t, uint64(6), r.AppliedIndex())
		executed = append(executed, 4)
	}
	stage5 := func(r *raft.Raft) {
		assert.Equal(t, raft.Follower, r.State())
		assert.Equal(t, uint64(6), r.AppliedIndex())
		executed = append(executed, 5)
	}

	scenario := rafttest.LeadershipLostQuorumSameLeaderScenario(4)
	scenario(control, stage1, stage2, stage3, stage4, stage5)

	assert.Equal(t, []int{1, 2, 3, 4, 5}, executed)
}

func TestLeadershipLostQuorumOtherLeaderScenario(t *testing.T) {
	_, control := rafttest.Cluster(t, rafttest.FSMs(3))
	defer control.Close()

	executed := make([]int, 0)

	stage1 := func(r *raft.Raft) {
		assert.Equal(t, raft.Leader, r.State())
		err := r.Apply([]byte{}, rafttest.Duration(time.Second)).Error()
		require.NoError(t, err)
		executed = append(executed, 1)
	}
	stage2 := func(r *raft.Raft) {
		err := r.Apply([]byte{}, rafttest.Duration(time.Second)).Error()
		assert.Equal(t, raft.ErrLeadershipLost, err)
		executed = append(executed, 2)
	}
	stage3 := func(r *raft.Raft) {
		assert.Equal(t, raft.Leader, r.State())
		err := r.Apply([]byte{}, rafttest.Duration(time.Second)).Error()
		require.NoError(t, err)
		executed = append(executed, 3)
	}
	stage4 := func(r *raft.Raft) {
		assert.Equal(t, raft.Follower, r.State())
		assert.Equal(t, uint64(6), r.AppliedIndex())
		executed = append(executed, 4)
	}
	stage5 := func(r *raft.Raft) {
		assert.Equal(t, raft.Follower, r.State())
		assert.Equal(t, uint64(6), r.AppliedIndex())
		executed = append(executed, 5)
	}

	scenario := rafttest.LeadershipLostQuorumOtherLeaderScenario(4)
	scenario(control, stage1, stage2, stage3, stage4, stage5)

	assert.Equal(t, []int{1, 2, 3, 4, 5}, executed)
}
