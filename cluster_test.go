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
)

// Create and shutdown a cluster.
func TestCluster_CreateAndShutdown(t *testing.T) {
	rafts, cleanup := rafttest.Cluster(t, rafttest.FSMs(1))
	defer cleanup()

	assert.Len(t, rafts, 1)
}

// Get a node other than the leader.
func TestOther(t *testing.T) {
	notify := rafttest.Notify()
	rafts, cleanup := rafttest.Cluster(t, rafttest.FSMs(3), notify)
	defer cleanup()

	i := notify.NextAcquired(time.Second)
	j := rafttest.Other(rafts, i)

	assert.NotEqual(t, i, j)
	assert.NotEqual(t, raft.Leader, rafts[j].State())
}

// Get a node other than the leader and a certainf follower.
func TestOther_Multi(t *testing.T) {
	notify := rafttest.Notify()
	rafts, cleanup := rafttest.Cluster(t, rafttest.FSMs(3), notify)
	defer cleanup()

	i := notify.NextAcquired(time.Second)
	j := rafttest.Other(rafts, i)
	k := rafttest.Other(rafts, i, j)

	assert.NotEqual(t, i, j)
	assert.NotEqual(t, j, k)
	assert.NotEqual(t, i, k)
	assert.NotEqual(t, raft.Leader, rafts[j].State())
}

// If there's only a single node, Other returns -1.
func TestOther_SingleNode(t *testing.T) {
	rafts, cleanup := rafttest.Cluster(t, rafttest.FSMs(1))
	defer cleanup()

	assert.Equal(t, -1, rafttest.Other(rafts, 0))
}
