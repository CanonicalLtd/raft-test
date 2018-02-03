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
)

func TestWaitLeader(t *testing.T) {
	rafts, cleanup := rafttest.Cluster(t, rafttest.FSMs(3))
	defer cleanup()
	rafttest.WaitLeader(t, rafts[0], time.Second)
	assert.NotEqual(t, "", rafts[0].Leader())
}

func TestWaitLeader_Timeout(t *testing.T) {
	// A node with a single node won't be able to perform an election.
	rafts, cleanup := rafttest.Cluster(t, rafttest.FSMs(1))
	defer cleanup()

	succeeded := false

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		rafttest.WaitLeader(&testing.T{}, rafts[0], time.Microsecond)
		succeeded = true
	}()
	wg.Wait()

	assert.False(t, succeeded)
}

func TestFindLeader(t *testing.T) {
	rafts, cleanup := rafttest.Cluster(t, rafttest.FSMs(3))
	defer cleanup()
	i := rafttest.FindLeader(t, rafts, time.Second)
	assert.Equal(t, raft.Leader, rafts[i].State())
}

func TestFindLeader_Timeout(t *testing.T) {
	// A node with a single node won't be able to perform an election.
	rafts, cleanup := rafttest.Cluster(t, rafttest.FSMs(1))
	defer cleanup()

	succeeded := false

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		rafttest.FindLeader(&testing.T{}, rafts, time.Microsecond)
		succeeded = true
	}()
	wg.Wait()

	assert.False(t, succeeded)
}
