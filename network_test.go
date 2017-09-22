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

func TestNetwork_Disconnect(t *testing.T) {
	notify := rafttest.Notify()
	network := rafttest.Network()
	rafts, cleanup := rafttest.Cluster(t, rafttest.FSMs(3), notify, network)
	defer cleanup()

	i := notify.NextAcquired(time.Second)
	network.Disconnect(i)

	j := notify.NextLost(time.Second)

	assert.True(t, i == j)
	assert.False(t, rafts[i].State() == raft.Leader)
}

func TestNetwork_DisconnectInvalidIndex(t *testing.T) {
	network := rafttest.Network()
	_, cleanup := rafttest.Cluster(&testing.T{}, rafttest.FSMs(3), network)
	defer cleanup()

	succeeded := false

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		network.Disconnect(1000)
		succeeded = true
	}()
	wg.Wait()

	assert.False(t, succeeded)
}
