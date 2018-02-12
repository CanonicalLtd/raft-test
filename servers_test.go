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

// If the Servers knob is used, only the given nodes are connected and
// bootstrapped.
func TestServers(t *testing.T) {
	fsms := rafttest.FSMs(3)
	servers := rafttest.Servers(0)
	rafts, control := rafttest.Cluster(t, fsms, servers)
	defer control.Close()

	assert.Equal(t, rafts[0], control.LeadershipAcquired(time.Second))

	assert.Equal(t, raft.Leader, rafts[0].State())
	future := rafts[0].GetConfiguration()
	require.NoError(t, future.Error())
	assert.Len(t, future.Configuration().Servers, 1)
}
