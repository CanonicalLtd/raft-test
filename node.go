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

package rafttest

import (
	"testing"

	"github.com/hashicorp/raft"
)

// Node is a convenience for creating a Cluster with a single raft.Raft node
// with single mode enabled and that immediately starts as leader.
//
// The default network address of a test node is "0".
//
// Dependencies can be replaced or mutated using the various knobs.
func Node(t *testing.T, fsm raft.FSM, knobs ...Knob) (*raft.Raft, func()) {
	fsms := []raft.FSM{fsm}

	config := Config(func(i int, config *raft.Config) {
		if i != 0 {
			t.Fatal("expected to have a cluster with exactly one node")
		}
		config.StartAsLeader = true
	})

	rafts, control := Cluster(t, fsms, config)

	return rafts[0], control.Close
}
