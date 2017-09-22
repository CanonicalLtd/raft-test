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
// Dependencies can be replaced or mutated using the various NodeOption knobs.
func Node(t *testing.T, fsm raft.FSM, knobs ...Knob) *raft.Raft {
	fsms := []raft.FSM{fsm}
	knobs = append(knobs, &singleModeKnob{})

	rafts, _ := Cluster(t, fsms, knobs...)

	return rafts[0]
}

type singleModeKnob struct{}

func (k *singleModeKnob) init(cluster *cluster) {
	if len(cluster.nodes) != 1 {
		panic("expected to have a cluster with exactly one node")
	}
	cluster.nodes[0].Config.EnableSingleNode = true
	cluster.nodes[0].Config.StartAsLeader = true
}

func (k *singleModeKnob) cleanup(cluster *cluster) {
}
