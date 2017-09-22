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

// Network provides control over a mesh of connected in-memory transports.
func Network() *NetworkKnob {
	return &NetworkKnob{}
}

// NetworkKnob can connect and disconnect nodes via loopback transports.
type NetworkKnob struct {
	t          *testing.T
	transports []raft.LoopbackTransport
}

// Disconnect the network transport of the raft node with the given index.
func (k *NetworkKnob) Disconnect(i int) {
	n := len(k.transports)
	if i < 0 || i >= n {
		k.t.Fatalf("invalid index %d (%d nodes available)", i, n)
	}
	this := k.transports[i]
	this.DisconnectAll()
	for _, other := range k.transports {
		other.Disconnect(this.LocalAddr())
	}
}

func (k *NetworkKnob) init(cluster *cluster) {
	k.t = cluster.t
	k.transports = make([]raft.LoopbackTransport, len(cluster.nodes))
	for i, node := range cluster.nodes {
		transport := node.Transport.(raft.LoopbackTransport)
		k.transports[i] = transport
	}
}

func (k *NetworkKnob) cleanup(cluster *cluster) {
}
