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
func Network(options ...NetworkOption) *NetworkKnob {
	k := &NetworkKnob{}
	for _, option := range options {
		option(k)
	}
	return k
}

// NoAutoConnect disables the default behavior of auto-connecting the node
// transports. The first node (with address "0") will be configured with
// EnableSingleNode, so it can self elect itself.
func NoAutoConnect() NetworkOption {
	return func(k *NetworkKnob) {
		k.noAutoConnect = true
	}
}

// TransportFactory can be used to create custom transports.
//
// The given function takes a node index as argument and returns the Transport
// that the node should use.
//
// If the transports returned by the factory do not implement
// LoopbackTransport, the Disconnect API won't work.
func TransportFactory(factory func(int) raft.Transport) NetworkOption {
	return func(k *NetworkKnob) {
		k.transportFactory = factory
	}
}

// NetworkOption configures the behavior of a NetworkKnob.
type NetworkOption func(*NetworkKnob)

// NetworkKnob can connect and disconnect nodes via loopback transports.
type NetworkKnob struct {
	t                *testing.T
	noAutoConnect    bool
	transportFactory func(int) raft.Transport
	transports       []raft.LoopbackTransport
}

// Disconnect the network transport of the raft node with the given index.
func (k *NetworkKnob) Disconnect(i int) {
	n := len(k.transports)
	if i < 0 || i >= n {
		k.t.Fatalf("invalid index %d (%d nodes available)", i, n)
	}
	this := k.transports[i]
	if this == nil {
		k.t.Fatalf("node %d's transport is not a raft.LoobackTransport", i)
	}
	this.DisconnectAll()
	for _, other := range k.transports {
		other.Disconnect(this.LocalAddr())
	}
}

// Reconnect the network transport of the raft node with the given index.
func (k *NetworkKnob) Reconnect(i int) {
	n := len(k.transports)
	if i < 0 || i >= n {
		k.t.Fatalf("invalid index %d (%d nodes available)", i, n)
	}
	this := k.transports[i]
	if this == nil {
		k.t.Fatalf("node %d's transport is not a raft.LoobackTransport", i)
	}
	for _, other := range k.transports {
		this.Connect(other.LocalAddr(), other)
		other.Connect(this.LocalAddr(), this)
	}
}

func (k *NetworkKnob) init(cluster *cluster) {
	k.t = cluster.t
	k.transports = make([]raft.LoopbackTransport, len(cluster.nodes))
	for i, node := range cluster.nodes {
		if k.transportFactory != nil {
			node.Transport = k.transportFactory(i)
		}
		if loopback, ok := node.Transport.(raft.LoopbackTransport); ok {
			k.transports[i] = loopback
		}
	}

	if k.noAutoConnect {
		cluster.autoConnect = false
	}
}

func (k *NetworkKnob) cleanup(cluster *cluster) {
}
