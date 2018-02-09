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
	"fmt"

	"github.com/hashicorp/raft"
)

// Provides control over a mesh of connected in-memory transports.
//
// Each node must be configured with an in-memory transport. All transports
// will be connected to each other at cluster creation time.
type network struct {
	transports []raft.LoopbackTransport
}

func newNetwork(transports []raft.LoopbackTransport) *network {
	network := &network{
		transports: transports,
	}
	return network
}

// Disconnect the network transport of the raft node with the given index.
func (n *network) Disconnect(i int) error {
	this := n.transports[i]
	if this == nil {
		return fmt.Errorf("node %d's transport is not a raft.LoobackTransport", i)
	}
	this.DisconnectAll()
	for j, other := range n.transports {
		if j == i {
			continue
		}
		if other == nil {
			return fmt.Errorf("node %d's transport is not a raft.LoobackTransport", j)
		}
		other.Disconnect(this.LocalAddr())
	}
	return nil
}

// Reconnect the network transport of the raft node with the given index.
func (n *network) Reconnect(i int) error {
	this := n.transports[i]
	if this == nil {
		return fmt.Errorf("node %d's transport is not a raft.LoobackTransport", i)
	}
	for j, other := range n.transports {
		if j == i {
			continue
		}
		if other == nil {
			return fmt.Errorf("node %d's transport is not a raft.LoobackTransport", j)
		}
		this.Connect(other.LocalAddr(), other)
		other.Connect(this.LocalAddr(), this)
	}
	return nil
}
