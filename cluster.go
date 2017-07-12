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
	"bytes"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/hashicorp/raft"
)

// Cluster keeps tracks of one or more Nodes belonging to a test
// cluster.
type Cluster struct {
	// Apply this timeout to any Cluster method that could
	// possibly block indefinitely, and panic if the time expires
	// during the method execution. It's 5 seconds by default.
	Timeout time.Duration

	// Maximum number of expect of leadership changes expected
	// during the lifetime of the cluster. Since these changes are
	// buffered in a channel, their upper bound must be known
	// beforehands. The default is 1024.
	MaxLeadershipChanges int

	// Whether to automatically connect the nodes of the cluster
	// to each other. If false, user code must manually create the
	// connections. The default is true.
	AutoConnectNodes bool

	// Output stream for logging
	LogOutput *bytes.Buffer

	nodes               []*Node    // All nodes in the cluster
	leadershipChangedCh chan *Node // Cluster-wide leadership changes
}

// NewCluster creates a new cluster comprised of n Nodes created with
// default raft dependencies.
func NewCluster(n int) *Cluster {
	nodes := make([]*Node, n)
	output := bytes.NewBuffer(nil)

	for i := range nodes {
		node := NewNode()

		// Use progressive numbers as network addressess.
		addr := fmt.Sprintf("%d", i)
		_, node.Transport = raft.NewInmemTransport(addr)

		// Logging
		prefix := fmt.Sprintf("%d: ", i)
		node.Config.Logger = log.New(output, prefix, log.Lmicroseconds)

		nodes[i] = node
	}

	return &Cluster{
		Timeout:              5 * time.Second,
		MaxLeadershipChanges: 1024,
		AutoConnectNodes:     true,
		LogOutput:            output,
		nodes:                nodes,
	}
}

// Start all the raft nodes in the cluster. By default, all
// LoopbackTransport instances will automatically be connected to each
// other and the PeerStore instances populated with the addresses of
// the other nodes. This behavior can be turned on off by setting the
// AutoConnectNodes attribute to false.
func (c *Cluster) Start() {
	if c.AutoConnectNodes {
		connectLoobackTransports(c.nodes)
		populateNodeStores(c.nodes)
	}

	c.leadershipChangedCh = make(chan *Node, c.MaxLeadershipChanges)
	c.consumeNotifyCh()
	for _, node := range c.nodes {
		node.Start()
	}
}

// Shutdown all the raft nodes in the cluster.
func (c *Cluster) Shutdown() {
	for _, node := range c.nodes {
		node.Shutdown()
		close(node.Config.NotifyCh)
	}
}

// Node returns the i-th node of the cluster.
func (c *Cluster) Node(i int) *Node {
	return c.nodes[i]
}

// LeadershipChanged returns the next node whose leadership state in
// the cluster has changed, possibly blocking until it happens.
func (c *Cluster) LeadershipChanged() *Node {
	select {
	case node := <-c.leadershipChangedCh:
		return node
	case <-time.After(c.Timeout):
		panic("timeout waiting for leadership change")
	}
}

// LeadershipAcquired returns the next node that has acquired
// leadership, possibly blocking until it happens.
func (c *Cluster) LeadershipAcquired() *Node {
	for {
		node := c.LeadershipChanged()
		if node.Raft().State() == raft.Leader {
			return node
		}
	}
}

// LeadershipLost returns the next node that has lost leadership,
// possibly blocking until it happens.
func (c *Cluster) LeadershipLost() *Node {
	for {
		node := c.LeadershipChanged()
		if node.Raft().State() != raft.Leader {
			return node
		}
	}
}

// Peers returns all nodes that are not the given one. This is useful
// in combination with LeadershipAcquired to get all nodes that
// are currently not in leader state.
func (c *Cluster) Peers(node *Node) []*Node {
	others := []*Node{}
	for _, other := range c.nodes {
		if other != node {
			others = append(others, other)
		}
	}
	return others
}

// Disconnect the network transport of the given nodes from the ther
// nodes.
func (c *Cluster) Disconnect(node *Node) {
	thisTransport := loopbackTransport(node.Transport)
	thisTransport.DisconnectAll()
	for _, other := range c.nodes {
		otherTransport := loopbackTransport(other.Transport)
		otherTransport.Disconnect(node.Transport.LocalAddr())
	}
}

// Listen for leadership change notification across all nodes, using
// their configured NotifyCh and fill our leadershipChangedCh with the
// node that changed its leadership state. The internal loop will run
// forever until all NotifyCh of our peers get closed.
func (c *Cluster) consumeNotifyCh() {
	cases := make([]reflect.SelectCase, len(c.nodes))

	for i, node := range c.nodes {
		if node.Config.NotifyCh != nil {
			panic("non-nil NotifyCh on node: cluster needs to set its own")
		}
		notifyCh := make(chan bool, 0)
		node.Config.NotifyCh = notifyCh
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(notifyCh),
		}
	}

	// Loop until all nodes have shutdown and closed their
	// notifyCh.
	go func() {
		for len(cases) > 0 {
			i, _, ok := reflect.Select(cases)
			if !ok {
				// Remove from the select cases the notify
				// channels that have been closed, since that
				// means the node was shutdown.
				cases = append(cases[:i], cases[i+1:]...)
			}
			c.leadershipChangedCh <- c.nodes[i]
		}
	}()
}

// Convert a regular Transport to a LoopbackTransport, or panic if the
// given transport does not implement LoopbackTransport.
func loopbackTransport(transport raft.Transport) raft.LoopbackTransport {
	loopbackTransport, ok := transport.(raft.LoopbackTransport)
	if !ok {
		panic("transport does not implement loopback interface")
	}
	return loopbackTransport
}

// Connect all the given transports to each other.
func connectLoobackTransports(nodes []*Node) {
	for i, node1 := range nodes {
		for j, node2 := range nodes {
			if i != j {
				transport1 := loopbackTransport(node1.Transport)
				transport2 := loopbackTransport(node2.Transport)
				transport1.Connect(transport2.LocalAddr(), transport2)
				transport2.Connect(transport1.LocalAddr(), transport1)
			}
		}
	}
}

// Populate the node stores with the addresses of the other nodes.
func populateNodeStores(nodes []*Node) {
	for i := range nodes {
		for j := range nodes {
			if i == j {
				continue
			}
			store := nodes[i].Peers
			transport := nodes[j].Transport

			addrs, err := store.Peers()
			if err != nil {
				panic(fmt.Sprintf(
					"failed to get peers for node %d: %v", i, err))
			}

			store.SetPeers(append(addrs, transport.LocalAddr()))

		}
	}
}
