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
	"io"
	"io/ioutil"
	"log"
	"time"

	"github.com/hashicorp/raft"
)

// Node captures a raft.Raft instance along with its dependencies for
// a single node. Dependencies can be replaced or mutated before the
// Start method is invoked and the node's Raft instance created. After
// that point, dependencies can't be replaced, but they can be
// inspected or, if it makes sense, mutated.
type Node struct {
	// Apply this timeout to any Node method that could possibly
	// block indefinitely, and panic if the time expires during
	// the method execution. It's 5 seconds by default.
	Timeout time.Duration

	Config    *raft.Config
	FSM       raft.FSM
	Logs      raft.LogStore
	Stable    raft.StableStore
	Snapshots raft.SnapshotStore
	Peers     raft.PeerStore
	Transport raft.Transport

	notifyCh chan bool
	raft     *raft.Raft
}

// NewNode creates a new raft test node with sane defaults. All
// dependencies are created using in-memory implementations of the
// relevant interfaces. The default FSM is a dummy one that no-ops
// every method. If the given addr is the empty string, the node will
// start in single-node mode.
func NewNode(addr string) *Node {
	node := NewUnstartedNode(addr, ioutil.Discard)
	node.Start()
	return node
}

// NewUnstartedNode creates a new test raft Node without starting its raft
// engine.
func NewUnstartedNode(addr string, output io.Writer) *Node {
	notifyCh := make(chan bool, 0)

	config := defaultConfig()
	if addr == "" {
		addr = "0"
		config.EnableSingleNode = true
	} else {
		// Block leadership changes on this channel, it will
		// be used by Cluster to know about leadership
		// changes.
		config.NotifyCh = notifyCh
	}
	config.Logger = log.New(output, fmt.Sprintf("%s: ", addr), log.Lmicroseconds)

	_, transport := raft.NewInmemTransport(addr)

	return &Node{
		Timeout:   5 * time.Second,
		Config:    config,
		FSM:       &FSM{},
		Logs:      raft.NewInmemStore(),
		Stable:    raft.NewInmemStore(),
		Snapshots: raft.NewDiscardSnapshotStore(),
		Peers:     &raft.StaticPeers{},
		Transport: transport,
		notifyCh:  notifyCh,
	}
}

// Start the node by instantiating its raft instance with the
// configured dependencies. It panics if any error happens or if the
// nodes has already been started.
func (n *Node) Start() {
	if n.raft != nil {
		panic("this node has already been started")
	}

	raft, err := raft.NewRaft(
		n.Config,
		n.FSM,
		n.Logs,
		n.Stable,
		n.Snapshots,
		n.Peers,
		n.Transport,
	)
	if err != nil {
		panic(fmt.Sprintf("failed to start raft: %v", err))
	}

	n.raft = raft
}

// Shutdown turns of raft on this node. It panics if any error occurs.
func (n *Node) Shutdown() {
	// The shutdownFuture doesn't seem to possibly return any
	// error, so let's not bother checking.
	n.raft.Shutdown().Error()

	close(n.notifyCh)
}

// Raft is the raft.Raft instance that was created when Started was
// invoked, or nil if it wasn't.
func (n *Node) Raft() *raft.Raft {
	return n.raft
}

// LeaderKnown blocks the raft instance of this node sets a
// leader (which could possibly be the node itself).
func (n *Node) LeaderKnown() {
	observations := make(chan raft.Observation, 64)
	observer := raft.NewObserver(observations, false, leaderObservationFilter)

	n.raft.RegisterObserver(observer)
	defer n.raft.DeregisterObserver(observer)

	if n.raft.Leader() == "" {
		select {
		case <-observations:
		case <-time.After(n.Timeout):
			panic(fmt.Sprintf(
				"node %s not notified of elected leader",
				n.Transport.LocalAddr()))
		}
	}
}

// IsLeader returns true if the node is currently the leader, false
// otherwise.
func (n *Node) IsLeader() bool {
	return n.Raft().State() == raft.Leader
}

// Wrapper around raft.DefaultConfig() tweaking the default
// configuration for use with in-memory transports.
func defaultConfig() *raft.Config {
	config := raft.DefaultConfig()

	// Decrease timeouts, since everything happens in-memory by
	// default.
	config.HeartbeatTimeout = 50 * time.Millisecond
	config.ElectionTimeout = 50 * time.Millisecond
	config.LeaderLeaseTimeout = 50 * time.Millisecond
	config.CommitTimeout = 25 * time.Millisecond

	return config
}

// A raft.Observer filter function that returns true only if the
// observation is a leader change observation.
func leaderObservationFilter(observation *raft.Observation) bool {
	_, ok := observation.Data.(raft.LeaderObservation)
	return ok
}
