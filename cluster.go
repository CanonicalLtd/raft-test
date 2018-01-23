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
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// Cluster creates n raft nodes, one for each of the given FSMs.
//
// Each raft.Raft instance is created with sane test-oriented dependencies,
// such as in-memory transports and very low timeouts.
func Cluster(t *testing.T, fsms []raft.FSM, knobs ...Knob) ([]*raft.Raft, func()) {
	n := len(fsms)
	cluster := &cluster{
		t:           t,
		nodes:       make(map[int]*node, n),
		autoConnect: true,
	}

	servers := make([]raft.Server, n)
	transports := make([]raft.LoopbackTransport, n)
	for i := 0; i < n; i++ {
		cluster.nodes[i] = newDefaultNode(t, i)
		transports[i] = cluster.nodes[i].Transport.(raft.LoopbackTransport)
		servers[i] = raft.Server{
			ID:      raft.ServerID(strconv.Itoa(i)),
			Address: transports[i].LocalAddr(),
		}
	}

	for _, knob := range knobs {
		knob.init(cluster)
	}

	configuration := raft.Configuration{}
	if cluster.autoConnect {
		connectLoobackTransports(transports)
		configuration.Servers = servers
		bootstrapCluster(t, cluster.nodes, configuration)
	} else {
		configuration.Servers = servers[0:1]
		nodes := map[int]*node{0: cluster.nodes[0]}
		bootstrapCluster(t, nodes, configuration)
	}

	rafts := make([]*raft.Raft, n)
	for i := range fsms {
		raft, err := newRaft(fsms[i], cluster.nodes[i])
		if err != nil {
			t.Fatalf("failed to start test raft node %d: %v", i, err)
		}
		rafts[i] = raft
	}

	cleanup := func() {
		Shutdown(t, rafts)
		for _, knob := range knobs {
			knob.cleanup(cluster)
		}
	}

	return rafts, cleanup
}

// Knob can be used to tweak the dependencies of test Raft nodes created with
// Cluster() or Node().
type Knob interface {
	init(*cluster)
	cleanup(*cluster)
}

// Shutdown all the given raft nodes and fail the test if any of them errors
// out while doing so.
func Shutdown(t *testing.T, rafts []*raft.Raft) {
	futures := make([]raft.Future, len(rafts))
	for i, r := range rafts {
		futures[i] = r.Shutdown()
	}
	for i, future := range futures {
		if err := future.Error(); err != nil {
			t.Fatalf("failed to shutdown raft node %d: %v", i, err)
		}
	}
}

// Other the index of a raft.Raft node which differs from the given one.
//
// This is useful in combination with Notify to get a node that is not
// currently in leader state.
func Other(rafts []*raft.Raft, i int) int {
	for j := range rafts {
		if i != j {
			return j
		}
	}
	return -1
}

type cluster struct {
	t           *testing.T
	nodes       map[int]*node // Options for node N.
	autoConnect bool          // Whether to automatically connect the transports
}

// Hold dependencies for a single node.
type node struct {
	Config        *raft.Config
	Logs          raft.LogStore
	Stable        raft.StableStore
	Snapshots     raft.SnapshotStore
	Configuration *raft.Configuration
	Transport     raft.Transport
}

// Create default dependencies for a single raft node.
func newDefaultNode(t *testing.T, i int) *node {
	addr := strconv.Itoa(i)
	_, transport := raft.NewInmemTransport(raft.ServerAddress(addr))

	out := TestingWriter(t)
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(addr)
	config.Logger = log.New(out, fmt.Sprintf("%s: ", addr), 0)

	// Decrease timeouts, since everything happens in-memory by
	// default.
	config.HeartbeatTimeout = 50 * time.Millisecond
	config.ElectionTimeout = 50 * time.Millisecond
	config.LeaderLeaseTimeout = 50 * time.Millisecond
	config.CommitTimeout = 25 * time.Millisecond

	options := &node{
		Config:    config,
		Logs:      raft.NewInmemStore(),
		Stable:    raft.NewInmemStore(),
		Snapshots: raft.NewDiscardSnapshotStore(),
		Transport: transport,
	}

	return options
}

// Convenience around raft.NewRaft for creating a new Raft instance using the
// given dependencies.
func newRaft(fsm raft.FSM, node *node) (*raft.Raft, error) {
	return raft.NewRaft(
		node.Config,
		fsm,
		node.Logs,
		node.Stable,
		node.Snapshots,
		node.Transport,
	)
}

// Connect all the given loopback transports to each other.
func connectLoobackTransports(transports []raft.LoopbackTransport) {
	for i, transport1 := range transports {
		for j, transport2 := range transports {
			if i != j {
				transport1.Connect(transport2.LocalAddr(), transport2)
				transport2.Connect(transport1.LocalAddr(), transport1)
			}
		}
	}
}

func bootstrapCluster(t *testing.T, nodes map[int]*node, configuration raft.Configuration) {
	for _, node := range nodes {
		err := raft.BootstrapCluster(
			node.Config,
			node.Logs,
			node.Stable,
			node.Snapshots,
			node.Transport,
			configuration,
		)
		if err != nil {
			t.Fatalf("failed to bootstrap cluster: %v", err)
		}
	}
}
