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
// Each raft.Raft instance is created with sane test-oriented default
// dependencies, which include:
//
// - very low configuration timeouts
// - in-memory transports which are automatically connected to each other
// - in-memory log stores
// - in-memory snapshot stores
//
// All the created nodes will part of the cluster and act as voting servers,
// unless the Servers knob is used.
func Cluster(t testing.TB, fsms []raft.FSM, knobs ...Knob) ([]*raft.Raft, func()) {
	helper, ok := t.(testingHelper)
	if ok {
		helper.Helper()
	}

	n := len(fsms)
	cluster := &cluster{
		t:     t,
		nodes: make(map[int]*node, n),
	}

	for i := 0; i < n; i++ {
		cluster.nodes[i] = newDefaultNode(t, i)
	}

	for _, knob := range knobs {
		knob.init(cluster)
	}

	servers := make([]raft.Server, 0)
	for i, node := range cluster.nodes {
		if !node.Bootstrap {
			continue
		}
		server := raft.Server{
			ID:      raft.ServerID(strconv.Itoa(i)),
			Address: node.Transport.LocalAddr(),
		}
		servers = append(servers, server)
	}

	bootstrapCluster(t, cluster.nodes)

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
	}

	return rafts, cleanup
}

// Knob can be used to tweak the dependencies of test Raft nodes created with
// Cluster() or Node().
type Knob interface {
	init(*cluster)
}

// Shutdown all the given raft nodes and fail the test if any of them errors
// out while doing so.
func Shutdown(t testing.TB, rafts []*raft.Raft) {
	helper, ok := t.(testingHelper)
	if ok {
		helper.Helper()
	}

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
	t     testing.TB
	nodes map[int]*node // Options for node N.
}

// Hold dependencies for a single node.
type node struct {
	Config        *raft.Config
	Logs          raft.LogStore
	Stable        raft.StableStore
	Snapshots     raft.SnapshotStore
	Configuration *raft.Configuration
	Transport     raft.Transport
	Bootstrap     bool // Whether to bootstrap the node, making it join the cluster
}

// Create default dependencies for a single raft node.
func newDefaultNode(t testing.TB, i int) *node {
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
		Bootstrap: true,
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

// Bootstrap the cluster, by connecting the appropriate nodes to each other and
// setting up their initial configuration.
func bootstrapCluster(t testing.TB, nodes map[int]*node) {
	helper, ok := t.(testingHelper)
	if ok {
		helper.Helper()
	}

	servers := make([]raft.Server, 0)
	for i, node1 := range nodes {
		if !node1.Bootstrap {
			continue
		}
		server := raft.Server{
			ID:      raft.ServerID(strconv.Itoa(i)),
			Address: node1.Transport.LocalAddr(),
		}
		servers = append(servers, server)

		for _, node2 := range nodes {
			if node2 == node1 || !node2.Bootstrap {
				continue
			}
			peers, ok := node1.Transport.(raft.WithPeers)
			if !ok {
				t.Fatalf("transport of node %d does not implement WithPeers", i)
			}
			peers.Connect(node2.Transport.LocalAddr(), node2.Transport)
		}
	}

	configuration := raft.Configuration{}
	configuration.Servers = servers

	for _, node := range nodes {
		if !node.Bootstrap {
			continue
		}
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
