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
	"context"
	"fmt"
	"io"
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/CanonicalLtd/raft-test/internal/raftext"
	"github.com/hashicorp/raft"
)

// Cluster creates n raft nodes, one for each of the given FSMs, and returns a
// Control object that can be used to observe and manipulate them.
//
// Each raft.Raft instance is created with sane test-oriented default
// dependencies, which include:
//
// - very low configuration timeouts
// - in-memory transports which are automatically connected to each other
// - in-memory log and stable stores
// - in-memory snapshot stores
//
// All created nodes will be part of the cluster and act as voting servers,
// unless the Servers knob is used.
//
// If a GO_RAFT_TEST_LATENCY environment is found, the default configuration
// timeouts will be scaled up accordingly (useful when running tests on slow
// hardware). A latency of 1.0 is a no-op, since it just keeps the default
// values unchanged. A value greater than 1.0 increases the default timeouts by
// that factor. See also the Duration and Latency helpers.
func Cluster(t testing.TB, fsms []raft.FSM, knobs ...Knob) ([]*raft.Raft, *Control) {
	helper, ok := t.(testingHelper)
	if ok {
		helper.Helper()
	}

	// Create a set of default dependencies for each node.
	n := len(fsms)
	nodes := make([]*node, n)
	for i := 0; i < n; i++ {
		nodes[i] = newDefaultNode(t, i)
	}

	// Customize the default dependencies by applying the given knobs.
	for _, knob := range knobs {
		knob(nodes)
	}

	// Honor the GO_RAFT_TEST_LATENCY env var, if set.
	scaleTimeouts(nodes)

	// Create notification channels for all nodes.
	notifyChs := createNotifyChs(t, nodes)

	// Create log wrappers for all nodes.
	logWrappers := createLogWrappers(nodes)

	// Bootstrap the initial cluster configuration.
	bootstrapCluster(t, nodes)

	// Create a watcher for the given fsms.
	fsmsWacher := newFSMsWatcher(fsms)

	// Create a watcher for the log stores.
	logsWacher := newLogsWatcher(logWrappers)

	// Create a watcher for the notification channels
	notifyWatcher := newNotifyWatcher(notifyChs)

	// Create a new network helper
	transports := detectLoobackTransports(t, nodes)
	network := newNetwork(transports)

	// Start the individual nodes.
	rafts := make([]*raft.Raft, n)
	for i, fsm := range fsmsWacher.FSMs() {
		t.Logf("raft-test: node %d: start", i)
		raft, err := newRaft(fsm, nodes[i])
		if err != nil {
			t.Fatalf("raft-test: node %d: start error: %v", i, err)
		}
		rafts[i] = raft
	}

	// Create the Control instance for this cluster
	control := &Control{
		t:             t,
		fsmsWatcher:   fsmsWacher,
		logsWatcher:   logsWacher,
		notifyWatcher: notifyWatcher,
		network:       network,
		rafts:         rafts,
	}

	return rafts, control
}

// Knob can be used to tweak the dependencies of test Raft nodes created with
// Cluster() or Node().
type Knob func([]*node)

// Shutdown all the given raft nodes and fail the test if any of them errors
// out while doing so.
func Shutdown(t testing.TB, rafts []*raft.Raft) {
	helper, ok := t.(testingHelper)
	if ok {
		helper.Helper()
	}

	// Trigger the shutdown on all the nodes.
	futures := make([]raft.Future, len(rafts))
	for i, r := range rafts {
		t.Logf("raft-test: node %d: shutdown", i)
		futures[i] = r.Shutdown()
	}

	// Expect the shutdown to happen within a second by default.
	timeout := Duration(time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Watch for errors.
	errors := make([]chan error, len(rafts))
	for i, future := range futures {
		errors[i] = make(chan error)
		go func(i int, future raft.Future) {
			errors[i] <- future.Error()
		}(i, future)
	}
	for i := range futures {
		var err error
		select {
		case err = <-errors[i]:
		case <-ctx.Done():
			t.Fatalf("raft-test: node %d: did not shutdown within %s", i, timeout)
		}
		if err != nil {
			t.Fatalf("raft-test: node %d: shutdown error: %v", i, err)
			cancel()
		}
	}
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
	helper, ok := t.(testingHelper)
	if ok {
		helper.Helper()
	}

	// Use the node's index as its server address.
	addr := strconv.Itoa(i)
	_, transport := raftext.NewInmemTransportWithTimeout(
		raft.ServerAddress(addr), Duration(50*time.Millisecond))

	out := TestingWriter(t)
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(addr)
	config.Logger = log.New(out, fmt.Sprintf("%s: ", addr), log.Ltime|log.Lmicroseconds)

	config.HeartbeatTimeout = 20 * time.Millisecond
	config.ElectionTimeout = 20 * time.Millisecond
	config.CommitTimeout = 1 * time.Millisecond
	config.LeaderLeaseTimeout = 10 * time.Millisecond

	store := raft.NewInmemStore()
	node := &node{
		Config:    config,
		Logs:      store,
		Stable:    store,
		Snapshots: raft.NewInmemSnapshotStore(),
		Transport: transport,
		Bootstrap: true,
	}

	return node
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

// Scale the timeouts of the nodes to match GO_RAFT_TEST_LATENCY, is set.
func scaleTimeouts(nodes []*node) {
	for _, node := range nodes {
		config := node.Config
		config.HeartbeatTimeout = Duration(config.HeartbeatTimeout)
		config.ElectionTimeout = Duration(config.ElectionTimeout)
		config.CommitTimeout = Duration(config.CommitTimeout)
		config.LeaderLeaseTimeout = Duration(config.LeaderLeaseTimeout)
	}
}

// Bootstrap the cluster, by connecting the appropriate nodes to each other and
// setting up their initial configuration.
func bootstrapCluster(t testing.TB, nodes []*node) {
	helper, ok := t.(testingHelper)
	if ok {
		helper.Helper()
	}

	// Figure out which nodes should be part of the initial configuration,
	// and connect their transports to each other.
	servers := make([]raft.Server, 0)
	for i, node := range nodes {
		if !node.Bootstrap {
			// If the node is not initially part of the cluster,
			// there's nothing to do.
			t.Logf("raft-test: node %d: skip bootstrap (not part of initial configuration)", i)
			continue
		}
		server := raft.Server{
			ID:      raft.ServerID(strconv.Itoa(i)),
			Address: node.Transport.LocalAddr(),
		}
		servers = append(servers, server)

		// Connect the node's transport to the transports of all other
		// nodes that are initially part of the cluster.
		for j, other := range nodes {
			if j == i || !other.Bootstrap {
				// This node is not part of the cluster, don't connect to it.
				continue
			}
			peers, ok := node.Transport.(raft.WithPeers)
			if !ok {
				t.Logf("raft-test: node %d: transport does not implement WithPeers", j)
				continue
			}
			peers.Connect(other.Transport.LocalAddr(), other.Transport)
		}
	}

	// Create the initial cluster configuration.
	configuration := raft.Configuration{Servers: servers}
	for i := 0; i < len(nodes); i++ {
		node := nodes[i]
		if !node.Bootstrap {
			continue
		}
		t.Logf("raft-test: node %d: bootstrap", i)
		err := raft.BootstrapCluster(
			node.Config,
			node.Logs,
			node.Stable,
			node.Snapshots,
			node.Transport,
			configuration,
		)
		if err != nil {
			t.Fatalf("raft-test: node %d: bootstrap error: %v", i, err)
		}
	}

}

// Create notification channels for all nodes.
func createNotifyChs(t testing.TB, nodes []*node) []chan bool {
	helper, ok := t.(testingHelper)
	if ok {
		helper.Helper()
	}

	notifyChs := make([]chan bool, len(nodes))
	for i, node := range nodes {
		if node.Config.NotifyCh != nil {
			t.Fatalf("no support for user-defined notification channels")
		}
		// Use a large pool, so raft won't block on us and tests can proceed
		// asynchronously.
		notifyChs[i] = make(chan bool, 1000)
		node.Config.NotifyCh = notifyChs[i]
	}

	return notifyChs
}

// Wrap log stores with an observable logWrapper.
func createLogWrappers(nodes []*node) []*logWrapper {
	wrappers := make([]*logWrapper, len(nodes))
	for i, node := range nodes {
		wrappers[i] = newLogWrapper(node.Logs)
		node.Logs = wrappers[i]
	}
	return wrappers
}

// Detect loopback transports from nodes that have them.
func detectLoobackTransports(t testing.TB, nodes []*node) []raft.LoopbackTransport {
	helper, ok := t.(testingHelper)
	if ok {
		helper.Helper()
	}

	transports := make([]raft.LoopbackTransport, len(nodes))
	for i, node := range nodes {
		loopback, ok := node.Transport.(raft.LoopbackTransport)
		if !ok {
			// Non-loopback transports are ignored. If the user
			// tries to disconnect them the test will fail at that
			// time.
			t.Logf("raft-test: node %d: warning: transport does not implement LoopbackTransport", i)
			continue
		}
		transports[i] = loopback
	}

	return transports
}

// TestingWriter returns an io.Writer that forwards the stream it receives to
// the Logf function of the given testing instance.
func TestingWriter(t testing.TB) io.Writer {
	return &testingWriter{t: t}
}

// Implement io.Writer and forward what it receives to a
// t.Testing logger.
type testingWriter struct {
	t testing.TB
}

func (w *testingWriter) Write(p []byte) (n int, err error) {
	w.t.Logf(string(p))
	return len(p), nil
}

// For compatibility with Go <1.9
type testingHelper interface {
	Helper()
}
