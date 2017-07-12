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

package rafttest_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dqlite/raft-test"
	"github.com/hashicorp/raft"
)

func TestCluster_LogOutput(t *testing.T) {
	cluster := rafttest.NewCluster(3)
	cluster.Start()
	defer cluster.Shutdown()

	cluster.LeadershipAcquired()
	if output := cluster.LogOutput.String(); !strings.Contains(output, "[DEBUG] raft") {
		t.Errorf("output does not seem to be a raft log:\n%s", output)
	}
}

func TestCluster_AutoConnectWithNonLoopbackTransport(t *testing.T) {
	cluster := rafttest.NewCluster(2)
	cluster.Node(0).Transport, _ = raft.NewTCPTransport("", nil, 0, time.Second, nil)
	const want = "transport does not implement loopback interface"
	defer func() {
		got := recover()
		if want != got {
			t.Fatalf("expected panic\n%q\ngot\n%q", want, got)
		}
	}()
	cluster.Start()
}

func TestCluster_PeerStoreError(t *testing.T) {
	dir, err := ioutil.TempDir("", "go-rafttest-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	ioutil.WriteFile(filepath.Join(dir, "peers.json"), []byte("}gar![age"), 0600)

	cluster := rafttest.NewCluster(3)
	node := cluster.Node(0)
	node.Peers = raft.NewJSONPeers(dir, node.Transport)

	const want = "failed to get peers for node 0"
	defer func() {
		got, ok := recover().(string)
		if !ok {
			t.Errorf("recover didn't return a string")
		}
		if !strings.Contains(got, want) {
			t.Errorf("expected\n%q\nto contain\n%q", got, want)
		}
	}()

	cluster.Start()
}

func TestCluster_LeadershipChanged(t *testing.T) {
	cluster := rafttest.NewCluster(3)
	cluster.Start()
	defer cluster.Shutdown()

	node := cluster.LeadershipAcquired()
	if !node.IsLeader() {
		t.Fatalf("node is not leader state %s", node.Raft().State())
	}
	cluster.Disconnect(node)
	if cluster.LeadershipLost() != node {
		t.Fatalf("leader node was not the one losing leadership")
	}
}

func TestCluster_LeadershipChangedTimeout(t *testing.T) {
	cluster := rafttest.NewCluster(3)
	cluster.Start()
	defer cluster.Shutdown()

	cluster.LeadershipAcquired()
	cluster.Timeout = time.Microsecond

	const want = "timeout waiting for leadership change"
	defer func() {
		got := recover()
		if want != got {
			t.Fatalf("expected panic\n%q\ngot\n%q", want, got)
		}
	}()
	cluster.LeadershipAcquired()
}

func TestCluster_LeaderKnown(t *testing.T) {
	cluster := rafttest.NewCluster(3)
	cluster.Start()
	defer cluster.Shutdown()

	leader := cluster.LeadershipAcquired()
	leader.Raft().VerifyLeader().Error()
	follower := cluster.Peers(leader)[0]
	follower.LeaderKnown()

	want := leader.Transport.LocalAddr()
	got := follower.Raft().Leader()
	if want != got {
		t.Errorf("expected leader address\n%q\ngot\n%q", want, got)
	}
}
