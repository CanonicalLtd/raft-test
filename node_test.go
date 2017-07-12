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

func TestNode_StartAndShutdown(t *testing.T) {
	node := rafttest.NewNode()
	node.Start()
	defer node.Shutdown()
}

func TestNode_StartTwice(t *testing.T) {
	node := rafttest.NewNode()
	node.Start()
	defer node.Shutdown()

	const want = "this node has already been started"
	defer func() {
		got := recover()
		if want != got {
			t.Errorf("expected\n%q\ngot\n%q", want, got)
		}
	}()

	node.Start()
}

func TestNode_LeaderKnown(t *testing.T) {
	node := rafttest.NewNode()
	node.Config.EnableSingleNode = true
	node.Start()
	defer node.Shutdown()

	node.LeaderKnown()

	if node.Raft().Leader() == "" {
		t.Error("leader is still unknown")
	}
}

func TestNode_LeaderKnownTimeout(t *testing.T) {
	node := rafttest.NewNode()
	_, node.Transport = raft.NewInmemTransport("0")
	node.Start()
	defer node.Shutdown()

	node.Timeout = time.Microsecond

	const want = "node 0 not notified of elected leader"
	defer func() {
		got := recover()
		if want != got {
			t.Errorf("expected\n%q\ngot\n%q", want, got)
		}
	}()

	node.LeaderKnown()
}

func TestNode_RaftApplyAndSnapshot(t *testing.T) {
	node := rafttest.NewNode()
	node.Config.EnableSingleNode = true
	node.Start()
	defer node.Shutdown()
	node.LeaderKnown()

	if err := node.Raft().Apply([]byte{}, time.Second).Error(); err != nil {
		t.Fatal(err)
	}

	if err := node.Raft().Snapshot().Error(); err != nil {
		t.Fatal(err)
	}
}

func TestNode_StartRaftError(t *testing.T) {
	dir, err := ioutil.TempDir("", "go-rafttest-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	ioutil.WriteFile(filepath.Join(dir, "peers.json"), []byte("}gar![age"), 0600)

	node := rafttest.NewNode()
	node.Peers = raft.NewJSONPeers(dir, node.Transport)

	const want = "failed to start raft"
	defer func() {
		got, ok := recover().(string)
		if !ok {
			t.Errorf("recover didn't return a string")
		}
		if !strings.Contains(got, want) {
			t.Errorf("expected\n%q\nto contain\n%q", got, want)
		}
	}()

	node.Start()
}
