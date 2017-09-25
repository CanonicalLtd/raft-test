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
	"io/ioutil"
	"os"
	"testing"

	"github.com/hashicorp/raft"
)

// FileSnapshots configures the nodes to use FileSnapshotStores backed by
// temporary dirs instead of the default DiscardSnapshotStore.
func FileSnapshots() *FileSnapshotsKnob {
	return &FileSnapshotsKnob{}
}

// FileSnapshotsKnob gives access to the FileSnapshotStore objects used by the
// various nodes.
type FileSnapshotsKnob struct {
	t      *testing.T
	dirs   []string
	stores []*raft.FileSnapshotStore
}

func (k *FileSnapshotsKnob) init(cluster *cluster) {
	n := len(cluster.nodes)
	k.t = cluster.t
	k.dirs = make([]string, n)
	k.stores = make([]*raft.FileSnapshotStore, n)
	for i, node := range cluster.nodes {
		dir, err := ioutil.TempDir("", "go-rafttest-snapshot-")
		if err != nil {
			k.t.Fatalf("can't create snapshot temp dir: %v", err)
		}
		store, err := raft.NewFileSnapshotStore(dir, 3, ioutil.Discard)
		if err != nil {
			k.t.Fatalf("can't create snapshot store: %v", err)
		}
		node.Snapshots = store
		k.dirs[i] = dir
		k.stores[i] = store
	}
}

func (k *FileSnapshotsKnob) cleanup(cluster *cluster) {
	for _, dir := range k.dirs {
		if err := os.RemoveAll(dir); err != nil {
			k.t.Fatalf("failed to cleanup snapshot dir: %v", err)
		}
	}
}
