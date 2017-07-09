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
)

func TestCluster_consumeNotifyChPanicsIfNoValidNotifyCh(t *testing.T) {
	cluster := NewUnstartedCluster(1)
	node := cluster.Node(0)
	node.Config.NotifyCh = make(chan bool)

	const want = "no valid notification channel for node 0"
	defer func() {
		got := recover()
		if want != got {
			t.Errorf("expected panic\n%q\ngot\n%q", want, got)
		}
	}()

	cluster.consumeNotifyCh()
}
