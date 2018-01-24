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
	"time"

	"github.com/hashicorp/raft"
)

// WaitLeader blocks until the given raft instance sets a leader (which
// could possibly be the instance itself).
//
// It fails the test if this doesn't happen within the specified timeout.
func WaitLeader(t testing.TB, raft *raft.Raft, timeout time.Duration) {
	helper, ok := t.(testingHelper)
	if ok {
		helper.Helper()
	}

	check := func() bool {
		return raft.Leader() != ""
	}
	wait(t, check, 25*time.Millisecond, timeout, "no leader was set")
}
