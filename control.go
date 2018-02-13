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
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// Control lets users watch and modify a cluster of test raft instances.
type Control struct {
	t             testing.TB
	fsmsWatcher   *fsmsWatcher
	logsWatcher   *logsWatcher
	notifyWatcher *notifyWatcher
	network       *network
	rafts         []*raft.Raft
}

// Close the control for this raft cluster, stopping all monitoring goroutines.
//
// It must be called by every test creating a test cluster with Cluster().
func (c *Control) Close() {
	c.t.Helper()

	c.t.Logf("raft-test: shutdown cluster")

	Shutdown(c.t, c.rafts)

	c.notifyWatcher.Close()
}

// Index returns the index of the given raft instance.
func (c *Control) Index(raft *raft.Raft) int {
	c.t.Helper()

	for i := range c.rafts {
		if c.rafts[i] == raft {
			return i
		}
	}
	c.t.Fatalf("raft-test: unknown raft instance at %p", raft)
	return -1
}

// Other returns a raft instance of the cluster which is different from the
// given one.
func (c *Control) Other(rafts ...*raft.Raft) *raft.Raft {
	for _, this := range c.rafts {
		different := true
		for _, other := range rafts {
			if this == other {
				different = false
				break
			}
		}
		if different {
			return this
		}
	}
	return nil
}

// LeadershipAcquired blocks until one of the nodes in the cluster changes its state
// to raft.Leader.
//
// It returns the raft instance that became leader.
//
// It fails the test if no node has acquired leadership within the timeout.
//
// In case GO_RAFT_TEST_LATENCY is set, the timeout will be transparently
// scaled by that factor.
func (c *Control) LeadershipAcquired(timeout time.Duration) *raft.Raft {
	c.t.Helper()

	timeout = Duration(timeout)
	c.t.Logf("raft-test: wait for a non-leader node to acquire leadership within %s", timeout)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	info := c.notifyWatcher.Next(ctx)
	if info == nil {
		c.t.Fatalf("raft-test: no node changed leadership state")
	}

	c.t.Logf("raft-test: node %d: leadership %s", info.On, info.Verb())

	if !info.Acquired {
		c.t.Fatalf("raft-test: node %d: lost leadership instead of acquiring it", info.On)
	}

	// Sanity check that the state is actually the right one. This should
	// always be true with the current raft code.
	r := c.rafts[info.On]

	if state := r.State(); state != raft.Leader {
		c.t.Fatalf("raft-test: node %d: unexpected state %s", info.On, state)
	}

	return r
}

// LeadershipLost blocks until the given raft instance notifies that it has
// lost leadership.
//
// It fails the test if the raft instance doesn't lose leadership within the timeout.
//
// In case GO_RAFT_TEST_LATENCY is set, the timeout will be transparently
// scaled by that factor.
func (c *Control) LeadershipLost(r *raft.Raft, timeout time.Duration) {
	c.t.Helper()

	timeout = Duration(timeout)

	i := c.Index(r)
	c.t.Logf("raft-test: node %d: wait to lose leadership within %s", i, timeout)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	info := c.notifyWatcher.Next(ctx)
	if info == nil {
		c.t.Fatalf("raft-test: no node changed leadership state")
	}

	c.t.Logf("raft-test: node %d: leadership %s", info.On, info.Verb())

	if info.Acquired {
		c.t.Fatalf("raft-test: node %d: leadership acquired by %d instead", i, info.On)
	}

	if state := r.State(); state == raft.Leader {
		c.t.Fatalf("raft-test: node %d: unexpected state %s", info.On, state)
	}
}

// Disconnect the given raft instance from the others.
//
// Requires that the transports to implement LoopbackTransports.
func (c *Control) Disconnect(r *raft.Raft) {
	c.t.Helper()

	i := c.Index(r)
	c.t.Logf("raft-test: node %d: disconnect", i)

	err := c.network.Disconnect(i)
	if err != nil {
		c.t.Fatalf("raft-test: node %d: disconnect error: %v", i, err)
	}
}

// Reconnect the given raft instance to all the others.
//
// Requires that the transports to implement LoopbackTransports.
func (c *Control) Reconnect(r *raft.Raft) {
	c.t.Helper()

	i := c.Index(r)
	c.t.Logf("raft-test: node %d: reconnect", i)

	err := c.network.Reconnect(i)
	if err != nil {
		c.t.Fatalf("raft-test: node %d: reconnect error: %v", i, err)
	}
}

// BeforeStoreLog sets a hook triggering when the log store of the given raft
// instance is about to store the the log with the given index.
func (c *Control) BeforeStoreLog(r *raft.Raft, index uint64, hook func()) {
	i := c.Index(r)
	c.logsWatcher.BeforeStoreLog(i, index, hook)
}

// BeforeApply sets a hook triggering when the FSM of the given raft instance
// is about to apply the log command with given index.
func (c *Control) BeforeApply(r *raft.Raft, index uint64, hook func()) {
	i := c.Index(r)
	c.fsmsWatcher.BeforeApply(i, index, hook)
}

// AfterApply sets a hook triggering when the FSM of the given raft
// instance has applied the log command with the given index.
func (c *Control) AfterApply(r *raft.Raft, index uint64, hook func()) {
	i := c.Index(r)
	c.fsmsWatcher.AfterApply(i, index, hook)
}

// AppliedIndex returns the index of the last log applied by the FSM of the
// given raft instance.
func (c *Control) AppliedIndex(r *raft.Raft) uint64 {
	i := c.Index(r)
	return c.fsmsWatcher.AppliedIndex(i)
}

// WaitIndex waits until the FSM of the given raft instance reaches at least
// the given index.
//
// It fails the test if this does not happen withing the given timeout.
//
// In case GO_RAFT_TEST_LATENCY is set, the timeout will be transparently
// scaled by that factor.
func (c *Control) WaitIndex(r *raft.Raft, index uint64, timeout time.Duration) {
	c.t.Helper()

	i := c.Index(r)
	c.t.Logf("raft-test: node %d: wait for FSM to apply index %d", i, index)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// First set up a hook for intercepting the log.
	c.fsmsWatcher.AfterApply(i, index, func() {
		cancel()
	})

	// Secondly poll the wrapper to check if the FSM has reached the
	// desired apply count (this can happen if it started before we
	// could set up the hook).
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if current := c.fsmsWatcher.AppliedIndex(i); current >= index {
				cancel()
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	// Wait for either the hook or the poller to cancel the context.
	select {
	case <-ctx.Done():
		c.t.Logf("raft-test: node %d: FSM applied index %d", i, index)
	case <-time.After(timeout):
		current := c.fsmsWatcher.AppliedIndex(i)
		c.t.Fatalf("raft-test: node %d: FSM did not apply index %d within %s (current %d)",
			i, index, timeout, current)
	}
}

// WaitSnapshot waits until the FSM of the given raft instance has performed at
// least the given number of snapshots
//
// It fails the test if this does not happen withing the given timeout.
//
// In case GO_RAFT_TEST_LATENCY is set, the timeout will be transparently
// scaled by that factor.
func (c *Control) WaitSnapshot(r *raft.Raft, n int, timeout time.Duration) {
	c.t.Helper()

	timeout = Duration(timeout)

	i := c.Index(r)
	c.t.Logf("raft-test: node %d: wait for FSM to perform snapshot %d", i, n)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// First set up a hook for intercepting the snapshot.
	c.fsmsWatcher.AfterSnapshot(i, n, func() {
		cancel()
	})

	// Secondly poll the wrapper to check if the FSM has reached the
	// desired snapshot count (this can happen if it startedt before we
	// could set up the hook).
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if current := c.fsmsWatcher.SnapshotCount(i); current >= n {
				cancel()
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	// Wait for the hook to fire the channel
	select {
	case <-ctx.Done():
		c.t.Logf("raft-test: node %d: FSM performed %d snapshots", i, n)
	case <-time.After(timeout):
		c.t.Fatalf("raft-test: node %d: FSM did not perform snapshot %d within %s", i, n, timeout)
	}
}

// WaitRestore waits until the FSM of the given raft instance has restored at least
// the given number of snapshots.
//
// It fails the test if this does not happen withing the given timeout.
//
// In case GO_RAFT_TEST_LATENCY is set, the timeout will be transparently
// scaled by that factor.
func (c *Control) WaitRestore(r *raft.Raft, n int, timeout time.Duration) {
	c.t.Helper()

	timeout = Duration(timeout)

	i := c.Index(r)
	c.t.Logf("raft-test: node %d: wait for FSM to perform restore %d", i, n)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// First set up a hook for intercepting the restore.
	c.fsmsWatcher.AfterRestore(i, n, func() {
		cancel()
	})

	// Secondly poll the wrapper to check if the FSM has reached the
	// desired restore count (this can happen if it startedt before we
	// could set up the hook).
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if current := c.fsmsWatcher.RestoreCount(i); current >= n {
				cancel()
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	// Wait for the hook to fire the channel
	select {
	case <-ctx.Done():
		c.t.Logf("raft-test: node %d: FSM restored %d snapshots", i, n)
	case <-time.After(timeout):
		c.t.Fatalf("raft-test: node %d: FSM did not perform restore %d within %s", i, n, timeout)
	}
}

// WaitLeader blocks until the given raft instance sets a leader (which
// could possibly be the instance itself).
//
// It fails the test if this doesn't happen within the specified timeout.
func WaitLeader(t testing.TB, raft *raft.Raft, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	waitLeader(ctx, t, raft)
}

func waitLeader(ctx context.Context, t testing.TB, raft *raft.Raft) {
	helper, ok := t.(testingHelper)
	if ok {
		helper.Helper()
	}

	check := func() bool {
		return raft.Leader() != ""
	}
	wait(ctx, t, check, 25*time.Millisecond, "no leader was set")
}

// Poll the given function at the given internval, until it returns true, or
// the given context expires.
func wait(ctx context.Context, t testing.TB, f func() bool, interval time.Duration, message string) {
	helper, ok := t.(testingHelper)
	if ok {
		helper.Helper()
	}

	start := time.Now()
	for {
		select {
		case <-ctx.Done():
			if err := ctx.Err(); err == context.Canceled {
				return
			}
			t.Fatalf("%s within %s", message, time.Since(start))
		default:
		}
		if f() {
			return
		}
		time.Sleep(interval)
	}
}
