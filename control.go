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

// Control lets users watch and modify the a cluster for test raft instances.
type Control struct {
	t             testing.TB
	fsmsWatcher   *fsmsWatcher
	notifyWatcher *notifyWatcher
	network       *network
	rafts         []*raft.Raft
}

// Close the control for this raft cluster, stopping a monitoring goroutines.
//
// It must be called by every test creating a test cluster with Cluster().
func (c *Control) Close() {
	helper, ok := c.t.(testingHelper)
	if ok {
		helper.Helper()
	}

	c.t.Logf("raft-test: shutdown cluster")

	Shutdown(c.t, c.rafts)

	c.notifyWatcher.Close()
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
func (c *Control) LeadershipAcquired(timeout time.Duration) *raft.Raft {
	helper, ok := c.t.(testingHelper)
	if ok {
		helper.Helper()
	}

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
func (c *Control) LeadershipLost(r *raft.Raft, timeout time.Duration) {
	helper, ok := c.t.(testingHelper)
	if ok {
		helper.Helper()
	}

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
		c.t.Fatalf("raft-test: node %d: acquired leadership instead of losing it", info.On)
	}

	if state := r.State(); state == raft.Leader {
		c.t.Fatalf("raft-test: node %d: unexpected state %s", info.On, state)
	}
}

// Disconnect the given raft instance from the others.
//
// Requires that the transports to implement LoopbackTransports.
func (c *Control) Disconnect(r *raft.Raft) {
	helper, ok := c.t.(testingHelper)
	if ok {
		helper.Helper()
	}

	i := c.Index(r)
	c.t.Logf("raft-test: node %d: disconnect", i)

	err := c.network.Disconnect(i)
	if err != nil {
		c.t.Fatalf("raft-test: disconnect error: %v", err)
	}
}

// Reconnect the given raft instance to all the others.
//
// Requires that the transports to implement LoopbackTransports.
func (c *Control) Reconnect(r *raft.Raft) {
	helper, ok := c.t.(testingHelper)
	if ok {
		helper.Helper()
	}

	i := c.Index(r)
	c.t.Logf("raft-test: node %d: reconnect", i)

	err := c.network.Reconnect(i)
	if err != nil {
		c.t.Fatalf("raft-test: reconnect error: %v", err)
	}
}

// AppliedIndex returns the index of the last log applied by the FSM of the
// given raft instance.
func (c *Control) AppliedIndex(r *raft.Raft) uint64 {
	i := c.Index(r)
	return c.fsmsWatcher.ApplyIndex(i)
}

// ApplyHook sets a hook triggering when the FSM reaches the given index.
func (c *Control) ApplyHook(r *raft.Raft, index uint64, hook func()) {
	i := c.Index(r)
	c.fsmsWatcher.ApplyHook(i, func(current uint64) {
		if current == index {
			hook()
			c.fsmsWatcher.ApplyHook(i, nil)
		}
	})
}

// FindLeader blocks until one of the given raft instance sets a leader, and returns
// the its index.
//
// It fails the test if this doesn't happen within the specified timeout.
func (c *Control) FindLeader(timeout time.Duration) *raft.Raft {
	helper, ok := c.t.(testingHelper)
	if ok {
		helper.Helper()
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	start := time.Now()
	for _, r := range c.rafts {
		go func(r *raft.Raft) {
			waitLeader(ctx, c.t, r)
		}(r)
	}
	timeout -= time.Since(start)

	for {
		for i, r := range c.rafts {
			if r.State() == raft.Leader {
				c.t.Logf("found leader %d", i)
				return r
			}
		}
		if timeout <= 0 {
			c.t.Fatalf("no leader was found")
		}
		timeout -= 25 * time.Millisecond
		time.Sleep(25 * time.Millisecond)
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

// WaitIndex waits until the FSM of the given raft instance reaches at least
// the given index.
//
// It fails the test if this does not happen withing the given timeout.
func (c *Control) WaitIndex(r *raft.Raft, index uint64, timeout time.Duration) {
	helper, ok := c.t.(testingHelper)
	if ok {
		helper.Helper()
	}

	i := c.Index(r)
	c.t.Logf("raft-test: node %d: wait for FSM to apply index %d", i, index)

	// First set up a hook for intercepting the logs.
	done := make(chan struct{})
	c.fsmsWatcher.ApplyHook(i, func(current uint64) {
		if current >= index {
			close(done)
		}
	})
	defer c.fsmsWatcher.ApplyHook(i, nil)

	// Secondly poll the wrapper to check if the FSM has reached the
	// desired apply count (this can happen if it startedt before we
	// could set up the hook).
	go func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			if current := c.fsmsWatcher.ApplyIndex(i); current >= index {
				c.t.Logf("raft-test: node %d: FSM already applied up to index %d", i, current)
				close(done)
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	// Wait for the hook to fire the channel
	select {
	case <-done:
		c.t.Logf("raft-test: node %d: FSM applied index %d", i, index)
		return
	case <-time.After(timeout):
		c.t.Fatalf("raft-test: node %d: FSM did not apply index %d within %s", i, index, timeout)
	}
}

// WaitSnapshot waits until the FSM of the given raft instance has performed at
// least the given number of snapshots
//
// It fails the test if this does not happen withing the given timeout.
func (c *Control) WaitSnapshot(r *raft.Raft, n int, timeout time.Duration) {
	helper, ok := c.t.(testingHelper)
	if ok {
		helper.Helper()
	}

	i := c.Index(r)
	c.t.Logf("raft-test: node %d: wait for FSM to perform snapshot %d", i, n)

	// First set up a hook for intercepting the snapshot.
	done := make(chan struct{})
	c.fsmsWatcher.SnapshotHook(i, func() {
		if c.fsmsWatcher.SnapshotCount(i) >= n {
			close(done)
		}
	})
	defer c.fsmsWatcher.SnapshotHook(i, nil)

	// Secondly poll the wrapper to check if the FSM has reached the
	// desired snapshot count (this can happen if it startedt before we
	// could set up the hook).
	go func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			if current := c.fsmsWatcher.SnapshotCount(i); current >= n {
				c.t.Logf("raft-test: node %d: FSM already performed %d snapshots", i, current)
				close(done)
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	// Wait for the hook to fire the channel
	select {
	case <-done:
		c.t.Logf("raft-test: node %d: FSM performed %d snapshots", i, n)
		return
	case <-time.After(timeout):
		c.t.Fatalf("raft-test: node %d: FSM did not perform snapshot %d within %s", i, n, timeout)
	}
}

// WaitRestore waits until the FSM of the given raft instance has restored at least
// the given number of snapshot.
//
// It fails the test if this does not happen withing the given timeout.
func (c *Control) WaitRestore(r *raft.Raft, n int, timeout time.Duration) {
	helper, ok := c.t.(testingHelper)
	if ok {
		helper.Helper()
	}

	i := c.Index(r)
	c.t.Logf("raft-test: node %d: wait for FSM to perform restore %d", i, n)

	// First set up a hook for intercepting the restore.
	done := make(chan struct{})
	c.fsmsWatcher.RestoreHook(i, func() {
		if c.fsmsWatcher.RestoreCount(i) >= n {
			close(done)
		}
	})
	defer c.fsmsWatcher.RestoreHook(i, nil)

	// Secondly poll the wrapper to check if the FSM has reached the
	// desired restore count (this can happen if it startedt before we
	// could set up the hook).
	go func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			if current := c.fsmsWatcher.RestoreCount(i); current >= n {
				c.t.Logf("raft-test: node %d: FSM already restored %d snapshots", i, current)
				close(done)
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	// Wait for the hook to fire the channel
	select {
	case <-done:
		c.t.Logf("raft-test: node %d: FSM restored %d snapshots", i, n)
		return
	case <-time.After(timeout):
		c.t.Fatalf("raft-test: node %d: FSM did not perform restore %d within %s", i, n, timeout)
	}
}

// Index returns the index of the given raft instance.
func (c *Control) Index(raft *raft.Raft) int {
	helper, ok := c.t.(testingHelper)
	if ok {
		helper.Helper()
	}

	for i := range c.rafts {
		if c.rafts[i] == raft {
			return i
		}
	}
	c.t.Fatalf("raft-test: unknown raft instance at %p", raft)
	return -1
}
