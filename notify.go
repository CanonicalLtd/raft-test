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
	"reflect"
	"testing"
	"time"
)

// Notify exposes APIs to block until a node of the cluster acquires or loses
// leadership.
func Notify() *NotifyKnob {
	return &NotifyKnob{
		ch: make(chan leadershipChange),
	}
}

// NotifyKnob can be used for receiving leadershipChange notifications
// whenever the leadership status of a node in the cluster changes.
type NotifyKnob struct {
	t         testing.TB
	ch        chan leadershipChange
	notifyChs []chan bool
}

// NextAcquired blocks until this channel receives a leadershipChange object whose
// Acquired attribute is true, and then returns its Node attribute.
//
// All leadershipChange objects received whose Acquired attribute is set to
// false will be discarded.
//
// It fails the test if no matching leadershipChange is received within the
// timeout.
func (k *NotifyKnob) NextAcquired(timeout time.Duration) int {
	helper, ok := k.t.(testingHelper)
	if ok {
		helper.Helper()
	}

	return k.nextMatching(timeout, true)
}

// NextLost blocks until this channel receives a leadershipChange object whose
// Acquired attribute is false, and then returns its Node attribute.
//
// All leadershipChange objects received whose Acquired attribute is set to
// true will be discarded.
//
// It fails the test if no matching leadershipChange is received within the
// timeout.
func (k *NotifyKnob) NextLost(timeout time.Duration) int {
	helper, ok := k.t.(testingHelper)
	if ok {
		helper.Helper()
	}

	return k.nextMatching(timeout, false)
}

// Return the next leadershipChange received matching 'acquired'.
func (k *NotifyKnob) nextMatching(timeout time.Duration, acquired bool) int {
	helper, ok := k.t.(testingHelper)
	if ok {
		helper.Helper()
	}

	for {
		start := time.Now()
		info := k.next(timeout)
		if info.Acquired != acquired {
			timeout -= time.Since(start)
			continue
		}
		verb := ""
		if acquired {
			verb = "acquired"
		} else {
			verb = "lost"
		}
		k.t.Logf("node %d %s leadership", info.On, verb)
		return info.On
	}
}

func (k *NotifyKnob) init(cluster *cluster) {
	k.t = cluster.t

	// Use a large pool, so raft won't block on us and tests can proceed
	// asynchronously.
	k.notifyChs = make([]chan bool, len(cluster.nodes), 1000)

	for i, node := range cluster.nodes {
		notifyCh := make(chan bool)
		node.Config.NotifyCh = notifyCh
		k.notifyChs[i] = notifyCh
	}
	go k.watch()
}

// Block until there's a leadership change in any node of the cluster, and then
// returns a leadershipChange object with the relevant information.
//
// It fails the test if no leadershipChange is received within the given timeout.
func (k *NotifyKnob) next(timeout time.Duration) (info leadershipChange) {
	select {
	case info = <-k.ch:
		return
	case <-time.After(timeout):
		k.t.Fatalf("no notification received within %s", timeout)
		return
	}
}

func (k *NotifyKnob) watch() {
	n := len(k.notifyChs)
	cases := make([]reflect.SelectCase, n)

	for i, notifyCh := range k.notifyChs {
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(notifyCh),
		}
	}

	// Loop until all nodes have shutdown and closed their
	// notifyCh.
	for len(cases) > 1 {
		i, value, ok := reflect.Select(cases)
		if !ok {
			// Remove from the select cases the notify
			// channels that have been closed, since that
			// means the node was shutdown.
			cases = append(cases[:i], cases[i+1:]...)
		}
		k.ch <- leadershipChange{On: i, Acquired: value.Bool()}
	}
}

// leadershipChange includes information about a leadership change in a node.
type leadershipChange struct {
	On       int  // The index of the node whose leadership status changed.
	Acquired bool // Whether the leadership was acquired or lost.
}
