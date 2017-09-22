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
		ch: make(chan LeadershipChange),
	}
}

// NotifyKnob can be used for receiving LeadershipChange notifications
// whenever the leadership status of a node in the cluster changes.
type NotifyKnob struct {
	t         *testing.T
	ch        chan LeadershipChange
	notifyChs []chan bool
}

// LeadershipChange includes information about a leadership change in a node.
type LeadershipChange struct {
	On       int  // The index of the node whose leadership status changed.
	Acquired bool // Whether the leadership was acquired or lost.
}

// Next blocks until there's a leadership change in any node of the cluster,
// and then returns a LeadershipChange object with the relevant information.
//
// It fails the test if no LeadershipChange is received within the given timeout.
func (k *NotifyKnob) Next(timeout time.Duration) (info LeadershipChange) {
	select {
	case info = <-k.ch:
		return
	case <-time.After(timeout):
		k.t.Fatalf("no notification received within %s", timeout)
		return
	}
}

// NextAcquired blocks until this channel receives a LeadershipChange object whose
// Acquired attribute is true, and then returns its Node attribute.
//
// All LeadershipChange objects received whose Acquired attribute is set to
// false will be discarded.
//
// It fails the test if no matching LeadershipChange is received within the
// timeout.
func (k *NotifyKnob) NextAcquired(timeout time.Duration) int {
	return k.nextMatching(timeout, true)
}

// NextLost blocks until this channel receives a LeadershipChange object whose
// Acquired attribute is false, and then returns its Node attribute.
//
// All LeadershipChange objects received whose Acquired attribute is set to
// true will be discarded.
//
// It fails the test if no matching LeadershipChange is received within the
// timeout.
func (k *NotifyKnob) NextLost(timeout time.Duration) int {
	return k.nextMatching(timeout, false)
}

// Return the next LeadershipChange received matching 'acquired'.
func (k *NotifyKnob) nextMatching(timeout time.Duration, acquired bool) int {
	for {
		start := time.Now()
		info := k.Next(timeout)
		if info.Acquired == acquired {
			return info.On
		}
		timeout -= time.Since(start)
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

func (k *NotifyKnob) cleanup(cluster *cluster) {
	for _, notifyCh := range k.notifyChs {
		close(notifyCh)
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
		k.ch <- LeadershipChange{On: i, Acquired: value.Bool()}
	}
}
