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
	"reflect"
)

// Receive leadershipChange notifications whenever the leadership status of a
// node in the cluster changes.
//
// It's essentially a "demultiplexer" of the Config.NotifyCh channels attached
// to the various nodes.
type notifyWatcher struct {
	cancel    context.CancelFunc
	ch        chan leadershipChange
	notifyChs []chan bool
}

func newNotifyWatcher(notifyChs []chan bool) *notifyWatcher {
	ctx, cancel := context.WithCancel(context.Background())

	watcher := &notifyWatcher{
		cancel:    cancel,
		ch:        make(chan leadershipChange),
		notifyChs: notifyChs,
	}

	go watcher.start(ctx)

	return watcher
}

// Close stops the goroutine that monitors the underlying notify channels.
func (w *notifyWatcher) Close() {
	w.cancel()
}

// Next returns the next leadershipChange happening in the cluster.
//
// If the context is done before a notification is received, nil is returned.
func (w *notifyWatcher) Next(ctx context.Context) *leadershipChange {
	select {
	case info := <-w.ch:
		return &info
	case <-ctx.Done():
		return nil
	}
}

// Start watching all the notify channels and feed notification to our
// "demultiplexer" channel
//
// The loop will be terminated once the context is done.
func (w *notifyWatcher) start(ctx context.Context) {
	n := len(w.notifyChs)
	cases := make([]reflect.SelectCase, n+1)

	// Create a select case for each of the notify channels we're watching.
	for i, ch := range w.notifyChs {
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ch),
		}
	}

	// Create a select case for the ctx.Done() channel.
	done := len(cases) - 1
	cases[done] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ctx.Done()),
	}

	// Loop until the context is done.
	for {
		i, value, ok := reflect.Select(cases)

		// If the done channel was selected, we quit.
		if i == done {
			if ok {
				panic("ctx.Done() channel was selected but it wasn't closed")
			}
			break
		}

		w.ch <- leadershipChange{On: i, Acquired: value.Bool()}
	}

	// Close all notify channels for good measure, so any attempt
	// from raft instances to send to them will fail.
	for _, ch := range w.notifyChs {
		close(ch)
	}
}

// leadershipChange includes information about a leadership change in a node.
type leadershipChange struct {
	On       int  // The index of the node whose leadership status changed.
	Acquired bool // Whether the leadership was acquired or lost.
}

func (c leadershipChange) Verb() string {
	if c.Acquired {
		return "acquired"
	}
	return "lost"
}
