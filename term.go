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
	"github.com/CanonicalLtd/raft-test/internal/election"
	"github.com/CanonicalLtd/raft-test/internal/event"
	"github.com/hashicorp/raft"
)

// A Term holds information about an event that should happen while a certain
// node is the leader.
type Term struct {
	control    *Control
	id         raft.ServerID
	leadership *election.Leadership
	events     []*Event
}

// When can be used to schedule a certain action when a certain expected
// event occurs in the cluster during this Term.
func (t *Term) When() *Event {
	// TODO: check that we're not using Connect()
	t.control.t.Helper()

	event := &Event{
		control:    t.control,
		id:         t.id,
		leadership: t.leadership,
	}

	t.events = append(t.events, event)
	return event
}

// ConnectAllServers connect all servers to each other after election.
//
// This will prevent using When().
func (t *Term) ConnectAllServers() {
	// TODO: check that we're not using When()
	//t.control.network.
}

// Event that is expected to happen during a Term.
type Event struct {
	control     *Control
	id          raft.ServerID
	leadership  *election.Leadership
	isScheduled bool
}

// Command schedules the event to occur when the Raft.Apply() method is called
// on the leader raft instance in order to apply the n'th command log during
// the current term.
func (e *Event) Command(n uint64) *Dispatch {
	e.control.t.Helper()

	if e.isScheduled {
		e.control.t.Fatal("raft-test: error: term event already scheduled")
	}
	e.isScheduled = true

	return &Dispatch{
		control:    e.control,
		id:         e.id,
		leadership: e.leadership,
		n:          n,
	}
}

// Dispatch defines at which phase of the dispatch process a command log event
// should fire.
type Dispatch struct {
	control    *Control
	id         raft.ServerID
	leadership *election.Leadership
	n          uint64
	event      *event.Event
}

// Enqueued configures the command log event to occurr when the command log is
// enqueued, but not yet appended by the followers.
func (d *Dispatch) Enqueued() *Action {
	d.control.t.Helper()

	if d.event != nil {
		d.control.t.Fatal("raft-test: error: dispatch event already defined")
	}
	d.event = d.control.whenCommandEnqueued(d.id, d.n)

	return &Action{
		control:    d.control,
		id:         d.id,
		leadership: d.leadership,
		event:      d.event,
	}
}

// Appended configures the command log event to occurr when the command log is
// appended by all followers, but not yet committed by the leader.
func (d *Dispatch) Appended() *Action {
	d.control.t.Helper()

	if d.event != nil {
		d.control.t.Fatal("raft-test: error: dispatch event already defined")
	}

	d.event = d.control.whenCommandAppended(d.id, d.n)

	return &Action{
		control:    d.control,
		id:         d.id,
		leadership: d.leadership,
		event:      d.event,
	}
}

// Committed configures the command log event to occurr when the command log is
// committed.
func (d *Dispatch) Committed() *Action {
	d.control.t.Helper()

	if d.event != nil {
		d.control.t.Fatal("raft-test: error: dispatch event already defined")
	}

	d.event = d.control.whenCommandCommitted(d.id, d.n)

	return &Action{
		control:    d.control,
		id:         d.id,
		leadership: d.leadership,
		event:      d.event,
	}
}

// Action defines what should happen when the event defined in the term occurs.
type Action struct {
	control    *Control
	id         raft.ServerID
	leadership *election.Leadership
	event      *event.Event
}

// Depose makes the action depose the current leader.
func (a *Action) Depose() {
	a.control.t.Helper()
	//a.control.t.Logf(
	//"raft-test: event: schedule depose server %s when command %d gets %s", a.id, a.n, a.phase)

	a.control.deposing = make(chan struct{})

	go func() {
		//c.t.Logf("raft-test: node %d: fsm: wait log command %d", i, n)
		a.control.deposeUponEvent(a.event, a.id, a.leadership)
	}()
}

// Snapshot makes the action trigger a snapshot on the leader.
//
// The typical use is to take the snapshot after a certain command log gets
// committed (see Dispatch.Committed()).
func (a *Action) Snapshot() {
	a.control.t.Helper()
	// a.control.t.Logf(
	// 	"raft-test: event: schedule snapshot server %s when command %d gets %s", a.id, a.n, a.phase)

	go func() {
		//c.t.Logf("raft-test: node %d: fsm: wait log command %d", i, n)
		a.control.snapshotUponEvent(a.event, a.id)
	}()
}

// Disconnect makes the action disconnect a follower, which will stop
// receiving RPCs.
func (a *Action) Disconnect(id raft.ServerID) {
	go func() {
		//c.t.Logf("raft-test: node %d: fsm: wait log command %d", i, n)
		a.control.disconnectUponEvent(a.event, a.id, id)
	}()
}

// Reconnect makes the action reconnect a previously disconnected
// follower, which will start receiving RPCs again.
func (a *Action) Reconnect(id raft.ServerID) {
	go func() {
		//c.t.Logf("raft-test: node %d: fsm: wait log command %d", i, n)
		a.control.reconnectUponEvent(a.event, a.id, id)
	}()
}
