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
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

// A Scenario is a function that takes a Control for a cluster of 3 raft nodes
// and runs a sequence of scenario-specific stages, performing particular
// actions on the cluster before and after each stage.
type Scenario func(*Control, ...ScenarioStage)

// A ScenarioStage runs user-defined code at particular moments during the
// execution of a Scenario.
type ScenarioStage func(*raft.Raft)

// ReplicationScenario is used to test the "happy path" of the user's FSMs.
//
// It expects a slice containing three user stages and proceeds as follows:
//
//  - Wait for a raft instance to acquire leadership.
//
//  - Invoke the first stage, passing it the newly elected raft instance.
//
//  - Wait for the FSM of one of the two follower raft instances to catch up
//    with the last applied index of the leader.
//
//  - Invoke the second stage, passing it the above follower.
//
//  - Wait for the FSM of the other follower raft instance to catch up with
//    the last applied index of the leader.
//
//  - Invoke the third stage, passing it the above follower.
func ReplicationScenario() Scenario {
	return func(control *Control, stages ...ScenarioStage) {
		control.t.Helper()

		scenarioAssertPreconditions(control, "ReplicationScenario", 3, stages)

		r1 := control.LeadershipAcquired(time.Second)
		stages[0](r1)

		r2 := control.Other(r1)
		control.WaitIndex(r2, r1.AppliedIndex(), time.Second)
		stages[1](r2)

		r3 := control.Other(r1)
		control.WaitIndex(r3, r1.AppliedIndex(), time.Second)
		stages[2](r3)
	}
}

// NotLeaderScenario is used to test what happens when user code runs
// Raft.Apply() against a leader that got deposed.
//
// It expects a slice containing five user stages and proceeds as follows:
//
//  - Wait for a raft instance to acquire leadership.
//
//  - Invoke the first stage, passing it the newly elected raft instance.
//
//  - Disconnect the leader instance and wait for it to lose leadership.
//
//  - Invoke the second stage passing it the raft instance that lost leadership.
//
//  - Wait for one of the two other raft instances to become leader and catch up
//    with logs.
//
//  - Invoke the third stage passing it the new leader.
//
//  - Reconnect the raft instance that had lost leadership.
//
//  - Wait for the FSM of the reconnected raft instance to catch up with
//    the last applied index of the new leader.
//
//  - Invoke the fourth stage passing it the above raft instance.
//
//  - Wait for the FSM of the raft instance that has been always a follower to
//    catch up with the last applied index of the new leader.
//
//  - Invoke the fifth stage passing it the above raft instance.
func NotLeaderScenario(index uint64) Scenario {
	return func(control *Control, stages ...ScenarioStage) {
		control.t.Helper()

		scenarioAssertPreconditions(control, "NotLeaderScenario", 5, stages)

		r1 := control.LeadershipAcquired(time.Second)
		stages[0](r1)

		if index > 2 {
			control.WaitIndex(r1, index, time.Second)
		}

		control.Disconnect(r1)
		control.LeadershipLost(r1, time.Second)
		stages[1](r1)

		r2 := control.LeadershipAcquired(time.Second)
		if i := r1.AppliedIndex(); i > 2 {
			control.WaitIndex(r2, i, time.Second)
		}
		stages[2](r2)
		i := control.AppliedIndex(r2)

		control.Reconnect(r1)
		control.WaitIndex(r1, i, time.Second)
		stages[3](r1)

		r3 := control.Other(r1, r2)
		control.WaitIndex(r3, i, time.Second)
		stages[4](r3)
	}
}

// LeadershipLostScenario is used to test what happens when user code runs
// Raft.Apply() against a leader which has just been disconnected and hence
// will lose leadership while applying a command. The command won't be received
// by the followers and no quorum will be reached, so it won't be applied by
// any FSM.
//
// It expects a slice containing five user stages and proceeds as follows:
//
//  - Wait for a raft instance to acquire leadership.
//
//  - Invoke the first stage, passing it the newly elected raft instance.
//
//  - Disconnect the leader instance.
//
//  - Invoke the second stage immediately passing it the leader just
//    disconnected. The state of the raft instance is still leader, since the
//    timeouts haven't expired yet, but a Raft.Apply() call will fail with
//    ErrLeadershipLost.
//
//  - Assert that the disconnected leader has actually lost leadership, then wait
//    for a follower to be elected as new leader and for it to catch up with logs.
//
//  - Invoke the third stage passing it the new leader.
//
//  - Reconnect the raft instance that had lost leadership.
//
//  - Wait for the FSM of the reconnected raft instance to catch up with
//    the last applied index of the new leader.
//
//  - Invoke the fourth stage passing it the above raft instance.
//
//  - Wait for the FSM of the raft instance that has been always a follower to
//    catch up with the last applied index of the new leader.
//
//  - Invoke the fifth stage passing it the above raft instance.
func LeadershipLostScenario() Scenario {
	return func(control *Control, stages ...ScenarioStage) {
		control.t.Helper()

		scenarioAssertPreconditions(control, "LeadershipLostScenario", 5, stages)

		r1 := control.LeadershipAcquired(time.Second)
		stages[0](r1)

		control.Disconnect(r1)
		stages[1](r1)

		control.LeadershipLost(r1, time.Second)
		r2 := control.LeadershipAcquired(time.Second)
		if i := r1.AppliedIndex(); i > 2 {
			control.WaitIndex(r2, i, time.Second)
		}
		stages[2](r2)
		i := control.AppliedIndex(r2)

		control.Reconnect(r1)
		control.WaitIndex(r1, i, time.Second)
		stages[3](r1)

		r3 := control.Other(r1, r2)
		control.WaitIndex(r3, i, time.Second)
		stages[4](r3)
	}
}

// LeadershipLostQuorumSameLeaderScenario is used to test what happens when user
// code runs Raft.Apply() against a leader which loses leadership while the
// command it's applying is inflight and a timeout hits before it gets notified
// by the followers. The command will still be received by the followers node
// and a quorum will be reached, so all FSMs will eventually apply it, despite
// the associated Raft.Apply() call failed with ErrLeadershipLost.
//
// This factory takes a log index as argument, which is the index of the log
// that should have the leader timeout when applying it but the followers still
// receive it.
//
// It expects a slice containing five user stages and proceeds as follows:
//
//  - Wait for a raft instance to acquire leadership.
//
//  - Invoke the first stage, passing it the newly elected raft instance.
//
//  - Configure the log stores to be slow at saving log with the given index,
//    causing the leader to timeout and lose leadership. The stores will still
//    save the log they received, so a quorum for that log is reached.
//
//  - Invoke the second stage passing it the leader. The first call to
//    Raft.Apply() sends a log with the given special index will fail with
//    ErrLeadershipLost, however the log will still be delivered and a quorum
//    will be reached.
//
//  - Assert that the leader actually loses leadership, then wait for a
//    follower to be elected as new leader and for it to catch up with logs.
//
//  - Invoke the third stage passing it the new leader.
//
//  - Reconnect the raft instance that had lost leadership.
//
//  - Wait for the FSM of the reconnected raft instance to catch up with
//    the last applied index of the new leader.
//
//  - Invoke the fourth stage passing it the above raft instance.
//
//  - Wait for the FSM of the raft instance that has been always a follower to
//    catch up with the last applied index of the new leader.
//
//  - Invoke the fifth stage passing it the above raft instance.
func LeadershipLostQuorumSameLeaderScenario(index uint64) Scenario {
	return func(control *Control, stages ...ScenarioStage) {
		control.t.Helper()

		scenarioAssertPreconditions(control, "LeadershipLostQuorumSameLeaderScenario", 5, stages)

		r1 := control.LeadershipAcquired(time.Second)
		stages[0](r1)

		slowdown := func() {
			// Sleep for longer than heartbeat timeout
			time.Sleep(Duration(40 * time.Millisecond))
		}

		r2 := control.Other(r1)
		r3 := control.Other(r1, r2)
		control.BeforeStoreLog(r2, index, slowdown)
		control.BeforeStoreLog(r3, index, slowdown)
		stages[1](r1)

		control.LeadershipLost(r1, time.Second)

		// The new leader applied the entry that the old leader
		// appended despite losing leadership inbetween. The new leader
		// happens to be the same r1.
		r := control.LeadershipAcquired(time.Second)
		require.Equal(control.t, r1, r)
		control.WaitIndex(r1, index, time.Second)
		stages[2](r1)
		i := control.AppliedIndex(r1)

		control.WaitIndex(r2, i, time.Second)
		stages[3](r2)

		control.WaitIndex(r3, i, time.Second)
		stages[4](r3)
	}
}

func LeadershipLostQuorumOtherLeaderScenario(index uint64) Scenario {
	return func(control *Control, stages ...ScenarioStage) {
		control.t.Helper()

		scenarioAssertPreconditions(control, "LeadershipLostQuorumOtherLeaderScenario", 5, stages)

		r1 := control.LeadershipAcquired(time.Second)
		stages[0](r1)

		slowdown := func() {
			// Sleep for longer than heartbeat timeout
			time.Sleep(Duration(40 * time.Millisecond))
		}

		r2 := control.Other(r1)
		r3 := control.Other(r1, r2)
		control.BeforeStoreLog(r2, index, slowdown)
		control.BeforeStoreLog(r3, index, slowdown)
		stages[1](r1)

		control.LeadershipLost(r1, time.Second)
		control.Disconnect(r1)

		// The new leader applied the entry that the old leader
		// appended despite losing leadership inbetween. The new leader
		// happens to be the same r1.
		r2 = control.LeadershipAcquired(time.Second)
		require.NotEqual(control.t, r1, r2)
		control.WaitIndex(r2, index, time.Second)
		stages[2](r2)
		i := control.AppliedIndex(r2)

		control.Reconnect(r1)
		control.WaitIndex(r1, i, time.Second)
		stages[3](r1)

		r3 = control.Other(r1, r2)
		control.WaitIndex(r3, i, time.Second)
		stages[4](r3)
	}
}

func scenarioAssertPreconditions(control *Control, scenario string, n int, stages []ScenarioStage) {
	control.t.Helper()

	if len(control.rafts) != 3 {
		control.t.Fatalf("%s requires 3 raft instances, but %d were given", scenario, len(control.rafts))
	}
	if len(stages) != n {
		control.t.Fatalf("%s requires %d stages, but %d were given", scenario, n, len(stages))
	}
}
