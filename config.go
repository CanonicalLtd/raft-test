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
	"log"
	"time"

	"github.com/hashicorp/raft"
)

// Config sets a hook for tweaking the raft configuration of individual nodes.
func Config(f func(int, *raft.Config)) Knob {
	return func(nodes map[int]*node) {
		for i, node := range nodes {
			f(i, node.Config)
		}
	}
}

// Latency is a convenience around Config that scales the values of the various
// raft timeouts that would be set by default by Cluster.
func Latency(factor float64) Knob {
	return Config(func(i int, config *raft.Config) {
		timeouts := []*time.Duration{
			&config.HeartbeatTimeout,
			&config.ElectionTimeout,
			&config.LeaderLeaseTimeout,
			&config.CommitTimeout,
		}
		for _, timeout := range timeouts {
			*timeout = scaleDuration(*timeout, factor)
		}
	})
}

// DiscardLogger is a convenience around Config that sets the output stream of
// raft's logger to ioutil.Discard
func DiscardLogger() Knob {
	return Config(func(i int, config *raft.Config) {
		config.Logger = log.New(ioutil.Discard, "", 0)
	})
}
