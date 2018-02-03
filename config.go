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
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/hashicorp/raft"
)

// Config sets a hook for tweaking the raft configuration of individual nodes.
func Config(f func(int, *raft.Config)) Knob {
	return &configKnob{
		f: f,
	}
}

// configKnob gives access to the Config objects used by the various nodes.
type configKnob struct {
	f func(int, *raft.Config)
}

func (k *configKnob) pre(cluster *cluster) {
	for i, node := range cluster.nodes {
		k.f(i, node.Config)
	}
}

func (k *configKnob) post([]*raft.Raft) {
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

// Duration is a convenience to scale the given duration according to the
// GO_RAFT_TEST_LATENCY environment variable.
func Duration(duration time.Duration) time.Duration {
	factor := 1.0
	if env := os.Getenv("GO_RAFT_TEST_LATENCY"); env != "" {
		var err error
		factor, err = strconv.ParseFloat(env, 64)
		if err != nil {
			panic(fmt.Sprintf("invalid value '%s' for GO_RAFT_TEST_LATENCY", env))
		}
	}
	return scaleDuration(duration, factor)
}
