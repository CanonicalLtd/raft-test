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
	"github.com/hashicorp/raft"
)

// Transport can be used to create custom transports.
//
// The given function takes a node index as argument and returns the Transport
// that the node should use.
//
// If the transports returned by the factory do not implement
// LoopbackTransport, the Disconnect API won't work.
func Transport(factory func(int) raft.Transport) Knob {
	return func(nodes []*node) {
		for i, node := range nodes {
			node.Transport = factory(i)
		}
	}
}
