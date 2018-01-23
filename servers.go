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

// Servers can be used to indicate which nodes should be initially part of the
// created cluster.
//
// If this knob is not used, the default is to have all nodes be part of the
// cluster.
func Servers(indexes ...int) Knob {
	return &serversKnob{indexes: indexes}
}

// serversKnob return a custom servers for a node.
type serversKnob struct {
	indexes []int
}

func (k *serversKnob) init(cluster *cluster) {
	for _, node := range cluster.nodes {
		node.Bootstrap = false
	}
	for _, index := range k.indexes {
		cluster.nodes[index].Bootstrap = true
	}
}
