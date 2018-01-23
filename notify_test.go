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

package rafttest_test

import (
	"sync"
	"testing"
	"time"

	"github.com/CanonicalLtd/raft-test"
	"github.com/stretchr/testify/assert"
)

func TestNotify_NextAcquired(t *testing.T) {
	notify := rafttest.Notify()

	_, cleanup := rafttest.Cluster(t, rafttest.FSMs(3), notify)
	defer cleanup()

	i := notify.NextAcquired(time.Second)
	assert.Contains(t, []int{0, 1, 2}, i)
}

// The test associated with the given testing.T object fails if a notification
// is not received within the given timeout.
func TestNotify_NextAcquiredTimeout(t *testing.T) {
	notify := rafttest.Notify()

	_, cleanup := rafttest.Cluster(&testing.T{}, rafttest.FSMs(3), notify)
	defer cleanup()

	succeeded := false

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		notify.NextAcquired(time.Microsecond)
		succeeded = true
	}()
	wg.Wait()

	assert.False(t, succeeded)
}

// If a node is shutdown, no more notification gets received from it
func TestNotify_Shutdown(t *testing.T) {
	notify := rafttest.Notify()

	rafts, cleanup := rafttest.Cluster(t, rafttest.FSMs(3), notify)
	defer cleanup()

	for i := 0; i < 2; i++ {
		n := notify.NextAcquired(time.Second)
		assert.NoError(t, rafts[n].Shutdown().Error())
	}
}
