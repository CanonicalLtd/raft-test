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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNotifyWatcher_NextAcquired(t *testing.T) {
	notifyChs := newNotifyChs()

	watcher := newNotifyWatcher(notifyChs)
	defer watcher.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	go func() {
		notifyChs[1] <- true
	}()

	change := watcher.Next(ctx)
	assert.NotNil(t, change)
	assert.Equal(t, 1, change.On)
	assert.True(t, change.Acquired)
	assert.Equal(t, "acquired", change.Verb())
}

func TestNotifyWatcher_NextLost(t *testing.T) {
	notifyChs := newNotifyChs()

	watcher := newNotifyWatcher(notifyChs)
	defer watcher.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	go func() {
		notifyChs[2] <- false
	}()

	change := watcher.Next(ctx)
	assert.NotNil(t, change)
	assert.Equal(t, 2, change.On)
	assert.False(t, change.Acquired)
	assert.Equal(t, "lost", change.Verb())
}

func TestNotifyWatcher_NextNoNotification(t *testing.T) {
	notifyChs := newNotifyChs()

	watcher := newNotifyWatcher(notifyChs)
	defer watcher.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assert.Nil(t, watcher.Next(ctx))
}

func newNotifyChs() []chan bool {
	notifyChs := make([]chan bool, 3)
	for i := range notifyChs {
		notifyChs[i] = make(chan bool)
	}
	return notifyChs
}
