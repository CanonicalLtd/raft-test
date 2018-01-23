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
	"testing"
	"time"
)

// Poll the given function at the given internval, until it returns true, or
// the timeout expires.
func wait(t testing.TB, f func() bool, interval, timeout time.Duration, message string) {
	helper, ok := t.(testingHelper)
	if ok {
		helper.Helper()
	}

	timer := time.After(timeout)
	for {
		select {
		case <-timer:
			t.Fatalf("%s within %s", message, timeout)
		default:
		}
		if f() {
			return
		}
		time.Sleep(interval)
	}
}
