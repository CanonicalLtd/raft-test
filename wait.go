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
)

// Poll the given function at the given internval, until it returns true, or
// the given context expires.
func wait(ctx context.Context, t testing.TB, f func() bool, interval time.Duration, message string) {
	helper, ok := t.(testingHelper)
	if ok {
		helper.Helper()
	}

	start := time.Now()
	for {
		select {
		case <-ctx.Done():
			if err := ctx.Err(); err == context.Canceled {
				return
			}
			t.Fatalf("%s within %s", message, time.Since(start))
		default:
		}
		if f() {
			return
		}
		time.Sleep(interval)
	}
}

// Poll the given function at the given internval, until it returns true, or
// the given timeout expires.
func waitTimeout(timeout time.Duration, t testing.TB, f func() bool, interval time.Duration, message string) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	wait(ctx, t, f, interval, message)
}
