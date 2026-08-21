// Copyright 2019 Edgard Castro
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"sync"
)

// targetLocks serializes iperf3 probes against the same target:port.
// iperf3's server only runs one test at a time and rejects a second
// concurrent client with "the server is busy running a test" — which
// happens routinely here because Prometheus scrapes download and upload as
// two independent jobs against the same server. Acquiring this lock before
// running iperf3 makes the second probe wait its turn instead of failing.
type targetLocks struct {
	mu    sync.Mutex
	locks map[string]chan struct{}
}

func newTargetLocks() *targetLocks {
	return &targetLocks{locks: make(map[string]chan struct{})}
}

// Acquire blocks until the lock for key is free or ctx is done, whichever
// happens first. On a nil return the caller must call Release(key) exactly
// once when done; on a non-nil (context) error, no lock was taken and
// Release must not be called.
func (l *targetLocks) Acquire(ctx context.Context, key string) error {
	l.mu.Lock()

	ch, ok := l.locks[key]
	if !ok {
		ch = make(chan struct{}, 1)
		l.locks[key] = ch
	}

	l.mu.Unlock()

	select {
	case ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release frees the lock for key, previously taken by a successful Acquire.
func (l *targetLocks) Release(key string) {
	l.mu.Lock()
	ch := l.locks[key]
	l.mu.Unlock()

	<-ch
}
