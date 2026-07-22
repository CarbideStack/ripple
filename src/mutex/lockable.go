/*
 * Copyright (c) 2026 Daniel Bergløv
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package mutex

import "sync"

/**
 * Lockable describes a read-write lock that supports blocking and non-blocking
 * acquisition and can expose its shared-lock view as a sync.Locker.
 */
type Lockable interface {
	sync.Locker

	RLock()
	RUnlock()
	TryRLock() bool
	TryLock() bool

	RLocker() sync.Locker
}

/**
 * LockState embeds a reusable read-write mutex.
 *
 * Types can embed LockState to expose sync.RWMutex directly and to gain helper
 * methods for running closures while the lock is held.
 */
type LockState struct {
	/*
	 * RWMutex provides the underlying exclusive and shared lock operations.
	 */
	sync.RWMutex
}

var _ Lockable = (*LockState)(nil)

/**
 * Synchronized executes fn while holding obj's lock and returns its result.
 *
 * The lock is released after fn returns.
 *
 * @param obj
 *   Locker to acquire before running fn.
 *
 * @param fn
 *   Function to run while the lock is held.
 *
 * @return
 *   Value returned by fn.
 *
 */
func Synchronized[T sync.Locker](obj T, fn func()) {
	obj.Lock()
	defer obj.Unlock()

	fn()
}
