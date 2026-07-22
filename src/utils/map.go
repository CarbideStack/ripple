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

/*
 * Implements a generic synchronized map for shared keyed state.
 */

package utils

import (
	"maps"
	"ripple/mutex"
)

/**
 * Store is a synchronized generic map.
 *
 * Store embeds Lockable and exposes Items for callers that need to perform
 * compound operations while holding an explicit lock.
 *
 * @param K
 *   Comparable key type.
 *
 * @param V
 *   Stored value type.
 */
type Map[K comparable, V any] struct {
	mutex.LockState

	/*
	 * Items contains the stored key/value pairs.
	 *
	 * Direct access must be protected by the Store lock unless the caller can
	 * otherwise guarantee exclusive access.
	 */
	items map[K]V
}

/**
 * NewMap creates an initialized map.
 *
 * @param K
 *   Comparable key type.
 *
 * @param V
 *   Stored value type.
 */
func NewMap[K comparable, V any]() *Map[K, V] {
	return &Map[K, V]{
		items: make(map[K]V),
	}
}

/**
 * Length returns the number of entries currently stored in the map.
 *
 * Callers must hold an appropriate lock when concurrent access is possible.
 */
func (m *Map[K, V]) Length() int {
	return len(m.items)
}

/**
 * Clear removes all items from the map.
 */
func (m *Map[K, V]) Clear() {
	m.items = make(map[K]V)
}

/**
 * Has reports whether key exists in the map.
 *
 * Callers must hold an appropriate lock when concurrent access is possible.
 */
func (m *Map[K, V]) Has(key K) bool {
	_, exists := m.items[key]

	return exists
}

/**
 * AddAll inserts or replaces all items from the passed map.
 *
 * @param other
 *   Map containing items to insert.
 */
func (m *Map[K, V]) AddAll(other map[K]V) {
	maps.Copy(m.items, other)
}

/**
 * Returns the entire backing map
 */
func (m *Map[K, V]) GetAll() map[K]V {
	return m.items
}

/**
 * Add inserts or replaces one item.
 *
 * @param key
 *   Item key.
 *
 * @param value
 *   Value to store under key.
 */
func (m *Map[K, V]) Add(key K, value V) {
	m.items[key] = value
}

/**
 * Get returns one item from the map.
 *
 * @param key
 *   Item key to look up.
 *
 * @return
 *   Stored value, or the zero value for V when absent.
 *
 * @return
 *   True when key exists.
 */
func (m *Map[K, V]) Get(key K) (V, bool) {
	value, ok := m.items[key]
	return value, ok
}

/**
 * Remove deletes one item from the map.
 *
 * @param key
 *   Item key to remove.
 *
 * @return
 *   Removed value, or the zero value for V when absent.
 *
 * @return
 *   True when key existed and was removed.
 */
func (m *Map[K, V]) Remove(key K) (V, bool) {
	value, ok := m.items[key]

	if ok {
		delete(m.items, key)
	}

	return value, ok
}

/**
 * RemoveIf deletes every item for which fn returns true.
 *
 * @param fn
 *   Predicate called with each key/value pair. Returning true removes the item.
 */
func (m *Map[K, V]) RemoveIf(fn func(K, V) bool) {
	var keys []K

	for key, value := range m.items {
		if fn(key, value) {
			keys = append(keys, key)
		}
	}

	for _, key := range keys {
		delete(m.items, key)
	}
}

/**
 * Range iterates over map items until fn returns true or iteration ends.
 *
 * @param fn
 *   Callback called with each key/value pair. Returning false stops iteration.
 */
func (m *Map[K, V]) Range(fn func(K, V) bool) {
	for key, value := range m.items {
		if !fn(key, value) {
			break
		}
	}
}
