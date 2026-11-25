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

package ipc

import "time"

/**
 * Request packet kinds.
 */
const (
	/**
	 * KIND_PING is reserved for connection liveness checks.
	 */
	KIND_PING Kind = iota + 1

	/**
	 * KIND_DISPATCH sends an event to available handlers.
	 */
	KIND_DISPATCH

	/**
	 * KIND_RELOAD requests that the daemon rebuild its event registry.
	 */
	KIND_RELOAD

	/**
	 * KIND_LIST_TIMERS requests the daemon's built-in timer status.
	 */
	KIND_LIST_TIMERS

	/**
	 * KIND_LIST_EVENTS requests the currently registered event names.
	 */
	KIND_LIST_EVENTS

	/**
	 * KIND_LIST_HANDLERS requests the handlers registered for events.
	 */
	KIND_LIST_HANDLERS
)

/**
 * Reply packet kinds.
 *
 * Reply kinds include FLAG_KIND_REPLY and are correlated with the request ID
 * of the packet they answer.
 */
const (
	/**
	 * KIND_REPLY_TIMEOUT indicates that the request timed out.
	 */
	KIND_REPLY_TIMEOUT Kind = FLAG_KIND_REPLY | (iota + 150)

	/**
	 * KIND_REPLY_DENIED indicates that the dispatcher rejected the connection
	 * or request because of permissions.
	 */
	KIND_REPLY_DENIED

	/**
	 * KIND_REPLY_INVALID indicates that the request kind or payload was not
	 * valid for the receiver.
	 */
	KIND_REPLY_INVALID

	/**
	 * KIND_REPLY_FAILED indicates that a valid request completed unsuccessfully.
	 */
	KIND_REPLY_FAILED

	/**
	 * KIND_REPLY_ERROR indicates that request handling failed with an error.
	 */
	KIND_REPLY_ERROR
)

/**
 * special KIND_REPLY that all indicate a successful reply
 */
const (
	/**
	 * KIND_REPLY_OK indicates a successful request.
	 */
	KIND_REPLY_OK Kind = FLAG_KIND_REPLY_OK | (iota + 100)

	/**
	 * KIND_REPLY_READY is sent by the dispatcher after a client has connected
	 * and passed authorization.
	 */
	KIND_REPLY_READY
)

/**
 * EmitRequest is the payload used to request event dispatch from the daemon.
 */
type EmitRequest struct {
	Event   string        `json:"event"`
	Args    []string      `json:"args,omitempty"`
	Wait    bool          `json:"wait"`
	Timeout time.Duration `json:"timeout"`
}

func (req *EmitRequest) Validate() bool {
	return req.Event != ""
}

/**
 *
 */
type EventRequest struct {
	Event string `json:"event"`
}

func (req *EventRequest) Validate() bool {
	return req.Event != ""
}
