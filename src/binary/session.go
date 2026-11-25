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

package main

import (
	"fmt"
	"ripple/events"
	"ripple/ipc"
	"ripple/triggers"
	"ripple/utils"
)

/**
 * ClientSession executes one client command over an authenticated Ripple
 * connection.
 *
 * Status reports a successful daemon connection. Emit serializes an event
 * request, waits for its reply, and translates failure replies into event
 * execution errors.
 *
 * @param socket
 *   Authenticated connection to the Ripple daemon.
 *
 * @param opts
 *   Parsed client command, arguments, and wait preference.
 *
 * @return
 *   Any event failure, timeout, or invalid reply error.
 */
func ClientSession(
	socket ipc.Socket,
	opts *Options,
) error {
	switch opts.Command {
	case "status":
		utils.Println("dispatch daemon is running")

	case "reload":
		packet := socket.NewPacket(ipc.KIND_RELOAD)

		var err error

		if err = socket.Send(packet); err != nil {
			return err

		} else if packet, err = socket.Receive(); err != nil {
			return err

		} else if !packet.IsKindOK() {
			return fmt.Errorf("failed to reload")
		}

		utils.Println("reload successful")

	case "emit":
		packet := socket.NewPacket(ipc.KIND_DISPATCH)
		err := packet.AttachPayload(&ipc.EmitRequest{
			Event:   opts.Args[0],
			Args:    opts.Args[1:],
			Wait:    opts.Wait,
			Timeout: opts.Timeout,
		})

		if err != nil {
			return err

		} else if err = socket.Send(packet); err != nil {
			return err

		} else if packet, err = socket.Receive(); err != nil {
			return err
		}

		if packet.IsKindOK() {
			return nil

		} else if packet.IsKind(ipc.KIND_REPLY_FAILED) {
			return fmt.Errorf("%w: one or more scripts terminated with error", events.ErrExitError)

		} else if packet.IsKind(ipc.KIND_REPLY_TIMEOUT) {
			return fmt.Errorf("%w: one or more handlers exceeded their execution timeout", events.ErrTimeout)
		}

		return fmt.Errorf("failed to emit event")

	case "list-events":
		packet := socket.NewPacket(ipc.KIND_LIST_EVENTS)

		var err error
		var list []string

		if err = socket.Send(packet); err != nil {
			return err

		} else if packet, err = socket.Receive(); err != nil {
			return err

		} else if !packet.IsKindOK() {
			return fmt.Errorf("failed to obtain the events list")

		} else if err = packet.ExtractPayload(&list); err != nil {
			return err
		}

		utils.Println("EVENT")
		utils.Println("-----")

		if len(list) == 0 {
			utils.Println("no events")

		} else {
			for _, event := range list {
				utils.Println(event)
			}
		}

	case "list-handlers":
		packet := socket.NewPacket(ipc.KIND_LIST_HANDLERS)
		err := packet.AttachPayload(&ipc.EventRequest{
			Event: opts.Args[0],
		})

		var list []*events.EventHandler

		if err != nil {
			return err

		} else if err = socket.Send(packet); err != nil {
			return err

		} else if packet, err = socket.Receive(); err != nil {
			return err

		} else if !packet.IsKindOK() {
			return fmt.Errorf("failed to obtain the handler list")

		} else if err = packet.ExtractPayload(&list); err != nil {
			return err
		}

		format := "%-24s %-12s %s"
		utils.Println(format, "NAME", "Detached", "Path")
		utils.Println(format, "----", "--------", "----")

		if len(list) == 0 {
			utils.Println("no handlers")

		} else {
			for _, handler := range list {
				detached := "no"

				if handler.Detached {
					detached = "yes"
				}

				utils.Println(
					format,
					handler.Name,
					detached,
					handler.Path,
				)
			}
		}

	case "list-timers":
		packet := socket.NewPacket(ipc.KIND_LIST_TIMERS)

		var err error
		var list []*triggers.TimerStat

		if err = socket.Send(packet); err != nil {
			return err

		} else if packet, err = socket.Receive(); err != nil {
			return err

		} else if !packet.IsKindOK() {
			return fmt.Errorf("failed to obtain the timers list")

		} else if err = packet.ExtractPayload(&list); err != nil {
			return err
		}

		format := "%-24s %-12s %s"
		utils.Println(format, "NAME", "TYPE", "ACTIVE")
		utils.Println(format, "----", "----", "------")

		for _, timer := range list {
			active := "no"

			if timer.Active {
				active = "yes"
			}

			utils.Println(
				format,
				timer.Name,
				timer.Type,
				active,
			)
		}
	}

	return nil
}
