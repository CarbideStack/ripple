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
	"errors"
	"io"
	"os"
	"ripple/events"
	"ripple/ipc"
	"ripple/triggers"
	"ripple/utils"
)

/**
 * Group identifier allowed to manage daemon operations.
 *
 * Clients belonging to this group, as well as the root user,
 * are granted access to the IPC interface.
 */
var AllowedGID uint32 = uint32(os.Getgid())

/**
 * ClientHandler handles one accepted client connection.
 *
 * The handler authenticates the peer, sends READY or DENIED.
 *
 * @param socket
 *   Accepted client socket with peer credentials.
 */
func ClientHandler(
	socket ipc.ClientSocket,
) {
	if socket.Uid() == 0 || socket.InGroup(AllowedGID) {
		utils.Println("authenticated client (pid: %d)", socket.Pid())

		if err := socket.Send(
			socket.NewPacket(ipc.KIND_REPLY_READY),
		); err != nil {
			utils.EPrintln("failed to send ready reply to client (pid: %d): %v", socket.Pid(), err)
		}

	} else {
		utils.EPrintln("authentication failed for client (pid: %d)", socket.Pid())

		if err := socket.Send(
			socket.NewPacket(ipc.KIND_REPLY_DENIED),
		); err != nil {
			utils.EPrintln("failed to send denied reply to client (pid: %d): %v", socket.Pid(), err)
		}

		socket.Close()

		return
	}

	defer func() {
		utils.Println("client (pid: %d) session was terminated", socket.Pid())
		socket.Close()
	}()

	for {
		packet, err := socket.Receive()

		if err != nil {
			if errors.Is(err, io.EOF) {
				return

			} else if errors.Is(err, io.ErrUnexpectedEOF) {
				utils.EPrintln("client (pid: %d) disconnected unexpectedly: %v", socket.Pid(), err)
				return
			}

			utils.EPrintln("read error from client (pid: %d): %v", socket.Pid(), err)
			return
		}

		packet, err = handlePacket(socket, packet)

		if err != nil {
			utils.Println("message error from client (pid: %d): %v", socket.Pid(), err)
		}

		if packet != nil {
			if err = socket.Send(packet); err != nil {
				utils.EPrintln("failed to send reply to client (pid: %d): %v", socket.Pid(), err)
			}
		}
	}
}

/**
 * handlePacket processes one authenticated client request and constructs its
 * response. Unsupported packet kinds receive KIND_REPLY_INVALID.
 *
 * @param socket
 *   Client socket used to construct the response packet.
 *
 * @param packet
 *   Request packet received from the client.
 *
 * @return
 *   Response packet to send, or nil when no response is required.
 *
 * @return
 *   Any request decoding or handling error.
 */
func handlePacket(
	socket ipc.ClientSocket,
	packet ipc.Packet,
) (ipc.Packet, error) {
	switch packet.Kind() {
	case ipc.KIND_PING:
		return socket.NewPacket(ipc.KIND_REPLY_OK), nil

	case ipc.KIND_RELOAD:
		if err := events.LoadRegistry(REGISTRY_PATH); err != nil {
			return socket.NewPacket(ipc.KIND_REPLY_ERROR), err

		} else if err := triggers.UpdateTimerRegistry(events.ListEvents(), EventDispatcher); err != nil {
			return socket.NewPacket(ipc.KIND_REPLY_ERROR), err
		}

		return socket.NewPacket(ipc.KIND_REPLY_OK), nil

	case ipc.KIND_DISPATCH:
		var req ipc.EmitRequest

		if err := packet.ExtractPayload(&req); err != nil {
			return socket.NewPacket(ipc.KIND_REPLY_ERROR), err
		}

		if req.Wait {
			if err := events.DispatchEvent(req.Event, req.Args); err != nil {
				kind := ipc.KIND_REPLY_ERROR

				if errors.Is(err, events.ErrExitError) {
					kind = ipc.KIND_REPLY_FAILED

				} else if errors.Is(err, events.ErrTimeout) {
					kind = ipc.KIND_REPLY_TIMEOUT
				}

				return socket.NewPacket(kind), err
			}

		} else {
			go events.DispatchEvent(req.Event, req.Args)
		}

		return socket.NewPacket(ipc.KIND_REPLY_OK), nil

	case ipc.KIND_LIST_EVENTS:
		packet = socket.NewPacket(ipc.KIND_REPLY_OK)
		err := packet.AttachPayload(
			events.ListEvents(),
		)

		if err != nil {
			return socket.NewPacket(ipc.KIND_REPLY_ERROR), err
		}

		return packet, nil

	case ipc.KIND_LIST_HANDLERS:
		var req ipc.EventRequest

		if err := packet.ExtractPayload(&req); err != nil {
			return socket.NewPacket(ipc.KIND_REPLY_ERROR), err
		}

		packet = socket.NewPacket(ipc.KIND_REPLY_OK)
		err := packet.AttachPayload(
			events.ListHandlers(req.Event),
		)

		if err != nil {
			return socket.NewPacket(ipc.KIND_REPLY_ERROR), err
		}

		return packet, nil

	case ipc.KIND_LIST_TIMERS:
		packet = socket.NewPacket(ipc.KIND_REPLY_OK)
		err := packet.AttachPayload(
			triggers.GetTimerList(),
		)

		if err != nil {
			return socket.NewPacket(ipc.KIND_REPLY_ERROR), err
		}

		return packet, nil

	default:
		return socket.NewPacket(ipc.KIND_REPLY_INVALID), nil
	}
}
