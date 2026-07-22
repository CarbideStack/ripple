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
 * Implements the Unix packet socket transport for Ripple IPC.
 */
package ipc

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/user"
	"slices"
	"strconv"
	"sync/atomic"

	"golang.org/x/sys/unix"
)

const (
	/**
	 * UNIX_SOCKET_FILE is the default filesystem path used by Ripple
	 * processes to connect to ripple.
	 */
	UNIX_SOCKET_FILE = "/var/run/ripple/ripple.sock"

	/**
	 * FLAG_KIND_REPLY marks a packet kind as a reply.
	 *
	 * The flag occupies the highest bit so reply packets can be
	 * classified independently from their concrete reply value.
	 */
	FLAG_KIND_REPLY Kind = 1 << 31

	/**
	 * FLAG_KIND_REPLY_OK marks a packet kind as a successful reply.
	 *
	 * It combines the reply and success bits so successful replies can be
	 * identified independently from their concrete reply value.
	 */
	FLAG_KIND_REPLY_OK Kind = (1 << 31) | (1 << 30)
)

/**
 * Kind identifies the semantic type of an IPC packet.
 *
 * The high bits are reserved for packet-classification flags. The remaining
 * bits identify the concrete request, reply, or notification kind.
 */
type Kind uint32

/**
 * nextSocketID generates process-local socket identifiers.
 *
 * Each accepted or connected UnixSocket receives one identifier, which is
 * later used as part of generated request IDs.
 */
var nextSocketID atomic.Uint64

/**
 * Sockid identifies a socket instance within the current process.
 *
 * Socket IDs are used as one component of Reqid values so request IDs can
 * be traced back to the socket that generated them.
 */
type Sockid uint64

/**
 * Validator is implemented by payload types that can validate themselves
 * after JSON unmarshalling.
 */
type Validator interface {
	/*
	 * Validate reports whether the decoded payload is semantically valid.
	 */
	Validate() bool
}

/**
 * Header describes the fixed metadata present on every packet.
 *
 * Header methods classify packet flow, expose request correlation state,
 * and allow forwarding code to restore or rewrite kind and request IDs.
 */
type Header interface {
	/*
	 * Kind returns the packet kind.
	 */
	Kind() Kind

	/*
	 * IsReply reports whether the kind has FLAG_KIND_REPLY set.
	 */
	IsReply() bool

	/*
	 * IsKindOK reports whether the kind represents an accepted or ready reply.
	 */
	IsKindOK() bool

	/*
	 * IsKind reports whether the header kind matches any supplied kind.
	 *
	 * @param kind
	 *   Accepted packet kinds.
	 */
	IsKind(kind ...Kind) bool
}

/**
 * Packet describes the complete IPC message abstraction.
 *
 * Implementations expose header classification, payload encoding and
 * decoding, optional file descriptor attachment, and reply derivation.
 */
type Packet interface {
	Header

	/*
	 * AttachPayload stores v as the packet payload.
	 *
	 * json.RawMessage and []byte are attached directly. Other values are
	 * marshalled with encoding/json.
	 *
	 * @param v
	 *   Value to attach as the JSON payload.
	 *
	 * @return
	 *   Any JSON marshaling error.
	 */
	AttachPayload(v any) error

	/*
	 * ExtractPayload unmarshals the packet payload into v.
	 *
	 * If v implements Validator, the returned bool is the validation result.
	 * The returned bool is also an indicator of whether the unmarshalling failed or not.
	 *
	 * @param v
	 *   Pointer to the value that receives the decoded payload.
	 *
	 * @return
	 *   Any JSON unmarshalling error.
	 */
	ExtractPayload(v any) error

	/*
	 * Payload returns the raw JSON payload bytes.
	 */
	Payload() []byte

	/*
	 * Length returns the payload length in bytes.
	 */
	Length() int
}

/**
 * Socket describes an established Ripple IPC connection.
 *
 * It can construct packets, send and receive
 * framed packets and close the underlying connection.
 */
type Socket interface {
	/*
	 * Sid returns the socket identifier.
	 */
	Sid() Sockid

	/*
	 * Close closes the underlying socket connection.
	 *
	 * @return
	 *   Any close error from the underlying connection.
	 */
	Close() error

	/*
	 * Receive reads one complete packet from the socket.
	 *
	 * @return
	 *   Received packet.
	 *
	 * @return
	 *   Any socket, framing, payload, or file descriptor error.
	 */
	Receive() (Packet, error)

	/*
	 * Send writes one packet to the socket.
	 *
	 * @param packet
	 *   Packet to send.
	 *
	 * @return
	 *   Any socket or transfer error.
	 */
	Send(packet Packet) error

	/*
	 * Packet creates a new packet.
	 *
	 * @param kind
	 *   Kind assigned to the new packet.
	 *
	 * @return
	 *   New packet associated with this socket.
	 */
	NewPacket(kind Kind) Packet
}

/**
 * ClientSocket describes an accepted peer connection.
 *
 * In addition to normal socket operations it exposes the peer credentials
 * gathered through SO_PEERCRED and the peer user's supplementary groups.
 */
type ClientSocket interface {
	Socket

	/*
	 * Uid returns the peer user ID.
	 */
	Uid() uint32

	/*
	 * Gid returns the peer primary group ID.
	 */
	Gid() uint32

	/*
	 * Pid returns the peer process ID.
	 */
	Pid() int32

	/*
	 * InGroup reports whether the peer belongs to gid as either its primary
	 * group or one of its supplementary groups.
	 *
	 * @param gid
	 *   Group ID to test.
	 */
	InGroup(gid uint32) bool
}

/**
 * Listener describes the server-side Unix socket listener.
 *
 * It accepts peer-capable sockets and can close the listening socket.
 */
type Listener interface {
	/*
	 * Close closes the underlying listener.
	 *
	 * @return
	 *   Any close error from the underlying listener.
	 */
	Close() error

	/*
	 * Accept waits for and returns the next client connection.
	 *
	 * @return
	 *   Accepted client socket with peer credentials populated.
	 *
	 * @return
	 *   Any accept or credential lookup error.
	 */
	Accept() (ClientSocket, error)
}

/**
 * UnixHeader is the fixed-size binary header sent before every payload.
 *
 * The fields are exported because binary.Read and binary.Write use
 * reflection and cannot populate unexported fields.
 */
type T_Header struct {
	H_Kind   Kind
	H_Length uint32
}

var _ Header = (*T_Header)(nil)

/**
 * IsReply reports whether the header kind is marked as a reply.
 *
 * @return
 *   True when FLAG_KIND_REPLY is set on the packet kind.
 */
func (header *T_Header) IsReply() bool {
	return header.H_Kind&FLAG_KIND_REPLY == FLAG_KIND_REPLY
}

/**
 * IsKindOK reports whether the packet kind represents a successful reply.
 *
 * @return
 *   True for KIND_REPLY_OK and KIND_REPLY_READY.
 */
func (header *T_Header) IsKindOK() bool {
	return header.H_Kind&FLAG_KIND_REPLY_OK == FLAG_KIND_REPLY_OK
}

/**
 * IsKind reports whether the packet kind matches one of the supplied kinds.
 *
 * @param kind
 *   Packet kinds to compare against the header kind.
 *
 * @return
 *   True when the header kind is present in kind.
 */
func (header *T_Header) IsKind(kind ...Kind) bool {
	return slices.Contains(kind, header.H_Kind)
}

/**
 * Kind returns the packet kind stored in the header.
 *
 * @return
 *   Packet kind.
 */
func (header *T_Header) Kind() Kind {
	return header.H_Kind
}

/**
 * UnixPacket is the concrete packet implementation used by T_Socket.
 *
 * A packet contains a fixed binary header, an optional JSON payload,
 * received through Unix socket ancillary data.
 */
type T_Packet struct {
	T_Header

	/*
	 * H_Payload contains the raw JSON payload bytes.
	 */
	H_Payload []byte
}

var _ Packet = (*T_Packet)(nil)

/**
 * Payload returns the raw JSON payload bytes.
 *
 * @return
 *   Packet payload, or nil when no payload is attached.
 */
func (packet *T_Packet) Payload() []byte {
	return packet.H_Payload
}

/**
 * AttachPayload serializes and stores a payload on the packet.
 *
 * json.RawMessage and []byte are stored directly. Other values are JSON
 * marshalled before being attached.
 *
 * @param v
 *   Payload value to attach.
 *
 * @return
 *   Any JSON marshaling error.
 */
func (packet *T_Packet) AttachPayload(v any) error {
	switch msg := v.(type) {
	case json.RawMessage:
		packet.H_Payload = msg

	case []byte:
		packet.H_Payload = json.RawMessage(msg)

	default:
		marshalled, err := json.Marshal(v)

		if err != nil {
			return err
		}

		packet.H_Payload = marshalled
	}

	return nil
}

/**
 * ExtractPayload decodes the packet payload into v.
 *
 * @param v
 *   Pointer to the destination value.
 *
 * @return
 *   True when v implements Validator and validation succeeds.
 *
 * @return
 *   Any JSON unmarshalling error.
 */
func (packet *T_Packet) ExtractPayload(v any) error {
	if len(packet.H_Payload) == 0 {
		return fmt.Errorf("invalid 0 byte payload")

	} else if err := json.Unmarshal(packet.H_Payload, v); err != nil {
		return err

	} else if val, ok := v.(Validator); ok {
		if !val.Validate() {
			return fmt.Errorf("payload failed custom validation")
		}
	}

	return nil
}

/**
 * Length returns the payload length in bytes.
 *
 * @return
 *   Number of bytes in the raw payload.
 */
func (packet *T_Packet) Length() int {
	return len(packet.H_Payload)
}

/**
 * T_Socket wraps an established Unix packet connection.
 *
 * It implements Socket for both outbound service/client connections and
 * accepted peer connections. Credential fields are populated for
 * accepted peer sockets.
 */
type T_Socket struct {
	/*
	 * sockid is the process-local identifier for this socket.
	 */
	sockid Sockid

	/*
	 * uid, gid, and pid describe the accepted peer process.
	 */
	uid uint32
	gid uint32
	pid int32

	/*
	 * client is true when this socket represents an accepted peer.
	 */
	client bool

	/*
	 * groups contains the accepted peer user's supplementary groups.
	 */
	groups map[uint32]struct{}

	/*
	 * conn is the underlying Unix packet connection.
	 */
	conn *net.UnixConn
}

var _ Socket = (*T_Socket)(nil)
var _ ClientSocket = (*T_Socket)(nil)

/**
 * creates a new UnixPacket associated with this socket.
 *
 * @param kind
 *   Packet kind assigned to the new packet.
 *
 * @return
 *   Newly allocated packet ready for payload attachment and sending.
 */
func (socket *T_Socket) NewPacket(
	kind Kind,
) Packet {
	return &T_Packet{
		T_Header: T_Header{
			H_Kind: kind,
		},
	}
}

/**
 * Close closes the underlying Unix connection.
 *
 * @return
 *   Any close error reported by the connection.
 */
func (socket *T_Socket) Close() error {
	return socket.conn.Close()
}

/**
 * Receive reads and decodes one packet from the Unix packet socket.
 *
 * The method first reads the fixed binary header.
 * Then reads the JSON payload in a second packet read when the
 * header length is non-zero.
 *
 * @return
 *   Received packet with header and payload.
 */
func (socket *T_Socket) Receive() (Packet, error) {
	var header T_Header

	headerBuf := make(
		[]byte,
		binary.Size(header),
	)

	n, _, flags, _, err := socket.conn.ReadMsgUnix(
		headerBuf,
		nil,
	)

	if err != nil {
		return nil, err

	} else if n != len(headerBuf) {
		return nil, fmt.Errorf("invalid header length")

	} else if flags&unix.MSG_TRUNC != 0 {
		return nil, fmt.Errorf("invalid header length")
	}

	err = binary.Read(
		bytes.NewReader(headerBuf),
		binary.LittleEndian,
		&header,
	)

	if err != nil {
		return nil, err
	}

	packet := &T_Packet{
		T_Header: header,
	}

	if header.H_Length > 0 {
		packet.H_Payload = make(
			[]byte,
			header.H_Length,
		)

		n, _, flags, _, err := socket.conn.ReadMsgUnix(
			packet.H_Payload,
			nil,
		)

		if err != nil {
			return nil, err

		} else if uint32(n) != header.H_Length {
			return nil, fmt.Errorf("invalid payload length")

		} else if flags&unix.MSG_TRUNC != 0 {
			return nil, fmt.Errorf("invalid payload length")
		}
	}

	return packet, nil
}

/**
 * Send writes one packet to the Unix packet socket.
 *
 * The packet header is sent first.
 * If a payload is present, it is sent in a
 * second write using the payload length recorded in the header.
 *
 * @param packet
 *   Packet to send.
 */
func (socket *T_Socket) Send(
	packet Packet,
) error {
	header := T_Header{
		H_Kind:   packet.Kind(),
		H_Length: uint32(packet.Length()),
	}

	var buf bytes.Buffer

	err := binary.Write(
		&buf,
		binary.LittleEndian,
		header,
	)

	if err != nil {
		return err
	}

	buffered := buf.Bytes()

	n, _, err := socket.conn.WriteMsgUnix(
		buffered,
		nil,
		nil,
	)

	if err != nil {
		return err

	} else if n != len(buffered) {
		return fmt.Errorf("failed to send header")
	}

	if packet.Length() > 0 {
		n, _, err = socket.conn.WriteMsgUnix(
			packet.Payload(),
			nil,
			nil,
		)

		if err != nil {
			return err

		} else if n != packet.Length() {
			return fmt.Errorf("failed to send payload")
		}
	}

	return nil
}

/**
 * InGroup reports whether the accepted peer belongs to a group.
 *
 * The check includes both the peer's primary group and the supplementary
 * groups collected during Accept.
 *
 * @param gid
 *   Group ID to test.
 *
 * @return
 *   True when the peer belongs to gid.
 */
func (socket *T_Socket) InGroup(
	gid uint32,
) bool {
	if _, ok := socket.groups[gid]; ok {
		return true
	}

	return socket.gid == gid
}

/**
 * Sid returns this socket's process-local identifier.
 *
 * @return
 *   Socket identifier.
 */
func (socket *T_Socket) Sid() Sockid {
	return socket.sockid
}

/**
 * Uid returns the accepted peer's user ID.
 *
 * Outbound sockets created by Connect do not populate peer credentials and
 * return the zero value.
 *
 * @return
 *   Peer user ID.
 */
func (socket *T_Socket) Uid() uint32 {
	return socket.uid
}

/**
 * Gid returns the accepted peer's primary group ID.
 *
 * Outbound sockets created by Connect do not populate peer credentials and
 * return the zero value.
 *
 * @return
 *   Peer primary group ID.
 */
func (socket *T_Socket) Gid() uint32 {
	return socket.gid
}

/**
 * Pid returns the accepted peer's process ID.
 *
 * Outbound sockets created by Connect do not populate peer credentials and
 * return the zero value.
 *
 * @return
 *   Peer process ID.
 */
func (socket *T_Socket) Pid() int32 {
	return socket.pid
}

/**
 * T_Listener wraps the server-side Unix packet listener.
 *
 * Ripple uses this type to accept peer connections and convert them
 * into ClientSocket values populated with Unix credentials.
 */
type T_Listener struct {
	conn *net.UnixListener
}

var _ Listener = (*T_Listener)(nil)

/**
 * Accept waits for and returns the next client connection.
 *
 * The accepted connection is inspected with SO_PEERCRED to collect the peer
 * process credentials. The peer user's supplementary groups are also loaded
 * to allow group-based authorizatin.
 *
 * @return
 *   Accepted client socket with credentials and group membership populated.
 *
 * @return
 *   Any accept, credential lookup, group lookup, or parsing error.
 */
func (listener *T_Listener) Accept() (
	ClientSocket,
	error,
) {
	conn, err := listener.conn.AcceptUnix()

	if err != nil {
		return nil, err
	}

	success := false

	defer func() {
		if !success {
			conn.Close()
		}
	}()

	raw, err := conn.SyscallConn()

	if err != nil {
		return nil, err
	}

	var (
		fd   int
		cred *unix.Ucred
	)

	err = raw.Control(func(f uintptr) {
		fd = int(f)

		cred, err = unix.GetsockoptUcred(
			fd,
			unix.SOL_SOCKET,
			unix.SO_PEERCRED,
		)
	})

	if err != nil {
		return nil, err
	}

	u, err := user.LookupId(
		strconv.FormatUint(
			uint64(cred.Uid),
			10,
		),
	)

	if err != nil {
		return nil, err
	}

	groupIDs, err := u.GroupIds()

	if err != nil {
		return nil, err
	}

	groups := make(map[uint32]struct{})

	for _, gid := range groupIDs {
		id, err := strconv.ParseUint(
			gid,
			10,
			32,
		)

		if err != nil {
			return nil, err
		}

		groups[uint32(id)] = struct{}{}
	}

	success = true

	return &T_Socket{
		conn:   conn,
		sockid: Sockid(nextSocketID.Add(1)),
		uid:    cred.Uid,
		gid:    cred.Gid,
		pid:    cred.Pid,
		groups: groups,
		client: true,
	}, nil
}

/**
 * Close closes the underlying Unix listener.
 *
 * @return
 *   Any close error reported by the listener.
 */
func (listener *T_Listener) Close() error {
	return listener.conn.Close()
}

/**
 * Connect opens an outbound Unix packet socket connection.
 *
 * Client commands and Ripple services use this to connect to the dispatcher
 * socket. The returned socket can send requests and receive replies, but it
 * does not contain peer credential metadata.
 *
 * @param file
 *   Filesystem path of the Unix packet socket to connect to.
 *
 * @return
 *   Connected socket.
 *
 * @return
 *   Any dial error.
 */
func Connect(
	file string,
) (Socket, error) {
	conn, err := net.DialUnix(
		"unixpacket",
		nil,
		&net.UnixAddr{
			Name: file,
			Net:  "unixpacket",
		},
	)

	if err != nil {
		return nil, err
	}

	return &T_Socket{
			sockid: Sockid(nextSocketID.Add(1)),
			conn:   conn,
		},
		nil
}

/**
 * Listen creates a Unix packet socket listener.
 *
 * If file already exists and is a socket, it is removed before binding. If it
 * exists and is not a socket, an error is returned to avoid overwriting an
 * unrelated file.
 *
 * @param file
 *   Filesystem path to bind as a Unix packet socket.
 *
 * @return
 *   Listener that accepts credential-aware client sockets.
 *
 * @return
 *   Any path validation, removal, or listen error.
 */
func Listen(
	file string,
) (Listener, error) {
	info, err := os.Lstat(file)

	if err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("existing file is not a socket (%s)", file)
		}

		err = os.Remove(file)
	}

	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	listener, err := net.ListenUnix(
		"unixpacket",
		&net.UnixAddr{
			Name: file,
			Net:  "unixpacket",
		},
	)

	if err != nil {
		return nil, err
	}

	return &T_Listener{
		conn: listener,
	}, nil
}
