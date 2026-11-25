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
	"fmt"
	"os"
	"os/user"
	"ripple/events"
	"ripple/ipc"
	"ripple/triggers"
	"ripple/utils"
	"strconv"
	"time"

	"github.com/coreos/go-systemd/v22/daemon"
	"github.com/spf13/pflag"
)

/**
 * REGISTRY_PATH is the default filesystem root for event handler discovery.
 */
const REGISTRY_PATH = "/etc/ripple/handlers"

/**
 * DefaultTimeout is the default synchronous handler timeout. Zero disables it.
 */
var DefaultTimeout = 5 * time.Second

/**
 * Options contains the command-line configuration used to select daemon or
 * client operation and execute a client command.
 */
type Options struct {
	/*
	 * ShowHelp requests command-line usage information.
	 */
	ShowHelp bool

	/*
	 * Wait requests that an emitted event complete before the client returns.
	 */
	Wait bool

	/*
	 * Timeout specifies the synchronous handler timeout. Zero disables it.
	 */
	Timeout time.Duration

	/*
	 * Daemon selects dispatch-daemon operation instead of client operation.
	 */
	Daemon bool

	/*
	 * AllowedGID identifies the group permitted to access the daemon socket.
	 */
	AllowedGID string

	/*
	 * Command is the client operation to perform.
	 */
	Command string

	/*
	 * Args contains positional arguments associated with Command.
	 */
	Args []string
}

/*
 * dispatch receives timer trigger requests from running timers and is
 * consumed by the dispatch watcher goroutine.
 */
var EventDispatcher chan triggers.DispatchEvent

/**
 * DispatchWatcher forwards timer trigger requests to the event dispatcher and
 * returns each dispatch result to the originating timer.
 */
func DispatchWatcher() {
	for req := range EventDispatcher {
		req.Reply <- events.DispatchEvent(
			req.Timer,
			nil,
			DefaultTimeout,
		)
	}
}

/**
 * Application entry point.
 */
func main() {
	if err := run(); err != nil {
		utils.EPrintln(err)

		if errors.Is(err, events.ErrExitError) {
			os.Exit(254)

		} else if errors.Is(err, events.ErrTimeout) {
			os.Exit(253)
		}

		os.Exit(1)
	}
}

/**
 * run parses command-line options and starts Ripple in daemon or client mode.
 *
 * @return
 *   Any option parsing, startup, connection, or command execution error.
 */
func run() error {
	opts, err := ParseOptions()

	if err != nil {
		return err

	} else if opts.Daemon {
		return runDaemon(opts)

	} else {
		return runClient(opts)
	}
}

/**
 * runDaemon initializes event discovery and timers, creates the authenticated
 * IPC listener, notifies systemd when ready, and serves clients indefinitely.
 *
 * @param opts
 *   Parsed daemon options, including the group allowed to access Ripple.
 *
 * @return
 *   Any initialization or listener setup error.
 */
func runDaemon(opts *Options) error {
	var err error

	if opts.AllowedGID != "" {
		AllowedGID, err = ResolveGID(opts.AllowedGID)

		if err != nil {
			return err
		}
	}

	listener, err := ipc.Listen(ipc.UNIX_SOCKET_FILE)

	if err != nil {
		return err
	}

	defer listener.Close()

	if err := os.Chown(ipc.UNIX_SOCKET_FILE, os.Getuid(), int(AllowedGID)); err != nil {
		return err

	} else if err := os.Chmod(ipc.UNIX_SOCKET_FILE, 0660); err != nil {
		return err
	}

	utils.Println("Starting ripple dispatch daemon")

	events.InitDispatcher()

	if err := events.LoadRegistry(REGISTRY_PATH); err != nil {
		return err

	} else {
		EventDispatcher = make(chan triggers.DispatchEvent)

		if err := triggers.UpdateTimerRegistry(events.ListEvents(), EventDispatcher); err != nil {
			return err
		}
	}

	go DispatchWatcher()

	// tell systemd we're ready
	daemon.SdNotify(false, daemon.SdNotifyReady)

	utils.Println("ready to accept incoming connections")

	for {
		conn, err := listener.Accept()

		if err != nil {
			continue
		}

		go ClientHandler(conn)
	}
}

/**
 * runClient connects to the Ripple daemon, verifies its ready response, and
 * executes the requested client command.
 *
 * @param opts
 *   Parsed client command and arguments.
 *
 * @return
 *   Any connection, authentication, or command execution error.
 */
func runClient(opts *Options) error {
	socket, err := ipc.Connect(ipc.UNIX_SOCKET_FILE)

	if err != nil {
		return err
	}

	defer socket.Close()

	packet, err := socket.Receive()

	if err != nil {
		return err

	} else if !packet.IsKindOK() {
		return fmt.Errorf("connection refused")
	}

	return ClientSession(socket, opts)
}

/**
 * Resolves a group identifier.
 *
 * The supplied value may be either a numeric GID or a group name.
 * Returns the resolved GID on success.
 *
 * @param value
 *   Group name or numeric group identifier.
 *
 * @return
 *   Resolved group identifier.
 *
 * @return
 *   Any group lookup or numeric conversion error.
 */
func ResolveGID(value string) (uint32, error) {
	allowgid, err := strconv.Atoi(value)

	if err != nil {
		group, err := user.LookupGroup(value)

		if err != nil {
			return 0, err
		}

		allowgid, err = strconv.Atoi(group.Gid)

		if err != nil {
			return 0, err
		}
	}

	if allowgid < 0 {
		return 0, fmt.Errorf("invalid gid: %s", value)
	}

	return uint32(allowgid), nil
}

/**
 * ParseOptions parses and validates Ripple's command-line arguments.
 *
 * @return
 *   Parsed daemon or client options.
 *
 * @return
 *   An error when flags or positional arguments form an invalid invocation.
 */
func ParseOptions() (*Options, error) {
	opts := Options{}

	fs := pflag.NewFlagSet("ripple", pflag.ContinueOnError)

	fs.BoolVarP(
		&opts.ShowHelp,
		"help",
		"h",
		false,
		"show help",
	)

	fs.BoolVar(
		&opts.Wait,
		"wait",
		false,
		"wait for event completion",
	)

	fs.BoolVar(
		&opts.Daemon,
		"daemon",
		false,
		"run the ripple dispatch daemon",
	)

	fs.DurationVar(
		&opts.Timeout,
		"timeout",
		DefaultTimeout,
		"timeout per synchronous event handler (0 disables timeout; --daemon or --emit only)",
	)

	fs.StringVar(
		&opts.AllowedGID,
		"gid",
		fmt.Sprintf("%d", AllowedGID),
		"group name or gid that can manage the dispatch daemon",
	)

	if err := fs.Parse(os.Args[1:]); err != nil {
		return nil, err

	} else if opts.ShowHelp {
		utils.Println("Usage:")
		utils.Println("  ripple [options] [command, ...]")
		utils.Println("")
		utils.Println("Commands:")
		utils.Println("  status                get daemon status")
		utils.Println("  reload                re-scan for event handlers")
		utils.Println("  emit EVENT [ARGS]     dispatch event")
		utils.Println("  list-events           list all available events")
		utils.Println("  list-handlers EVENT   list all handlers for a given event")
		utils.Println("  list-timers           list all built-in timers")
		utils.Println("")
		utils.Println("Options:")

		fs.PrintDefaults()

		os.Exit(0)
	}

	if opts.Timeout < 0 {
		return nil, fmt.Errorf("timeout must not be negative")
	}

	args := fs.Args()

	if opts.Daemon {
		if len(args) != 0 {
			return nil, fmt.Errorf("invalid arguments")

		} else if opts.Wait {
			return nil, fmt.Errorf("invalid use of --wait")
		}

		DefaultTimeout = opts.Timeout

		return &opts, nil

	} else if fs.Changed("gid") {
		return nil, fmt.Errorf("invalid use of --gid")

	} else if len(args) == 0 {
		return nil, fmt.Errorf("missing command")
	}

	opts.Command = args[0]
	opts.Args = args[1:]

	switch opts.Command {
	case "emit":
		if len(opts.Args) < 1 {
			return nil, fmt.Errorf("missing event")
		}

	case "list-handlers":
		if len(opts.Args) != 1 {
			return nil, fmt.Errorf("usage: list-handlers <event>")
		}

	case "list-events", "list-timers", "status", "reload":
		if len(opts.Args) != 0 {
			return nil, fmt.Errorf("unexpected arguments")
		}

	default:
		return nil, fmt.Errorf("unknown command %q", opts.Command)
	}

	if opts.Wait && opts.Command != "emit" {
		return nil, fmt.Errorf("invalid use of --wait")
	}

	if fs.Changed("timeout") && opts.Command != "emit" {
		return nil, fmt.Errorf("invalid use of --timeout")
	}

	return &opts, nil
}
