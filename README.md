# Ripple

Ripple is a small Linux event-dispatch daemon. It connects named events to
executable handlers and provides one place to trigger those handlers manually,
from other programs, or from built-in timers.

The project is intended for lightweight system automation: a producer emits an
event without needing to know which scripts respond to it, while handlers can
be added or removed through the filesystem. Ripple serializes dispatching,
records handler output in the system journal, and controls access through a
local Unix socket.

## What Ripple does

Ripple runs as a daemon and builds an event registry from executable files in
`/etc/ripple/handlers`. Its command-line client connects to the daemon to:

- check whether Ripple is available;
- emit an event with optional arguments;
- reload the handler registry;
- list registered events and their handlers; and
- inspect the built-in timers and whether they are active.

An event may have multiple handlers. Handlers can run synchronously, where
Ripple waits for them, or detached in the background. Events named after a
built-in timer are emitted automatically on that timer's schedule.

## How it works

At startup, the daemon:

1. Opens `/var/run/ripple/ripple.sock`.
2. Restricts the socket to the configured group with mode `0660`.
3. Scans `/etc/ripple/handlers` for event handlers.
4. Starts the timers required by the discovered event names.
5. Notifies systemd that it is ready.
6. Accepts authenticated local clients and dispatches their requests.

Clients are authenticated using Unix peer credentials. Root and members of the
group selected with `--gid` are allowed to connect.

Event requests enter a single FIFO dispatch queue. This prevents separate event
requests from running their synchronous handlers at the same time. Each handler
is launched through `systemd-cat`, so its standard output and standard error are
available through the system journal.

```text
CLI or timer
    |
    v
Unix packet socket
    |
    v
FIFO event dispatcher
    |
    +--> synchronous handlers
    |
    +--> detached handlers
```

## Defining handlers

Ripple discovers handlers by filename. A top-level executable uses the last
`@` in its filename to separate its name from its event:

```text
/etc/ripple/handlers/backup@daily
/etc/ripple/handlers/update-cache@package-changed
/etc/ripple/handlers/send-report@monthly.d
```

The supported forms are:

| Form | Meaning |
| --- | --- |
| `NAME@EVENT` | Run `NAME` synchronously when `EVENT` is emitted. |
| `NAME@EVENT.d` | Run `NAME` as a detached handler for `EVENT`. |
| `@EVENT/` | Register executable children of the directory for `EVENT`. |
| `@EVENT/NAME.d` | Register a detached handler inside an event directory. |

For example, the following layouts both register handlers for `network-ready`:

```text
/etc/ripple/handlers/configure-firewall@network-ready

/etc/ripple/handlers/@network-ready/
    configure-firewall
    announce.d
```

Handlers must have at least one executable permission bit. Ripple ignores
hidden files, common editor or partial-write files, non-executable files, and
nested directories inside an event directory. Symbolic links are resolved to
their targets before execution.

Every handler receives the emitted event name as its first argument, followed
by any arguments supplied to `ripple emit`. Given:

```sh
ripple emit package-changed openssl upgraded
```

a matching handler is invoked conceptually as:

```sh
/path/to/handler package-changed openssl upgraded
```

Synchronous handlers have a five-second execution timeout. Ripple continues
through all synchronous handlers and reports a failure if one or more fail.
Detached handlers have no execution timeout, but the same underlying executable
cannot run concurrently with itself. If it is already running, another
detached invocation is not started.

## Built-in timers

A timer is enabled only when the event registry contains an event with the same
name. For example, installing an executable named `backup@daily` activates the
`daily` timer.

Interval timers wait for a fixed duration between dispatches:

| Event | Interval |
| --- | --- |
| `1min` | 1 minute |
| `2min` | 2 minutes |
| `5min` | 5 minutes |
| `10min` | 10 minutes |
| `15min` | 15 minutes |
| `30min` | 30 minutes |
| `45min` | 45 minutes |
| `hourly` | 1 hour |

Calendar timers run at midnight in the system's local time zone:

| Event | Schedule |
| --- | --- |
| `daily` | Every day |
| `weekday` | Monday through Friday |
| `weekend` | Saturday |
| `weekly` | Monday |
| `biweekly` | Monday in even-numbered ISO weeks |
| `monthly` | First day of each month |
| `quarterly` | January 1, April 1, July 1, and October 1 |
| `semiannually` | January 1 and July 1 |
| `yearly` | January 1 |

Calendar timer state is persisted in `/var/lib/ripple/timers.json`. Ripple uses
that state to detect a missed occurrence after restart, suspend, hibernation,
or a significant forward clock change. A missed occurrence is dispatched when
the timer is restored.

## Command-line usage

```text
Usage:
  ripple [options] [command, ...]

Commands:
  status                get daemon status
  reload                re-scan for event handlers
  emit EVENT [ARGS]     dispatch event
  list-events           list all available events
  list-handlers EVENT   list all handlers for a given event
  list-timers           list all built-in timers

Options:
      --daemon          run the Ripple dispatch daemon
      --gid GROUP       group name or numeric gid allowed to use the daemon
  -h, --help            show help
      --wait            wait for event completion
```

Examples:

```sh
# Check connectivity and authorization.
ripple status

# Dispatch and acknowledge immediately after the request is accepted.
ripple emit network-ready eth0

# Wait for synchronous handlers and receive their aggregate result.
ripple --wait emit network-ready eth0

# Re-scan the handler directory and update active timers.
ripple reload

# Inspect the registry.
ripple list-events
ripple list-handlers network-ready
ripple list-timers
```

Without `--wait`, the daemon starts event dispatch asynchronously and replies
immediately. With `--wait`, the client waits for dispatch to finish. The client
uses exit status `254` for handler failure, `253` for dispatch timeout, and `1`
for other errors.

## Running the daemon

Start Ripple with:

```sh
ripple --daemon --gid ripple
```

`--gid` accepts either a group name or a numeric group ID. If it is omitted,
Ripple uses the primary group of the process starting the daemon.

Ripple currently expects its runtime directories to exist. Before starting the
daemon, ensure that these locations are present with permissions appropriate
for the service account:

```text
/etc/ripple/handlers
/var/run/ripple
/var/lib/ripple
```

The daemon is designed to run under systemd and sends a readiness notification
after its registry and timers have initialized. Handler execution also requires
`systemd-cat` to be available.

## Building from source

Ripple requires Linux and Go 1.25 or later. From the `src` directory, use the
included build script:

```sh
cd src
./make
```

This produces the `src/ripple` executable. The equivalent Go command is:

```sh
go build -buildvcs=false -o ripple ./binary
```

The source can be checked with:

```sh
go test -buildvcs=false ./...
go vet -buildvcs=false ./...
gofmt -d binary events ipc mutex triggers utils
```

## Project structure

```text
src/
├── binary/   daemon entry point and CLI request handling
├── events/   handler discovery, registry, and event execution
├── ipc/      Unix packet socket and protocol types
├── mutex/    synchronization helpers
├── triggers/ built-in timers and persistent timer state
└── utils/    shared map and output utilities
```

Ripple is distributed under the MIT license terms included in its source files.
