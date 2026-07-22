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

package events

import (
	"os"
	"path/filepath"
	"ripple/mutex"
	"ripple/utils"
	"strings"
	"syscall"
)

/**
 * FileID uniquely identifies a filesystem object using
 * its device and inode identifiers.
 */
type FileID struct {
	/*
	 * Dev is the device identifier reported by the filesystem.
	 */
	Dev uint64 `json:"dev"`

	/*
	 * Ino is the inode identifier reported by the filesystem.
	 */
	Ino uint64 `json:"ino"`
}

/**
 * EventHandler describes a registered executable event handler.
 *
 * Each event handler is associated with a specific event type
 * and may be configured to execute in detached mode.
 */
type EventHandler struct {
	/*
	 * Name is the handler name used in logs and status output.
	 */
	Name string `json:"name"`

	/*
	 * Path is the resolved filesystem path to the handler executable.
	 */
	Path string `json:"path"`

	/*
	 * ID uniquely identifies the handler executable for detached-run locking.
	 */
	ID FileID `json:"id"`

	/*
	 * Detached reports whether the handler should run asynchronously.
	 */
	Detached bool `json:"detached"`
}

/**
 * eventRegistry stores registered handlers indexed by event type.
 *
 * The registry is replaced atomically when LoadRegistry succeeds.
 */
var eventRegistry = utils.NewMap[string, []*EventHandler]()

/**
 * ListHandlers returns a snapshot of handlers registered for an event type.
 *
 * @param eventType
 *   Event type to look up.
 *
 * @return
 *   Copy of the registered handler list. The returned slice may be empty.
 */
func ListHandlers(eventType string) []*EventHandler {
	var list []*EventHandler

	mutex.Synchronized(eventRegistry.RLocker(), func() {
		if handlers, exists := eventRegistry.Get(eventType); exists {
			list = append(
				[]*EventHandler(nil),
				handlers...,
			)
		}
	})

	return list
}

/**
 * ListEvents returns the event types currently present in the registry.
 *
 * @return
 *   Registered event type names.
 */
func ListEvents() []string {
	var list []string

	mutex.Synchronized(eventRegistry.RLocker(), func() {
		eventRegistry.Range(func(event string, _ []*EventHandler) bool {
			list = append(list, event)
			return true
		})
	})

	return list
}

/**
 * Rebuilds the global event registry.
 *
 * A new registry is constructed from path and then atomically replaces the
 * current registry.
 *
 * @param path
 *   Root directory to scan for event handlers.
 *
 * @return
 *   Any error encountered while building the registry.
 */
func LoadRegistry(path string) error {
	events := make(map[string][]*EventHandler)
	err := scanDirectory(path, events)

	if err != nil {
		return err
	}

	mutex.Synchronized(eventRegistry, func() {
		eventRegistry.Clear()
		eventRegistry.AddAll(events)
	})

	return nil
}

/**
 * Scans an event directory and constructs an event registry.
 *
 * Top-level files use the NAME@EVENT naming convention. Top-level
 * directories use the @EVENT form and register their executable children as
 * handlers for that event. Hidden and temporary entries are ignored.
 *
 * @param root
 *   Root directory to scan.
 *
 * @param events
 *   Registry map being populated.
 *
 * @return
 *   Any error encountered while scanning the directory tree.
 */
func scanDirectory(
	root string,
	events map[string][]*EventHandler,
) error {
	entries, err := os.ReadDir(root)

	if err != nil {
		return err
	}

	for _, entry := range entries {
		name := entry.Name()

		if isHidden(name) || isTemporary(name) {
			continue
		}

		basename, eventType, detached, ok := parseEventName(name)

		if !ok {
			continue
		}

		entryPath := filepath.Join(root, name)
		info, err := os.Stat(entryPath)

		if err != nil {
			return err

		} else if info.IsDir() {
			if name[0] != '@' { // Directories have no name
				continue
			}

			err = scanEventDirectory(
				entryPath,
				eventType,
				events,
			)

			if err != nil {
				return err
			}

			continue
		}

		err = registerEvent(
			basename,
			entryPath,
			info,
			eventType,
			detached,
			events,
		)

		if err != nil {
			return err
		}
	}

	return nil
}

/**
 * Scans an event directory and registers all valid handlers
 * for a specific event type.
 *
 * Files ending with .d are marked as detached handlers. Hidden, temporary,
 * nested directory, and non-executable entries are ignored.
 *
 * @param path
 *   Directory containing event handlers.
 *
 * @param eventType
 *   Event type associated with all handlers in the directory.
 *
 * @param events
 *   Registry being populated.
 *
 * @return
 *   Any error encountered while reading the directory or statting entries.
 */
func scanEventDirectory(
	path string,
	eventType string,
	events map[string][]*EventHandler,
) error {
	entries, err := os.ReadDir(path)

	if err != nil {
		return err
	}

	for _, entry := range entries {
		name := entry.Name()
		detached := strings.HasSuffix(name, ".d")

		if isHidden(name) || isTemporary(name) {
			continue
		}

		entryPath := filepath.Join(path, name)
		info, err := os.Stat(entryPath)

		if err != nil {
			return err

		} else if info.IsDir() {
			continue
		}

		err = registerEvent(
			name,
			entryPath,
			info,
			eventType,
			detached,
			events,
		)

		if err != nil {
			continue
		}
	}

	return nil
}

/**
 * Validates and registers a single event handler.
 *
 * Non-executable files are ignored. Symbolic links are
 * resolved before the handler is added to the registry, while FileID is derived
 * from the metadata supplied by the caller.
 *
 * @param name
 *   Display name of the handler.
 *
 * @param path
 *   Filesystem path to the handler.
 *
 * @param info
 *   File metadata associated with the handler.
 *
 * @param eventType
 *   Event type handled by the executable.
 *
 * @param detached
 *   Whether the handler should execute in detached mode.
 *
 * @param events
 *   Registry being populated.
 *
 * @return
 *   Any registration error.
 */
func registerEvent(
	name string,
	path string,
	info os.FileInfo,
	eventType string,
	detached bool,
	events map[string][]*EventHandler,
) error {
	if !isExecutable(info) {
		return nil
	}

	realPath, err := resolvePath(path)

	if err != nil {
		return err
	}

	events[eventType] = append(
		events[eventType],
		&EventHandler{
			Name:     name,
			Path:     realPath,
			ID:       getFileID(info),
			Detached: detached,
		},
	)

	return nil
}

/**
 * isHidden reports whether a filename is hidden.
 *
 * @param name
 *   Filename to inspect.
 *
 * @return
 *   True when the filename starts with a dot.
 */
func isHidden(name string) bool {
	return strings.HasPrefix(name, ".")
}

/**
 * isTemporary reports whether a filename is considered temporary.
 *
 * Temporary files use common editor, backup, partial-write, or swap suffixes.
 *
 * @param name
 *   Filename to inspect.
 *
 * @return
 *   True when the filename has a known temporary suffix.
 */
func isTemporary(name string) bool {
	return strings.HasSuffix(name, "~") ||
		strings.HasSuffix(name, ".bak") ||
		strings.HasSuffix(name, ".tmp") ||
		strings.HasSuffix(name, ".swp") ||
		strings.HasSuffix(name, ".part")
}

/**
 * isExecutable reports whether a filesystem entry is executable.
 *
 * @param info
 *   File metadata to inspect.
 *
 * @return
 *   True when any executable permission bit is set.
 */
func isExecutable(info os.FileInfo) bool {
	return info.Mode().Perm()&0111 != 0
}

/**
 * resolvePath resolves all symbolic links in a filesystem path.
 *
 * @param path
 *   Filesystem path to resolve.
 *
 * @return
 *   Canonical filesystem path.
 *
 * @return
 *   Any resolution error.
 */
func resolvePath(path string) (string, error) {
	return filepath.EvalSymlinks(path)
}

/**
 * getFileID extracts the filesystem device and inode identifiers
 * from a file info structure.
 *
 * @param info
 *   File metadata containing syscall stat information.
 *
 * @return
 *   File identity derived from device and inode numbers.
 */
func getFileID(info os.FileInfo) FileID {
	stat := info.Sys().(*syscall.Stat_t)

	return FileID{
		Dev: uint64(stat.Dev),
		Ino: uint64(stat.Ino),
	}
}

/**
 * Parses an event filename.
 *
 * The filename is split at the last @ character. A .d suffix on the event type
 * marks the handler as detached and is retained on the basename while removed
 * from eventType.
 *
 * @param name
 *   Filename to parse.
 *
 * @return basename
 *   Handler name before the event suffix. Detached event suffixes leave .d on
 *   the basename.
 *
 * @return eventType
 *   Parsed event type.
 *
 * @return detached
 *   True if the handler should execute in detached mode.
 *
 * @return ok
 *   True if the filename is a valid event definition.
 */
func parseEventName(name string) (
	basename string,
	eventType string,
	detached bool,
	ok bool,
) {
	index := strings.LastIndex(name, "@")

	if index == -1 {
		return
	}

	eventType = name[index+1:]
	basename = name[:index]

	if strings.HasSuffix(eventType, ".d") {
		detached = true
		eventType = strings.TrimSuffix(eventType, ".d")
		basename += ".d"
	}

	ok = eventType != ""

	return
}
