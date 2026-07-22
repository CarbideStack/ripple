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
 * Defines built-in timers and manages timer activation, persistent calendar
 * state, interval scheduling, calendar scheduling, and dispatch triggering.
 */

package triggers

import (
	"context"
	"encoding/json"
	"os"
	"ripple/mutex"
	"ripple/utils"
	"sort"
	"sync/atomic"
	"time"
)

/**
 * Path to the persistent timer state file.
 *
 * The file stores calendar timer metadata such as creation time
 * and last successful execution. It is loaded during startup and
 * updated whenever timer state changes.
 */
const TIMER_STATE_FILE = "/var/lib/ripple/timers.json"

/**
 * TimerType identifies the scheduling model used by a timer.
 */
type TimerType int

/**
 * String returns the human-readable timer type name.
 *
 * @return
 *   String representation of the timer type.
 */
func (t TimerType) String() string {
	switch t {
	case TimerInterval:
		return "interval"

	case TimerCalendar:
		return "calendar"

	default:
		return "unknown"
	}
}

/**
 * Timer type values.
 */
const (
	/**
	 * TimerInterval identifies timers that run repeatedly after a fixed
	 * duration.
	 */
	TimerInterval TimerType = iota

	/**
	 * TimerCalendar identifies timers that run at calendar boundaries.
	 */
	TimerCalendar
)

/**
 * DispatchRequest is an internal request from a timer goroutine to the
 * dispatch watcher.
 */
type DispatchEvent struct {
	/*
	 * Timer is the timer name used as the dispatch trigger.
	 */
	Timer string

	/*
	 * Reply receives the result of sending the IPC dispatch request.
	 */
	Reply chan error
}

/**
 * Describes a timer definition.
 *
 * Interval timers use Interval to determine their execution
 * frequency. Calendar timers derive their schedule from the
 * timer name.
 */
type TimerSpec struct {
	/*
	 * Type selects interval or calendar scheduling.
	 */
	Type TimerType `json:"type"`

	/*
	 * Interval is the fixed delay between interval timer executions.
	 */
	Interval time.Duration `json:"interval"`
}

/**
 * TimerStat describes one timer for service status output.
 */
type TimerStat struct {
	/*
	 * Name is the timer name.
	 */
	Name string `json:"name"`

	/*
	 * Type is the human-readable timer type.
	 */
	Type string `json:"type"`

	/*
	 * Active reports whether the timer currently has a running goroutine.
	 */
	Active bool `json:"active"`
}

/**
 * Persistent state for a calendar timer.
 *
 * EnabledAt records when the timer state was first created and
 * remains unchanged for the lifetime of the timer. It serves as
 * the reference point when determining whether a timer has missed
 * an execution before its first successful run.
 *
 * LastRun records the most recent successful execution of the
 * timer. A zero value indicates that the timer has never run.
 */
type TimerState struct {
	/*
	 * EnabledAt records when persistent state for this timer was created.
	 */
	EnabledAt time.Time `json:"enabled_at"`

	/*
	 * LastRun records the latest successful calendar timer dispatch.
	 */
	LastRun time.Time `json:"last_run"`
}

/**
 * Built-in timer definitions.
 *
 * Each timer name can be enabled when a subscription notification reports that
 * the timer trigger is wanted on at least one dispatch channel.
 */
var Timers = map[string]TimerSpec{
	"1min": {
		Type:     TimerInterval,
		Interval: time.Minute,
	},

	"2min": {
		Type:     TimerInterval,
		Interval: 2 * time.Minute,
	},

	"5min": {
		Type:     TimerInterval,
		Interval: 5 * time.Minute,
	},

	"10min": {
		Type:     TimerInterval,
		Interval: 10 * time.Minute,
	},

	"15min": {
		Type:     TimerInterval,
		Interval: 15 * time.Minute,
	},

	"30min": {
		Type:     TimerInterval,
		Interval: 30 * time.Minute,
	},

	"45min": {
		Type:     TimerInterval,
		Interval: 45 * time.Minute,
	},

	"hourly": {
		Type:     TimerInterval,
		Interval: time.Hour,
	},

	"daily": {
		Type: TimerCalendar,
	},

	"weekday": {
		Type: TimerCalendar,
	},

	"weekend": {
		Type: TimerCalendar,
	},

	"weekly": {
		Type: TimerCalendar,
	},

	"biweekly": {
		Type: TimerCalendar,
	},

	"monthly": {
		Type: TimerCalendar,
	},

	"quarterly": {
		Type: TimerCalendar,
	},

	"semiannually": {
		Type: TimerCalendar,
	},

	"yearly": {
		Type: TimerCalendar,
	},
}

/**
 * Active timer instances indexed by timer name.
 *
 * Each entry contains the cancellation function used to stop
 * the timer goroutine.
 */
var activeTimers = utils.NewMap[string, context.CancelFunc]()

/**
 * Persistent calendar timer states indexed by timer name.
 *
 * Only calendar timers maintain state. Interval timers derive
 * their behavior entirely from runtime execution and do not
 * require persistence.
 */
var timerStates = utils.NewMap[string, *TimerState]()

/**
 * timerRegistry maps required timers
 */
var timerRegistry = utils.NewMap[string, struct{}]()

/**
 * Control for clock watcher goroutine
 */
var clockWatcher = struct {
	mutex.LockState

	/*
	 * Stops the active clock watcher goroutine. A nil value
	 * indicates that no watcher is currently running.
	 */
	Cancel context.CancelFunc
}{}

/**
 * timerStateLoaded reports whether calendar timer state has been loaded from
 * disk.
 */
var timerStateLoaded atomic.Bool

/**
 * GetTimerList returns all built-in timers and their active state.
 *
 * The result is sorted by timer type and interval.
 *
 * @return
 *   List of timer status entries.
 */
func GetTimerList() []*TimerStat {
	result := make([]*TimerStat, 0, len(Timers))

	mutex.Synchronized(activeTimers.RLocker(), func() {
		for timer, spec := range Timers {
			result = append(result, &TimerStat{
				Name:   timer,
				Type:   spec.Type.String(),
				Active: activeTimers.Has(timer),
			})
		}
	})

	sort.Slice(result, func(i, j int) bool {
		a := Timers[result[i].Name]
		b := Timers[result[j].Name]

		if a.Type != b.Type {
			return a.Type < b.Type
		}

		return a.Interval < b.Interval
	})

	return result
}

/**
 *
 * @param timers
 *   Timer names currently wanted by the channel.
 *
 * @param dispatch
 *   Internal dispatch watcher channel used by started timer goroutines.
 *
 * @return
 *   Any timer state load or save error.
 */
func UpdateTimerRegistry(
	timers []string,
	dispatch chan<- DispatchEvent,
) error {
	// Build a set of timers this channel wants.
	wanted := make(map[string]struct{}, len(timers))

	for _, timer := range timers {
		if _, exists := Timers[timer]; exists {
			wanted[timer] = struct{}{}
		}
	}

	mutex.Synchronized(timerRegistry, func() {
		timerRegistry.RemoveIf(func(timer string, _ struct{}) bool {
			if _, keep := wanted[timer]; keep {
				return false
			}

			return true
		})

		for timer := range wanted {
			if !timerRegistry.Has(timer) {
				timerRegistry.Add(timer, struct{}{})
			}
		}
	})

	if !timerStateLoaded.Swap(true) {
		if err := loadTimerStates(); err != nil {
			return err
		}
	}

	syncTimerStates()
	syncTimers(false, dispatch)

	return saveTimerStates()
}

/**
 * Synchronizes active timers with the current registry.
 *
 * Timers are enabled when at least one dispatch channel wants the timer
 * trigger and disabled when no channels want it.
 *
 * @param rebuildCalendars
 *   When true, active calendar timers are restarted to re-check missed
 *   occurrences.
 *
 * @param dispatch
 *   Internal dispatch watcher channel passed to started timers.
 *
 * @return
 *   Any active timer update error.
 */
func syncTimers(
	rebuildCalendars bool,
	dispatch chan<- DispatchEvent,
) error {
	hasCalendars := false
	required := make(map[string]bool)

	mutex.Synchronized(timerRegistry.RLocker(), func() {
		for timer := range Timers {
			if _, exists := timerRegistry.Get(timer); exists {
				required[timer] = true

				if Timers[timer].Type == TimerCalendar {
					hasCalendars = true
				}
			}
		}
	})

	err := updateActiveTimers(
		required,
		rebuildCalendars,
		dispatch,
	)

	mutex.Synchronized(&clockWatcher, func() {
		if hasCalendars && clockWatcher.Cancel == nil {
			ctx, cancel := context.WithCancel(
				context.Background(),
			)

			clockWatcher.Cancel = cancel

			go runClockWatch(ctx, dispatch)

		} else if !hasCalendars && clockWatcher.Cancel != nil {
			clockWatcher.Cancel()
			clockWatcher.Cancel = nil
		}
	})

	return err
}

/**
 * Synchronizes persistent timer state with the timer registry.
 *
 * Missing calendar timer states are created automatically and
 * obsolete states are removed. Existing state is preserved.
 *
 * Newly created states use the current time as their creation
 * timestamp.
 *
 * @return
 *   Currently nil.
 */
func syncTimerStates() error {
	timerRegistry.Lock()
	timerStates.Lock()

	defer func() {
		timerRegistry.Unlock()
		timerStates.Unlock()
	}()

	required := make(map[string]bool)

	for timer, spec := range Timers {
		if spec.Type != TimerCalendar {
			// Only calendar timers have saved states
			continue

		} else if _, exists := timerRegistry.Get(timer); exists {
			required[timer] = true
		}
	}

	now := time.Now()

	for timer := range required {
		if _, exists := timerStates.Get(timer); exists {
			// Already exists, skip
			continue
		}

		utils.Println("creating timer state '%s'", timer)

		timerStates.Add(timer, &TimerState{
			EnabledAt: now,
		})
	}

	timerStates.RemoveIf(func(timer string, state *TimerState) bool {
		if required[timer] {
			return false
		}

		utils.Println("removing timer state '%s'", timer)

		return true
	})

	return nil
}

/**
 * Saves all calendar timer states to persistent storage.
 *
 * State is written atomically by first writing a temporary file
 * and then replacing the existing state file.
 *
 * @return
 *   Error if the state could not be serialized, written, or renamed into
 *   place.
 */
func saveTimerStates() error {
	timerStates.Lock()
	defer timerStates.Unlock()

	tmp := TIMER_STATE_FILE + ".tmp"

	data, err := json.MarshalIndent(
		timerStates.GetAll(),
		"",
		"  ",
	)

	if err != nil {
		return err

	} else if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}

	return os.Rename(
		tmp,
		TIMER_STATE_FILE,
	)
}

/**
 * Loads calendar timer states from persistent storage.
 *
 * Missing state files are treated as an empty state set.
 *
 * @return
 *   Error if the state file exists but cannot be read or parsed.
 */
func loadTimerStates() error {
	timerStates.Lock()
	defer timerStates.Unlock()

	data, err := os.ReadFile(TIMER_STATE_FILE)

	if os.IsNotExist(err) {
		return nil

	} else if err != nil {
		return err
	}

	state := make(map[string]*TimerState)
	err = json.Unmarshal(
		data,
		&state,
	)

	if err != nil {
		return err
	}

	timerStates.AddAll(state)

	return nil
}

/**
 * Updates the last execution timestamp for a calendar timer.
 *
 * The updated state is persisted immediately to ensure missed
 * execution detection remains accurate across restarts.
 *
 * @param eventType
 *   Calendar timer name.
 *
 * @param when
 *   Timestamp of the successful execution.
 *
 * @return
 *   Any error encountered while saving updated state.
 */
func updateTimerLastRun(
	timer string,
	when time.Time,
) error {
	var state *TimerState
	var exists bool

	mutex.Synchronized(timerStates, func() {
		state, exists = timerStates.Get(timer)

		if exists {
			state.LastRun = when
		}
	})

	if exists {
		return saveTimerStates()
	}

	return nil
}

/**
 * Updates the active timer set.
 *
 * Required timers are started if not already running. Timers no
 * longer required are stopped and removed.
 *
 * @param required
 *   Set of timer names that should be active.
 *
 * @param rebuildCalendars
 *   When true, existing calendar timers are cancelled and restarted.
 *
 * @param dispatch
 *   Internal dispatch watcher channel passed to started timers.
 *
 * @return
 *   Currently nil.
 */
func updateActiveTimers(
	required map[string]bool,
	rebuildCalendars bool,
	dispatch chan<- DispatchEvent,
) error {
	activeTimers.Lock()
	defer activeTimers.Unlock()

	for timer := range required {
		if cancel, exists := activeTimers.Get(timer); exists {
			if Timers[timer].Type != TimerCalendar || !rebuildCalendars {
				continue
			}

			utils.Println("disabling calendar timer '%s'", timer)

			cancel()
			activeTimers.Remove(timer)
		}

		utils.Println("enabling timer '%s'", timer)

		ctx, cancel := context.WithCancel(
			context.Background(),
		)

		activeTimers.Add(timer, cancel)

		go runTimer(
			ctx,
			timer,
			Timers[timer],
			dispatch,
		)
	}

	activeTimers.RemoveIf(func(timer string, cancel context.CancelFunc) bool {
		if required[timer] {
			return false
		}

		utils.Println("disabling unused timer '%s'", timer)
		cancel()

		return true
	})

	// Placeholder to make sure that everything is made to consider errors
	return nil
}

/**
 * Executes a timer according to its specification.
 *
 * @param ctx
 *   Cancellation context for the timer.
 *
 * @param timer
 *   Timer name used as the dispatch trigger.
 *
 * @param spec
 *   Timer configuration.
 *
 * @param dispatch
 *   Internal dispatch watcher channel used to publish timer triggers.
 */
func runTimer(
	ctx context.Context,
	timer string,
	spec TimerSpec,
	dispatch chan<- DispatchEvent,
) {
	switch spec.Type {
	case TimerInterval:
		runIntervalTimer(
			ctx,
			timer,
			spec.Interval,
			dispatch,
		)

	case TimerCalendar:
		runCalendarTimer(
			ctx,
			timer,
			dispatch,
		)
	}
}

/**
 * Runs a fixed-interval timer.
 *
 * The associated event is dispatched each time the interval
 * expires until the context is cancelled.
 *
 * @param ctx
 *   Cancellation context for the timer.
 *
 * @param timer
 *   Timer name used as the dispatch trigger.
 *
 * @param interval
 *   Execution interval.
 *
 * @param dispatch
 *   Internal dispatch watcher channel used to publish timer triggers.
 */
func runIntervalTimer(
	ctx context.Context,
	timer string,
	interval time.Duration,
	dispatch chan<- DispatchEvent,
) {
	for {
		select {
		case <-time.After(interval):
			utils.Println("triggering interval timer '%s'", timer)

			reply := make(chan error, 1)

			dispatch <- DispatchEvent{
				Timer: timer,
				Reply: reply,
			}

			if err := <-reply; err != nil {
				utils.EPrintln(err)
			}

		case <-ctx.Done():
			return
		}
	}
}

/**
 * Runs a calendar-based timer.
 *
 * The timer first checks whether a scheduled occurrence was missed since the
 * last successful run. It then waits until each next calendar occurrence and
 * dispatches the timer trigger to all channels currently registered for that
 * timer.
 *
 * @param ctx
 *   Cancellation context for the timer.
 *
 * @param timer
 *   Timer name used as the dispatch trigger.
 *
 * @param dispatch
 *   Internal dispatch watcher channel used to publish timer triggers.
 */
func runCalendarTimer(
	ctx context.Context,
	timer string,
	dispatch chan<- DispatchEvent,
) {
	var state *TimerState

	mutex.Synchronized(timerStates.RLocker(), func() {
		state, _ = timerStates.Get(timer)
	})

	/*
	 * TODO:
	 *		Should be impossible, but what if state is nil?
	 */

	reference := state.LastRun

	if reference.IsZero() {
		reference = state.EnabledAt
	}

	next := nextOccurrence(timer, reference)

	if next.Before(time.Now()) {
		utils.Println("triggering missed calendar timer '%s'", timer)

		if err := triggerCalendarTimer(timer, dispatch); err != nil {
			utils.EPrintln(err)
		}
	}

	for {
		next := nextOccurrence(timer, time.Now())
		wait := time.Until(next)

		select {
		case <-time.After(wait):
			utils.Println("triggering calendar timer '%s'", timer)

			if err := triggerCalendarTimer(timer, dispatch); err != nil {
				utils.EPrintln(err)
			}

		case <-ctx.Done():
			return
		}
	}
}

/**
 * triggerCalendarTimer dispatches a calendar timer event and waits
 * for each handler to complete before updating the timer's last execution time.
 *
 * @param timer
 *   Name of the calendar timer to trigger.
 *
 * @param dispatch
 *   Channel used to submit dispatch requests.
 */
func triggerCalendarTimer(
	timer string,
	dispatch chan<- DispatchEvent,
) error {
	reply := make(chan error, 1)

	dispatch <- DispatchEvent{
		Timer: timer,
		Reply: reply,
	}

	var err error

	if err = <-reply; err == nil {
		err = updateTimerLastRun(timer, time.Now())
	}

	return err
}

/**
 * Calculates the next scheduled occurrence of a calendar timer.
 *
 * All calendar timers are scheduled relative to local time and
 * trigger at 00:00:00 on the calculated date.
 *
 * @param timer
 *   Calendar timer name.
 *
 * @param now
 *   Current timestamp used as the calculation reference.
 *
 * @return
 *   Timestamp of the next occurrence, or now for an unknown timer name.
 */
func nextOccurrence(
	timer string,
	now time.Time,
) time.Time {
	switch timer {
	case "daily":
		return time.Date(
			now.Year(),
			now.Month(),
			now.Day()+1,
			0, 0, 0, 0,
			now.Location(),
		)

	case "weekly":
		days := (8 - int(now.Weekday())) % 7

		if days == 0 {
			days = 7
		}

		return time.Date(
			now.Year(),
			now.Month(),
			now.Day()+days,
			0, 0, 0, 0,
			now.Location(),
		)

	case "biweekly":
		next := now.AddDate(0, 0, 1)

		for {
			_, week := next.ISOWeek()

			if week%2 == 0 &&
				next.Weekday() == time.Monday {

				return time.Date(
					next.Year(),
					next.Month(),
					next.Day(),
					0, 0, 0, 0,
					next.Location(),
				)
			}

			next = next.AddDate(0, 0, 1)
		}

	case "monthly":
		return time.Date(
			now.Year(),
			now.Month()+1,
			1,
			0, 0, 0, 0,
			now.Location(),
		)

	case "yearly":
		return time.Date(
			now.Year()+1,
			1,
			1,
			0, 0, 0, 0,
			now.Location(),
		)

	case "quarterly":
		month := now.Month()

		var nextMonth time.Month

		switch {
		case month < 4:
			nextMonth = 4

		case month < 7:
			nextMonth = 7

		case month < 10:
			nextMonth = 10

		default:
			return time.Date(
				now.Year()+1,
				1,
				1,
				0, 0, 0, 0,
				now.Location(),
			)
		}

		return time.Date(
			now.Year(),
			nextMonth,
			1,
			0, 0, 0, 0,
			now.Location(),
		)

	case "semiannually":
		month := now.Month()

		if month < 7 {
			return time.Date(
				now.Year(),
				7,
				1,
				0, 0, 0, 0,
				now.Location(),
			)
		}

		return time.Date(
			now.Year()+1,
			1,
			1,
			0, 0, 0, 0,
			now.Location(),
		)

	case "weekday":
		next := now.AddDate(0, 0, 1)

		for {
			switch next.Weekday() {
			case time.Saturday, time.Sunday:
				next = next.AddDate(0, 0, 1)

			default:
				return time.Date(
					next.Year(),
					next.Month(),
					next.Day(),
					0, 0, 0, 0,
					next.Location(),
				)
			}
		}

	case "weekend":
		days := (6 - int(now.Weekday()) + 7) % 7

		if days == 0 {
			days = 7
		}

		next := now.AddDate(0, 0, days)

		return time.Date(
			next.Year(),
			next.Month(),
			next.Day(),
			0, 0, 0, 0,
			next.Location(),
		)
	}

	return now
}

/**
 * Monitors for large forward wall-clock jumps caused by
 * suspend/resume, hibernation, or significant clock changes.
 *
 * When a jump of two minutes or more is detected, all active
 * calendar timers are rebuilt to ensure missed occurrences
 * are dispatched as soon as possible. Interval timers are
 * not affected.
 *
 * The watcher is only active while at least one calendar
 * timer is running.
 *
 * @param ctx
 *   Cancellation context for the watcher.
 *
 * @param dispatch
 *   Internal dispatch watcher channel used when calendar timers are rebuilt.
 */
func runClockWatch(
	ctx context.Context,
	dispatch chan<- DispatchEvent,
) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	last := time.Now()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			now := time.Now()

			if now.Unix()-last.Unix() >= 120 {
				utils.Println("detected clock inconsistencies, updating active timers")
				err := syncTimers(true, dispatch)

				if err != nil {
					utils.EPrintln(err)
				}
			}

			last = now
		}
	}
}
