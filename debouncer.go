// Package litestreamlib provides convenience wrappers around the litestream
// library for programmatic backup control without polling.
package litestreamlib

import (
	"context"
	"sync"
	"time"

	"github.com/benbjohnson/litestream"
)

// Debouncer provides debounced sync operations for a litestream database.
// It coalesces multiple sync requests into a single sync operation,
// reducing the frequency of remote uploads during high-write periods.
//
// Use Schedule() to request a debounced sync - it will delay the sync
// by the configured delay duration, resetting the timer on each call.
// However, syncs will never be delayed beyond maxWait from the first request.
//
// Use Flush() to trigger an immediate sync and clear any pending timers.
type Debouncer struct {
	db      *litestream.DB
	delay   time.Duration // debounce delay
	maxWait time.Duration // maximum time before forced sync

	mu          sync.Mutex
	timer       *time.Timer
	maxTimer    *time.Timer
	pending     bool
	lastErr     error
	lastErrMu   sync.RWMutex
	syncRunning bool
	syncCond    *sync.Cond
}

// NewDebouncer creates a new Debouncer for the given database.
//
// delay is the debounce delay - how long to wait after the last Schedule()
// call before syncing.
//
// maxWait is the maximum time to wait before forcing a sync, even if
// Schedule() is being called continuously. Set to 0 to disable.
func NewDebouncer(db *litestream.DB, delay, maxWait time.Duration) *Debouncer {
	d := &Debouncer{
		db:      db,
		delay:   delay,
		maxWait: maxWait,
	}
	d.syncCond = sync.NewCond(&d.mu)
	return d
}

// Schedule requests a debounced sync. The sync will occur after the delay
// period elapses, unless Schedule is called again (which resets the delay).
//
// If maxWait is configured and the time since the first Schedule() call
// exceeds maxWait, the sync will be triggered immediately.
//
// This method is non-blocking and safe to call from multiple goroutines.
func (d *Debouncer) Schedule() {
	d.mu.Lock()
	defer d.mu.Unlock()

	// If there's already a delayed timer pending, stop it
	if d.timer != nil {
		d.timer.Stop()
	}

	// Start the max timer on first call (if not already running)
	if !d.pending && d.maxWait > 0 {
		d.maxTimer = time.AfterFunc(d.maxWait, func() {
			d.doSync()
		})
	}

	d.pending = true

	// Set new delay timer
	d.timer = time.AfterFunc(d.delay, func() {
		d.doSync()
	})
}

// doSync performs the actual sync operation.
// Called by timers when they fire.
func (d *Debouncer) doSync() {
	d.mu.Lock()

	// Check if another sync is already running
	if d.syncRunning {
		d.mu.Unlock()
		return
	}

	// Check if still pending (might have been flushed)
	if !d.pending {
		d.mu.Unlock()
		return
	}

	// Stop both timers
	if d.timer != nil {
		d.timer.Stop()
		d.timer = nil
	}
	if d.maxTimer != nil {
		d.maxTimer.Stop()
		d.maxTimer = nil
	}

	d.syncRunning = true
	d.pending = false
	d.mu.Unlock()

	// Perform sync outside the lock
	err := SyncAndWait(context.Background(), d.db)

	d.lastErrMu.Lock()
	d.lastErr = err
	d.lastErrMu.Unlock()

	d.mu.Lock()
	d.syncRunning = false
	d.syncCond.Broadcast()
	d.mu.Unlock()
}

// Flush triggers an immediate sync and clears any pending timers.
// This blocks until the sync is complete.
//
// If a sync is already in progress (from a timer), this waits for it
// to complete rather than triggering a duplicate sync.
func (d *Debouncer) Flush(ctx context.Context) error {
	d.mu.Lock()

	// Stop any pending timers
	if d.timer != nil {
		d.timer.Stop()
		d.timer = nil
	}
	if d.maxTimer != nil {
		d.maxTimer.Stop()
		d.maxTimer = nil
	}

	// If a sync is already running, wait for it
	if d.syncRunning {
		for d.syncRunning {
			d.syncCond.Wait()
		}
		d.mu.Unlock()

		d.lastErrMu.RLock()
		err := d.lastErr
		d.lastErrMu.RUnlock()
		return err
	}

	// Mark pending as false since we're flushing now
	d.pending = false
	d.syncRunning = true
	d.mu.Unlock()

	// Perform sync
	err := SyncAndWait(ctx, d.db)

	d.lastErrMu.Lock()
	d.lastErr = err
	d.lastErrMu.Unlock()

	d.mu.Lock()
	d.syncRunning = false
	d.syncCond.Broadcast()
	d.mu.Unlock()

	return err
}

// LastError returns the error from the most recent sync operation.
// This is useful for checking if background syncs are succeeding.
func (d *Debouncer) LastError() error {
	d.lastErrMu.RLock()
	defer d.lastErrMu.RUnlock()
	return d.lastErr
}

// Stop cancels any pending sync timers. Does not wait for in-progress syncs.
// Call this when shutting down to clean up goroutines.
func (d *Debouncer) Stop() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.timer != nil {
		d.timer.Stop()
		d.timer = nil
	}
	if d.maxTimer != nil {
		d.maxTimer.Stop()
		d.maxTimer = nil
	}
	d.pending = false
}

// IsPending returns true if there is a sync scheduled but not yet executed.
func (d *Debouncer) IsPending() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.pending
}
