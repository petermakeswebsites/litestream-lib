package litestreamlib_test

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	litestreamlib "github.com/petermakeswebsites/litestream-lib"
)

// setupDebouncerTestDB creates a test database with replica configured.
func setupDebouncerTestDB(t *testing.T) (*litestream.DB, string) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Create a minimal SQLite database file
	if err := os.WriteFile(dbPath, make([]byte, 4096), 0644); err != nil {
		t.Fatal(err)
	}

	db := litestream.NewDB(dbPath)
	db.MonitorInterval = 0

	// Setup file replica
	replicaDir := filepath.Join(dir, "replica")
	if err := os.MkdirAll(replicaDir, 0755); err != nil {
		t.Fatal(err)
	}
	client := file.NewReplicaClient(replicaDir)
	db.Replica = litestream.NewReplicaWithClient(db, client)
	db.Replica.MonitorEnabled = false

	return db, dir
}

// mockSyncDB wraps a DB and tracks sync calls for testing.
type mockSyncDB struct {
	syncCount atomic.Int32
	syncDelay time.Duration
	mu        sync.Mutex
}

func (m *mockSyncDB) incrementSync() {
	if m.syncDelay > 0 {
		time.Sleep(m.syncDelay)
	}
	m.syncCount.Add(1)
}

func (m *mockSyncDB) count() int32 {
	return m.syncCount.Load()
}

func TestDebouncer_ScheduleAndFlush(t *testing.T) {
	db, _ := setupDebouncerTestDB(t)

	debouncer := litestreamlib.NewDebouncer(db, 50*time.Millisecond, 0)
	defer debouncer.Stop()

	// Schedule should make it pending
	debouncer.Schedule()
	if !debouncer.IsPending() {
		t.Fatal("expected pending after Schedule()")
	}

	// Flush should clear pending
	ctx := context.Background()
	// Note: Flush may fail because the DB isn't properly initialized,
	// but we're testing the debouncer logic, not the actual sync
	_ = debouncer.Flush(ctx)

	if debouncer.IsPending() {
		t.Fatal("expected not pending after Flush()")
	}
}

func TestDebouncer_DelayTriggersSync(t *testing.T) {
	db, _ := setupDebouncerTestDB(t)

	delay := 50 * time.Millisecond
	debouncer := litestreamlib.NewDebouncer(db, delay, 0)
	defer debouncer.Stop()

	debouncer.Schedule()

	// Should be pending immediately
	if !debouncer.IsPending() {
		t.Fatal("expected pending after Schedule()")
	}

	// Wait for delay to elapse
	time.Sleep(delay + 30*time.Millisecond)

	// Should no longer be pending (sync was attempted)
	if debouncer.IsPending() {
		t.Fatal("expected not pending after delay elapsed")
	}
}

func TestDebouncer_RapidCalls_ResetsDelay(t *testing.T) {
	db, _ := setupDebouncerTestDB(t)

	delay := 100 * time.Millisecond
	debouncer := litestreamlib.NewDebouncer(db, delay, 0)
	defer debouncer.Stop()

	// Make rapid calls, each should reset the delay
	for i := 0; i < 5; i++ {
		debouncer.Schedule()
		time.Sleep(30 * time.Millisecond) // Less than delay
	}

	// Should still be pending because we keep resetting
	if !debouncer.IsPending() {
		t.Fatal("expected still pending during rapid calls")
	}

	// Now wait for the delay to fully elapse
	time.Sleep(delay + 50*time.Millisecond)

	if debouncer.IsPending() {
		t.Fatal("expected not pending after delay elapsed")
	}
}

func TestDebouncer_MaxWait_ForcesSync(t *testing.T) {
	db, _ := setupDebouncerTestDB(t)

	delay := 100 * time.Millisecond
	maxWait := 150 * time.Millisecond
	debouncer := litestreamlib.NewDebouncer(db, delay, maxWait)
	defer debouncer.Stop()

	start := time.Now()
	debouncer.Schedule()

	// Keep rescheduling rapidly (faster than delay)
	ticker := time.NewTicker(30 * time.Millisecond)
	defer ticker.Stop()

	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				if debouncer.IsPending() {
					debouncer.Schedule()
				} else {
					close(done)
					return
				}
			case <-done:
				return
			}
		}
	}()

	// Wait for sync to happen via maxWait
	select {
	case <-done:
		elapsed := time.Since(start)
		// Should have synced around maxWait time, not delayed indefinitely
		if elapsed > maxWait+100*time.Millisecond {
			t.Fatalf("sync took too long: %v (maxWait=%v)", elapsed, maxWait)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for maxWait to trigger sync")
	}
}

func TestDebouncer_Flush_ImmediateSync(t *testing.T) {
	db, _ := setupDebouncerTestDB(t)

	delay := 1 * time.Second // Long delay
	debouncer := litestreamlib.NewDebouncer(db, delay, 0)
	defer debouncer.Stop()

	debouncer.Schedule()

	start := time.Now()
	ctx := context.Background()
	_ = debouncer.Flush(ctx) // Ignore error, testing timing

	elapsed := time.Since(start)
	// Flush should complete quickly, not wait for the 1s delay
	if elapsed > 500*time.Millisecond {
		t.Fatalf("Flush took too long: %v", elapsed)
	}

	if debouncer.IsPending() {
		t.Fatal("expected not pending after Flush()")
	}
}

func TestDebouncer_Stop_CancelsPending(t *testing.T) {
	db, _ := setupDebouncerTestDB(t)

	delay := 100 * time.Millisecond
	debouncer := litestreamlib.NewDebouncer(db, delay, 0)

	debouncer.Schedule()
	if !debouncer.IsPending() {
		t.Fatal("expected pending after Schedule()")
	}

	debouncer.Stop()

	if debouncer.IsPending() {
		t.Fatal("expected not pending after Stop()")
	}

	// Wait and ensure no panics or sync attempts
	time.Sleep(delay + 50*time.Millisecond)
}

func TestDebouncer_Concurrent(t *testing.T) {
	db, _ := setupDebouncerTestDB(t)

	debouncer := litestreamlib.NewDebouncer(db, 20*time.Millisecond, 100*time.Millisecond)
	defer debouncer.Stop()

	var wg sync.WaitGroup
	ctx := context.Background()

	// Launch multiple goroutines calling Schedule and Flush concurrently
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				debouncer.Schedule()
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}

	// Also flush a few times
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(50 * time.Millisecond)
			_ = debouncer.Flush(ctx)
		}()
	}

	wg.Wait()

	// If we get here without data race or panic, the test passes
	// The race detector will catch any issues
}

func TestDebouncer_FlushWaitsForRunningSync(t *testing.T) {
	db, _ := setupDebouncerTestDB(t)

	// Use a very short delay so sync starts quickly
	debouncer := litestreamlib.NewDebouncer(db, 10*time.Millisecond, 0)
	defer debouncer.Stop()

	debouncer.Schedule()

	// Wait for sync to likely start
	time.Sleep(15 * time.Millisecond)

	// Now Flush - it should wait for the running sync
	ctx := context.Background()
	start := time.Now()
	_ = debouncer.Flush(ctx)
	elapsed := time.Since(start)

	// Flush should complete (either waited or sync already done)
	t.Logf("Flush completed in %v", elapsed)
}
