# litestream-lib

A convenience wrapper around [litestream](https://github.com/benbjohnson/litestream) for programmatic backup control.

## Features

### Core Functions
- **`SyncAndWait`** - Trigger a backup and wait for confirmation
- **`GetSyncStatus`** - Check if local and remote are in sync
- **`EnsureExists`** - Restore from replica if local database is missing

### Utility Functions
- **`ForceCheckpoint`** - Force WAL checkpoint to truncate the WAL
- **`IsHealthy`** - Check if the replica is reachable
- **`RestoreToPath`** - Restore to a custom path (for copies/testing)
- **`WaitForSync`** - Poll until local and remote TXIDs match
- **`NewS3ReplicaClient`** - Create S3 client with programmatic auth config

### Debounced Sync
- **`Debouncer`** - Coalesce multiple sync requests into one operation
  - `Schedule()` - Request a debounced sync (resets delay timer)
  - `Flush()` - Trigger immediate sync and clear timers
  - `maxWait` - Force sync after max time even with continuous rescheduling

## Installation

```bash
go get github.com/petermakeswebsites/litestream-lib
```

## Usage

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/benbjohnson/litestream"
    litestreamlib "github.com/petermakeswebsites/litestream-lib"
    
    // Import the backend you need
    _ "github.com/benbjohnson/litestream/s3"
)

func main() {
    ctx := context.Background()

    // Create DB with polling disabled
    db := litestream.NewDB("/path/to/my.db")
    db.MonitorInterval = 0

    // Create replica client from URL
    client, _ := litestream.NewReplicaClientFromURL("s3://my-bucket/backups")
    db.Replica = litestream.NewReplicaWithClient(db, client)
    db.Replica.MonitorEnabled = false

    // Restore from backup if local database doesn't exist
    _ = litestreamlib.EnsureExists(ctx, db)

    // Open the database
    _ = db.Open()
    defer db.Close(ctx)

    // --- Immediate Sync ---
    // Trigger a backup on-demand and wait
    _ = litestreamlib.SyncAndWait(ctx, db)

    // --- Debounced Sync ---
    // For high-write scenarios, coalesce syncs
    debouncer := litestreamlib.NewDebouncer(db, 
        5*time.Second,   // delay: wait 5s after last write
        30*time.Second,  // maxWait: sync at least every 30s
    )
    defer debouncer.Stop()

    // Call this after non-critical writes
    debouncer.Schedule()

    // Before shutdown or critical operations, flush pending sync
    _ = debouncer.Flush(ctx)

    // --- Health Check ---
    healthy, _ := litestreamlib.IsHealthy(ctx, db)
    log.Printf("Replica healthy: %t", healthy)

    // --- Status ---
    status, _ := litestreamlib.GetSyncStatus(ctx, db)
    log.Printf("InSync: %t (local=%s, remote=%s)",
        status.InSync, status.LocalTXID, status.RemoteTXID)
}
```

## S3 Auth Configuration

By default, litestream uses `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment
variables. Use `NewS3ReplicaClient` to override credentials programmatically:

```go
// Create S3 client with explicit credentials (overrides env vars)
client, err := litestreamlib.NewS3ReplicaClient(litestreamlib.S3Config{
    Bucket:          "my-bucket",
    Path:            "backups/mydb",
    Region:          "us-west-2",
    AccessKeyID:     "your-access-key",     // Optional: overrides AWS_ACCESS_KEY_ID
    SecretAccessKey: "your-secret-key",     // Optional: overrides AWS_SECRET_ACCESS_KEY
})
if err != nil {
    log.Fatal(err)
}

// For S3-compatible services (MinIO, Backblaze B2, etc.)
client, err := litestreamlib.NewS3ReplicaClient(litestreamlib.S3Config{
    Bucket:          "my-bucket",
    Path:            "backups",
    Endpoint:        "https://s3.us-west-002.backblazeb2.com",
    ForcePathStyle:  true,
    AccessKeyID:     os.Getenv("B2_ACCESS_KEY"),
    SecretAccessKey: os.Getenv("B2_SECRET_KEY"),
})

// Use with litestream DB
db := litestream.NewDB("/path/to/my.db")
db.Replica = litestream.NewReplicaWithClient(db, client)
```


## Debouncer for API Use

The `Debouncer` is ideal for API handlers where you want to batch backup requests:

```go
// On non-critical writes (e.g., page view counters)
func handlePageView(w http.ResponseWriter, r *http.Request) {
    // ... save to database ...
    debouncer.Schedule() // Sync will happen after delay
    w.WriteHeader(http.StatusOK)
}

// On critical writes (e.g., payments)
func handlePayment(w http.ResponseWriter, r *http.Request) {
    // ... save to database ...
    if err := debouncer.Flush(ctx); err != nil {
        http.Error(w, "backup failed", 500)
        return
    }
    w.WriteHeader(http.StatusOK)
}
```

## Disabling Polling

To use litestream programmatically without background goroutines:

```go
db := litestream.NewDB(path)
db.MonitorInterval = 0  // Disable DB monitoring

db.Replica = litestream.NewReplicaWithClient(db, client)
db.Replica.MonitorEnabled = false  // Disable replica monitoring
```

## License

Same license as litestream (Apache 2.0).
