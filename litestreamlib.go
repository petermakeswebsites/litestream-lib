// Package litestreamlib provides convenience wrappers around the litestream
// library for programmatic backup control without polling.
package litestreamlib

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/s3"
	"github.com/superfly/ltx"
)

// SyncStatus represents the synchronization state of a database.
type SyncStatus struct {
	// LocalTXID is the highest transaction ID written locally (to LTX files).
	LocalTXID ltx.TXID
	// RemoteTXID is the highest transaction ID confirmed on the remote replica.
	RemoteTXID ltx.TXID
	// InSync is true if LocalTXID == RemoteTXID (and both are non-zero).
	InSync bool
}

// GetSyncStatus returns the current synchronization status by comparing
// the local database position with the remote replica position.
// Returns an error if the database or replica is not configured.
func GetSyncStatus(ctx context.Context, db *litestream.DB) (SyncStatus, error) {
	if db.Replica == nil {
		return SyncStatus{}, fmt.Errorf("no replica configured")
	}

	// Get local position (highest TXID in local LTX files).
	localPos, err := db.Pos()
	if err != nil {
		return SyncStatus{}, fmt.Errorf("get local position: %w", err)
	}

	// Get remote position (highest TXID on replica).
	// Use MaxLTXFileInfo which is public.
	remoteInfo, err := db.Replica.MaxLTXFileInfo(ctx, 0)
	if err != nil {
		return SyncStatus{}, fmt.Errorf("get remote position: %w", err)
	}

	return SyncStatus{
		LocalTXID:  localPos.TXID,
		RemoteTXID: remoteInfo.MaxTXID,
		InSync:     localPos.TXID == remoteInfo.MaxTXID && localPos.TXID != 0,
	}, nil
}

// SyncAndWait performs a full sync cycle (DB -> LTX -> Remote) and blocks
// until the remote replica confirms the upload. Returns nil on success,
// or an error if any part of the sync fails.
//
// This function is intended for programmatic use where you need to ensure
// data is safely replicated before proceeding (e.g., before acknowledging
// a write to a client).
func SyncAndWait(ctx context.Context, db *litestream.DB) error {
	if db.Replica == nil {
		return fmt.Errorf("no replica configured")
	}

	// Step 1: Sync WAL to local LTX files.
	if err := db.Sync(ctx); err != nil {
		return fmt.Errorf("sync db: %w", err)
	}

	// Step 2: Upload LTX files to remote replica.
	if err := db.Replica.Sync(ctx); err != nil {
		return fmt.Errorf("sync replica: %w", err)
	}

	return nil
}

// EnsureExists checks if the local database file exists. If not, it attempts
// to restore it from the configured replica. This is useful for the
// "start fresh or restore from backup" pattern.
//
// If the database file already exists, this is a no-op.
// If the replica has no data, an error is returned.
// The database must not be Open() when calling this function.
func EnsureExists(ctx context.Context, db *litestream.DB) error {
	dbPath := db.Path()

	// Check if database file exists.
	if _, err := os.Stat(dbPath); err == nil {
		// File exists, nothing to do.
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat database: %w", err)
	}

	// Database does not exist, attempt to restore from replica.
	if db.Replica == nil {
		return fmt.Errorf("database does not exist and no replica configured")
	}

	// Restore to the database path.
	opt := litestream.NewRestoreOptions()
	opt.OutputPath = dbPath

	if err := db.Replica.Restore(ctx, opt); err != nil {
		return fmt.Errorf("restore from replica: %w", err)
	}

	return nil
}

// ForceCheckpoint forces a WAL checkpoint with truncate mode, which
// copies all WAL frames to the database file and truncates the WAL.
// This is useful before backups or to reclaim disk space.
func ForceCheckpoint(ctx context.Context, db *litestream.DB) error {
	if err := db.Checkpoint(ctx, litestream.CheckpointModeTruncate); err != nil {
		return fmt.Errorf("checkpoint: %w", err)
	}
	return nil
}

// IsHealthy checks if the replica is reachable by attempting to query
// the remote position. Returns true if the replica responds, false otherwise.
// Returns an error only for configuration issues (no replica configured).
func IsHealthy(ctx context.Context, db *litestream.DB) (bool, error) {
	if db.Replica == nil {
		return false, fmt.Errorf("no replica configured")
	}

	// Attempt to query remote - if it succeeds, replica is healthy.
	_, err := db.Replica.MaxLTXFileInfo(ctx, 0)
	if err != nil {
		return false, nil // Replica unreachable, but not a config error
	}
	return true, nil
}

// RestoreToPath restores the database from the replica to a custom output path.
// This is useful for creating point-in-time copies or testing restores
// without overwriting the original database.
func RestoreToPath(ctx context.Context, db *litestream.DB, outputPath string) error {
	if db.Replica == nil {
		return fmt.Errorf("no replica configured")
	}

	opt := litestream.NewRestoreOptions()
	opt.OutputPath = outputPath

	if err := db.Replica.Restore(ctx, opt); err != nil {
		return fmt.Errorf("restore to path: %w", err)
	}
	return nil
}

// WaitForSync polls until the local and remote TXIDs match, indicating
// that all local changes have been replicated. This is useful when you
// have background syncing enabled and want to wait for completion.
//
// The pollInterval determines how often to check sync status.
// Returns immediately if already in sync or returns an error if context is cancelled.
func WaitForSync(ctx context.Context, db *litestream.DB, pollInterval time.Duration) error {
	if db.Replica == nil {
		return fmt.Errorf("no replica configured")
	}

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		status, err := GetSyncStatus(ctx, db)
		if err != nil {
			return fmt.Errorf("get sync status: %w", err)
		}
		if status.InSync {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Continue polling
		}
	}
}

// S3Config contains configuration for creating an S3 replica client.
// Optional auth fields (AccessKeyID, SecretAccessKey) override environment
// variables when set, matching the behavior of litestream's YAML config.
type S3Config struct {
	// Required: S3 bucket name
	Bucket string
	// Required: Path prefix within the bucket (e.g., "backups/mydb")
	Path string

	// Optional: AWS region (default: us-east-1)
	Region string
	// Optional: Custom endpoint for S3-compatible services (e.g., MinIO, Backblaze B2)
	Endpoint string
	// Optional: Force path-style addressing (required for some S3-compatible services)
	ForcePathStyle bool
	// Optional: Skip TLS certificate verification (for self-signed certs)
	SkipVerify bool

	// Optional: AWS access key ID (overrides AWS_ACCESS_KEY_ID env var when set)
	AccessKeyID string
	// Optional: AWS secret access key (overrides AWS_SECRET_ACCESS_KEY env var when set)
	SecretAccessKey string
}

// NewS3ReplicaClient creates an S3 replica client from configuration.
// If AccessKeyID/SecretAccessKey are empty, the client will fall back to
// environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) or
// other AWS credentials sources (IAM role, etc.) at connection time.
func NewS3ReplicaClient(cfg S3Config) (*s3.ReplicaClient, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("bucket is required")
	}
	if cfg.Path == "" {
		return nil, fmt.Errorf("path is required")
	}

	client := s3.NewReplicaClient()
	client.Bucket = cfg.Bucket
	client.Path = cfg.Path

	// Set region (default handled by litestream if empty)
	if cfg.Region != "" {
		client.Region = cfg.Region
	}

	// S3-compatible service settings
	if cfg.Endpoint != "" {
		client.Endpoint = cfg.Endpoint
	}
	client.ForcePathStyle = cfg.ForcePathStyle
	client.SkipVerify = cfg.SkipVerify

	// Auth credentials - override env vars when explicitly set
	if cfg.AccessKeyID != "" {
		client.AccessKeyID = cfg.AccessKeyID
	}
	if cfg.SecretAccessKey != "" {
		client.SecretAccessKey = cfg.SecretAccessKey
	}

	return client, nil
}
