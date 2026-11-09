// TEMP REVIEW BEGIN: new file: intent metadata, shared commit, and recovery
package mysql

import (
	"context"
	"fmt"
	"path"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// BinlogUploadIntent is written before any of its objects are uploaded. Entries
// contain independent contributions: a purged middle file need not be uploaded.
type BinlogUploadIntent struct {
	Files []BinlogUploadFile `json:"Files"`
}

type BinlogUploadFile struct {
	Name       string `json:"Name"`
	ObjectName string `json:"ObjectName"`
	// MySQL: this file's transaction set. MariaDB: the successor header's position.
	GTIDs string `json:"GTIDs,omitempty"`
}

// calculateBinlogCommit has no I/O and does not mutate its inputs. Both normal
// uploads and restart recovery use it. The map is membership only; intent order
// determines checkpoint application and the final committed object.
func calculateBinlogCommit(sentinel BinlogSentinelDto, uploaded map[string]bool,
	metadataHandler binlogMetadataHandler, checkGTIDs bool,
) (BinlogSentinelDto, *LogsCache, error) {
	if sentinel.Intent == nil {
		return BinlogSentinelDto{}, nil, fmt.Errorf("no binlog upload intent to commit")
	}
	result := sentinel
	result.Intent = nil
	var committedIntent BinlogUploadIntent
	var cache *LogsCache
	for _, file := range sentinel.Intent.Files {
		if !uploaded[file.ObjectName] {
			continue
		}
		cache = &LogsCache{LastArchivedBinlog: file.Name}
		if checkGTIDs {
			committedIntent.Files = append(committedIntent.Files, file)
			result.LastCommittedBinlog = path.Join(BinlogPath, file.ObjectName)
		}
	}
	if checkGTIDs {
		var err error
		result, err = metadataHandler.updateMetadata(committedIntent, result)
		if err != nil {
			return BinlogSentinelDto{}, nil, err
		}
	}
	return result, cache, nil
}

// commitBinlogBatch commits metadata only for intent files confirmed as uploaded.
// Some intended files may never have reached storage; their metadata is skipped.
// With GTID checking enabled, publish the sentinel before updating the cache.
// Otherwise, only update the cache.
func commitBinlogBatch(ctx context.Context, folder storage.Folder, sentinel BinlogSentinelDto,
	uploaded map[string]bool, metadataHandler binlogMetadataHandler, checkGTIDs bool) (BinlogSentinelDto, error) {
	next, cache, err := calculateBinlogCommit(sentinel, uploaded, metadataHandler, checkGTIDs)
	if err != nil {
		return sentinel, err
	}
	if checkGTIDs {
		if err := UploadBinlogSentinel(ctx, folder, &next); err != nil {
			// Keep the intent and cache unchanged on failure. The next invocation
			// reads storage again, including when the failed response hid success.
			return sentinel, fmt.Errorf("commit binlog upload metadata: %w", err)
		}
	}
	if cache != nil {
		putCache(*cache)
	}
	return next, nil
}

func recoverIfNeeded(ctx context.Context, folder storage.Folder, sentinel BinlogSentinelDto, metadataHandler binlogMetadataHandler) (BinlogSentinelDto, error) {
	if sentinel.Intent == nil {
		tracelog.InfoLogger.Printf("Binlog upload recovery is not needed")
		return sentinel, nil
	}

	intent := sentinel.Intent
	tracelog.InfoLogger.Printf("Found uncommitted binlog upload intent, starting recovery")

	// Find which files were uploaded to storage before binlog-push was interrupted.
	objects, _, err := folder.GetSubFolder(BinlogPath).ListFolder(ctx)
	if err != nil {
		return sentinel, fmt.Errorf("list binlogs for upload recovery: %w", err)
	}
	wanted := make(map[string]bool, len(intent.Files))
	for _, file := range intent.Files {
		wanted[file.ObjectName] = true
	}
	uploaded := make(map[string]bool, len(intent.Files))
	for _, object := range objects {
		if wanted[object.GetName()] {
			uploaded[object.GetName()] = true
		}
	}
	tracelog.InfoLogger.Printf("Committing %d of %d intended binlog uploads: %v", len(uploaded), len(intent.Files), uploaded)
	return commitBinlogBatch(ctx, folder, sentinel, uploaded, metadataHandler, true)
}

// TEMP REVIEW END: new file: intent metadata, shared commit, and recovery
