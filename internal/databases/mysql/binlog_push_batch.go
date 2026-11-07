// TEMP REVIEW BEGIN: new file: intent metadata, shared commit, recovery, and per-file GTIDs
package mysql

import (
	"context"
	"fmt"
	"path"

	"github.com/go-mysql-org/go-mysql/mysql"
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
	// MySQL: this file's GTIDs. MariaDB: the next header's per-domain position.
	GTIDs string `json:"GTIDs,omitempty"`
	// GTIDs from this file's previous-GTID header, used to initialize an empty
	// archived checkpoint and available for future gap calculation.
	GTIDExecutedBeforeBinlog string `json:"GTIDExecutedBeforeBinlog"`
}

// calculateBinlogCommit has no I/O and does not mutate its inputs. Both normal
// uploads and restart recovery use it. The map is membership only; intent order
// determines checkpoint application and the final committed object.
func calculateBinlogCommit(sentinel BinlogSentinelDto, uploaded map[string]bool, flavor string, checkGTIDs bool) (BinlogSentinelDto, *LogsCache, error) {
	if sentinel.Intent == nil {
		return BinlogSentinelDto{}, nil, fmt.Errorf("no binlog upload intent to commit")
	}
	intent := sentinel.Intent
	result := sentinel
	result.Intent = nil
	var archived mysql.GTIDSet
	var err error
	if checkGTIDs {
		archived, err = mysql.ParseGTIDSet(flavor, sentinel.GTIDArchived)
		if err != nil {
			return BinlogSentinelDto{}, nil, fmt.Errorf("invalid archived GTID checkpoint: %w", err)
		}
	}
	var cache *LogsCache
	initialize := sentinel.GTIDArchived == ""
	for _, file := range intent.Files {
		if !uploaded[file.ObjectName] {
			continue
		}
		cache = &LogsCache{
			LastArchivedBinlog: file.Name,
		}
		if !checkGTIDs {
			continue
		}
		if initialize {
			archived, err = mysql.ParseGTIDSet(flavor, file.GTIDExecutedBeforeBinlog)
			if err != nil {
				return BinlogSentinelDto{}, nil, err
			}
			initialize = false
		}
		contribution, err := mysql.ParseGTIDSet(flavor, file.GTIDs)
		if err != nil {
			return BinlogSentinelDto{}, nil, err
		}
		if err := mergeBinlogGTIDs(archived, contribution); err != nil {
			return BinlogSentinelDto{}, nil, err
		}
		result.GTIDArchived = archived.String()
		result.LastCommittedBinlog = path.Join(BinlogPath, file.ObjectName)
	}
	return result, cache, nil
}

// MariaDB Update replaces per-domain positions, even with smaller sequence
// numbers. Merge domains individually so an overlapping file cannot rewind a
// different domain while advancing another one.
func mergeBinlogGTIDs(archived, contribution mysql.GTIDSet) error {
	if maria, ok := archived.(*mysql.MariadbGTIDSet); ok {
		next, ok := contribution.(*mysql.MariadbGTIDSet)
		if !ok {
			return fmt.Errorf("incompatible binlog GTID flavors")
		}
		for domain, position := range next.Sets {
			current, exists := maria.Sets[domain]
			if !exists || current.SequenceNumber < position.SequenceNumber {
				if err := maria.AddSet(position); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return archived.Update(contribution.String())
}

// commitBinlogBatch commits metadata only for intent files confirmed as uploaded.
// Some intended files may never have reached storage; their metadata is skipped.
// With GTID checking enabled, publish the sentinel before updating the cache.
// Otherwise, only update the cache.
func commitBinlogBatch(ctx context.Context, folder storage.Folder, sentinel BinlogSentinelDto,
	uploaded map[string]bool, flavor string, checkGTIDs bool) (BinlogSentinelDto, error) {
	next, cache, err := calculateBinlogCommit(sentinel, uploaded, flavor, checkGTIDs)
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

func recoverIfNeeded(ctx context.Context, folder storage.Folder, sentinel BinlogSentinelDto, flavor string) (BinlogSentinelDto, error) {
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
	return commitBinlogBatch(ctx, folder, sentinel, uploaded, flavor, true)
}

// binlogFileGTIDs returns the file's contribution and its preceding executed GTIDs.
func binlogFileGTIDs(dir, name, nextName, flavor string) (mysql.GTIDSet, mysql.GTIDSet, error) {
	end, err := GetBinlogPreviousGTIDs(path.Join(dir, nextName), flavor)
	if err != nil {
		return nil, nil, err
	}
	if end == nil {
		return nil, nil, fmt.Errorf("missing GTID header in %s", nextName)
	}
	start, err := GetBinlogPreviousGTIDs(path.Join(dir, name), flavor)
	if err != nil {
		return nil, nil, err
	}
	if start == nil {
		return nil, nil, fmt.Errorf("missing GTID header in %s", name)
	}
	if flavor == mysql.MariaDBFlavor {
		return end, start, nil
	}
	before, ok := start.(*mysql.MysqlGTIDSet)
	if !ok || before == nil {
		return nil, nil, fmt.Errorf("missing MySQL GTID header in %s", name)
	}
	after, ok := end.(*mysql.MysqlGTIDSet)
	if !ok || !after.Contain(before) {
		return nil, nil, fmt.Errorf("inconsistent MySQL GTID headers for %s and %s", name, nextName)
	}
	contribution := after.Clone().(*mysql.MysqlGTIDSet)
	gtidSetMinus(contribution, before)
	return contribution, start, nil
}

// TEMP REVIEW END: new file: intent metadata, shared commit, recovery, and per-file GTIDs
