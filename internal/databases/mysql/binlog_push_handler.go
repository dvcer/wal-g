package mysql

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/user"
	"path"
	"path/filepath"

	// TEMP REVIEW BEGIN: new batch-size parsing import
	"strconv"
	// TEMP REVIEW END: new batch-size parsing import

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"

	// TEMP REVIEW BEGIN: new configuration import
	conf "github.com/wal-g/wal-g/internal/config"
	// TEMP REVIEW END: new configuration import
	"github.com/wal-g/wal-g/utility"
)

// TEMP REVIEW BEGIN: new default batch size
const defaultBinlogBatchSize = 10

// TEMP REVIEW END: new default batch size

const BinlogCacheFileName = ".walg_mysql_binlogs_cache"

type LogsCache struct {
	LastArchivedBinlog string `json:"LastArchivedBinlog"`
}

// binlogGtidFilter decides, based on a database's GTID state, whether a binlog
// needs to be uploaded again and which GTID set has been archived so far.
// Implementations exist per MySQL flavor (mysqlGtidFilter, mariadbGtidFilter).
type binlogGtidFilter interface {
	// isValid returns true if the filter is properly configured for the current flavor.
	isValid() bool
	// shouldUpload determines if a binlog should be uploaded based on GTID checkpoint comparison.
	shouldUpload(binlog, nextBinlog string) bool
	// getArchivedGTIDString returns the string representation of the archived GTID checkpoint for the current flavor.
	getArchivedGTIDString() string
}

// TEMP REVIEW BEGIN: new batch configuration helper
func binlogBatchSize() (int, error) {
	value, set := conf.GetSetting(conf.MysqlBinlogBatchSize)
	if !set {
		return defaultBinlogBatchSize, nil
	}
	size, err := strconv.Atoi(value)
	if err != nil || size < 1 {
		return 0, fmt.Errorf("%s must be a positive integer", conf.MysqlBinlogBatchSize)
	}
	return size, nil
}

// TEMP REVIEW END: new batch configuration helper

//gocyclo:ignore
//nolint:funlen
func HandleBinlogPush(ctx context.Context, uploader internal.Uploader, untilBinlog string, checkGTIDs bool) {
	rootFolder := uploader.Folder()
	uploader.ChangeDirectory(BinlogPath)

	conn, err := getMySQLConnection(ctx)
	tracelog.ErrorLogger.FatalOnError(err)
	defer utility.LoggedClose(conn, "")

	// TEMP REVIEW BEGIN: changed: detect flavor before recovery in both modes
	flavor, err := getMySQLFlavor(conn)
	tracelog.ErrorLogger.FatalOnError(err)
	metadataHandler := newBinlogMetadataHandler(flavor)

	// TEMP REVIEW END: changed: detect flavor before recovery in both modes

	// TEMP REVIEW BEGIN: GTID-checked sentinel fetch, logging, and startup recovery
	var binlogSentinelDto BinlogSentinelDto
	initializeCheckpoint := false
	if checkGTIDs {
		err = FetchBinlogSentinel(ctx, rootFolder, &binlogSentinelDto)
		// Preserve best-effort fetching. Bootstrap only when no sentinel was loaded;
		// an existing sentinel can legitimately contain an empty GTID checkpoint.
		initializeCheckpoint = err != nil && binlogSentinelDto.Intent == nil
		if err == nil && binlogSentinelDto.GTIDArchived != "" {
			tracelog.InfoLogger.Printf("fetched binlog archived GTID SET: %s\n", binlogSentinelDto.GTIDArchived)
		}

		binlogSentinelDto, err = recoverIfNeeded(ctx, rootFolder, binlogSentinelDto, metadataHandler)
		tracelog.ErrorLogger.FatalOnError(err)
	}

	// TEMP REVIEW END: GTID-checked sentinel fetch, logging, and startup recovery

	binlogsFolder, err := getMySQLBinlogsFolder(conn)
	tracelog.ErrorLogger.FatalOnError(err)

	binlogs, err := getMySQLBinlogs(conn)
	tracelog.ErrorLogger.FatalOnError(err)

	cache := getCache()
	if len(binlogs) > 0 && cache.LastArchivedBinlog != "" {
		if BinlogPrefix(binlogs[0]) != BinlogPrefix(cache.LastArchivedBinlog) ||
			BinlogNum(binlogs[len(binlogs)-1]) < BinlogNum(cache.LastArchivedBinlog) {
			tracelog.WarningLogger.Printf("binlog was reset or naming (%s => %s), clearing cache",
				cache.LastArchivedBinlog, binlogs[0])
			cache = LogsCache{}
		}
	}

	// TEMP REVIEW BEGIN: changed: initialize existing filters from the recovered checkpoint
	var filter binlogGtidFilter
	if checkGTIDs {
		// The filter's mutable state is only for upload selection. Commit uses
		// independent intent entries and the actual uploaded-file map.
		gtid, err := mysql.ParseGTIDSet(flavor, binlogSentinelDto.GTIDArchived)
		tracelog.ErrorLogger.FatalOnError(err)
		switch flavor {
		case mysql.MySQLFlavor:
			filter = &mysqlGtidFilter{
				binlogsFolder: binlogsFolder,
				flavor:        flavor,
				gtidArchived:  gtid.(*mysql.MysqlGTIDSet),
				lastGtidSeen:  nil,
			}
		case mysql.MariaDBFlavor:
			filter = &mariadbGtidFilter{
				binlogsFolder: binlogsFolder,
				flavor:        flavor,
				gtidArchived:  gtid.(*mysql.MariadbGTIDSet),
			}
			tracelog.InfoLogger.Printf("Using MariaDB GTID filter for binlog push")
		}
	}

	// TEMP REVIEW END: changed: initialize existing filters from the recovered checkpoint

	// TEMP REVIEW BEGIN: changed upload loop: prepare batch, upload files, commit metadata
	batchSize, err := binlogBatchSize()
	tracelog.ErrorLogger.FatalOnError(err)
	binlogsForUpload := filterBinlogs(binlogs, cache.LastArchivedBinlog, untilBinlog, filter)
	// Upload binlog in batches. Update metadata and cache
	cursor := 0
	for cursor < len(binlogsForUpload)-1 {
		// Prepare upload intent - per-file metadata for the batch without advancing the archived checkpoint.
		var intent BinlogUploadIntent
		// Keep the final slice element only as the successor for GTID header reads.
		for cursor < len(binlogsForUpload)-1 && len(intent.Files) < batchSize {
			i := cursor
			cursor++
			name := binlogsForUpload[i]
			next := binlogsForUpload[i+1]
			file, err := prepareBinlogEntry(binlogsFolder, name, next, metadataHandler, checkGTIDs)
			if errors.Is(err, os.ErrNotExist) {
				tracelog.WarningLogger.Printf("Binlog %s or its successor was purged during preparation; skipping", name)
				continue
			}
			tracelog.ErrorLogger.FatalOnError(err)
			// Bootstrap missing metadata from the history before the first prepared file.
			// Persist it with the intent so recovery only needs the recorded contributions.
			if initializeCheckpoint {
				gtids, err := readBinlogPreviousGTIDs(binlogsFolder, name, flavor)
				if errors.Is(err, os.ErrNotExist) {
					tracelog.WarningLogger.Printf("Binlog file %s not found; probably purged, skipping initialization", name)
					continue
				}
				tracelog.ErrorLogger.FatalOnError(err)
				binlogSentinelDto.GTIDArchived = gtids.String()
				initializeCheckpoint = false
				tracelog.InfoLogger.Printf("Initializing binlog archived GTID SET before %s: %s", name, gtids.String())
			}
			file.ObjectName = utility.AddFileExtension(name, uploader.Compression().FileExtension())
			intent.Files = append(intent.Files, file)
		}
		if len(intent.Files) == 0 {
			break
		}

		// Persist the intent before uploading files when GTID checking is enabled.
		binlogSentinelDto.Intent = &intent
		if checkGTIDs {
			tracelog.InfoLogger.Printf("Preparing binlog upload batch of %d files", len(intent.Files))
			err = UploadBinlogSentinel(ctx, rootFolder, &binlogSentinelDto)
			tracelog.ErrorLogger.FatalOnError(err)
		}

		// Upload the batch and record only the files that reached storage.
		uploadedFiles := make(map[string]bool, len(intent.Files))
		for _, file := range intent.Files {
			err := uploadBinlog(ctx, uploader, binlogsFolder, file.Name)
			if errors.Is(err, os.ErrNotExist) {
				tracelog.WarningLogger.Printf("Binlog file %s not found; probably purged, skipping upload", file.Name)
				continue
			}
			// On other errors, leave the intent for recovery on the next invocation.
			tracelog.ErrorLogger.FatalOnError(err)
			uploadedFiles[file.ObjectName] = true
		}

		// Commit metadata for uploaded files and update the local cache
		binlogSentinelDto, err = commitBinlogBatch(ctx, rootFolder, binlogSentinelDto, uploadedFiles, metadataHandler, checkGTIDs)
		tracelog.ErrorLogger.FatalOnError(err)
	}
	// TEMP REVIEW END: changed upload loop: prepare batch, upload files, commit metadata
}

// TEMP REVIEW BEGIN: new batch preparation and per-file metadata helpers
// filterBinlogs returns a slice of the original list after the cached file and
// before untilBinlog, plus one successor. The final element is kept only for its
// previous-GTID header and must not be uploaded; it may be the active binlog.
func filterBinlogs(binlogs []string, lastArchivedBinlog, untilBinlog string, filter binlogGtidFilter) []string {
	if len(binlogs) == 0 {
		return binlogs
	}
	start, end := 0, len(binlogs)-1
	if lastArchivedBinlog != "" {
		for start < end && BinlogNum(binlogs[start]) <= BinlogNum(lastArchivedBinlog) {
			start++
		}
	}
	if untilBinlog != "" {
		for i := start; i < end; i++ {
			if BinlogNum(binlogs[i]) >= BinlogNum(untilBinlog) {
				end = i
				break
			}
		}
	}
	if filter != nil && filter.isValid() {
		// Skip only the already archived prefix. Once a file needs uploading,
		// keep all subsequent files to preserve PITR replay order.
		for start < end {
			if filter.shouldUpload(binlogs[start], binlogs[start+1]) {
				break
			}
			tracelog.DebugLogger.Printf("Skip binlog %v (gtid check)\n", binlogs[start])
			start++
		}
	}
	return binlogs[start : end+1]
}

func prepareBinlogEntry(dir, name, next string, metadataHandler binlogMetadataHandler, checkGTIDs bool) (BinlogUploadFile, error) {
	if !checkGTIDs {
		return BinlogUploadFile{Name: name}, nil
	}
	file, err := metadataHandler.collectIntentInfoForBinlogFile(dir, name, next)
	if err != nil {
		return file, fmt.Errorf("read GTIDs for %s: %w", name, err)
	}
	return file, nil
}

// TEMP REVIEW END: new batch preparation and per-file metadata helpers

func getMySQLBinlogs(conn *client.Conn) ([]string, error) {
	var result []string
	// SHOW BINARY LOGS acquire binlog mutex and may hang while mysql is committing huge transactions
	// so we read binlog index from the disk with no locking
	r, err := conn.Execute("SELECT @@log_bin_index")
	if err != nil {
		return nil, fmt.Errorf("failed to query mysql variable: %w", err)
	}
	defer r.Close()
	binlogIndex, err := r.GetString(0, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to query mysql variable: %w", err)
	}
	fh, err := os.Open(binlogIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to open binlog index: %w", err)
	}
	s := bufio.NewScanner(fh)
	for s.Scan() {
		binlog := path.Base(s.Text())
		result = append(result, binlog)
	}
	// binlogs in index files are already sorted actually, so we don't need to sort them again
	return result, nil
}

func getMySQLBinlogsFolder(conn *client.Conn) (string, error) {
	r, err := conn.Execute("SHOW VARIABLES LIKE 'log_bin_basename'")
	if err != nil {
		return "", err
	}
	defer r.Close()
	logBinBasename, err := r.GetString(0, 1)
	if err != nil {
		return "", err
	}
	return path.Dir(logBinBasename), nil
}

// TEMP REVIEW BEGIN: renamed existing upload helper
func uploadBinlog(ctx context.Context, uploader internal.Uploader, dataDir string, binlog string) error {
	tracelog.InfoLogger.Printf("Archiving %v\n", binlog)

	filename := path.Join(dataDir, binlog)
	walFile, err := os.Open(filename)
	if err != nil {
		return errors.Wrapf(err, "upload: could not open '%s'\n", filename)
	}
	defer utility.LoggedClose(walFile, "")
	err = uploader.UploadFile(ctx, walFile)
	if err != nil {
		return errors.Wrapf(err, "upload: could not upload '%s'\n", filename)
	}

	return nil
}

// TEMP REVIEW END: renamed existing upload helper

func getCache() LogsCache {
	var cache LogsCache
	var cacheFilename string

	usr, err := user.Current()
	if err == nil {
		cacheFilename = filepath.Join(usr.HomeDir, BinlogCacheFileName)
		var file []byte
		file, err = os.ReadFile(cacheFilename)
		if err == nil {
			err = json.Unmarshal(file, &cache)
			if err == nil {
				return cache
			}
		}
	}
	if os.IsNotExist(err) {
		tracelog.InfoLogger.Println("MySQL binlog cache does not exist")
	} else {
		tracelog.ErrorLogger.Printf("%+v\n", err)
	}
	return LogsCache{}
}

func putCache(cache LogsCache) {
	var cacheFilename string
	usr, err := user.Current()
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to get current user homedir: %v\n", err)
	}
	cacheFilename = filepath.Join(usr.HomeDir, BinlogCacheFileName)
	if err != nil && !os.IsNotExist(err) {
		tracelog.ErrorLogger.Printf("Failed to read MySQL binlog cache file: %v\n", err)
	}

	marshal, err := json.Marshal(&cache)
	if err == nil && len(cacheFilename) > 0 {
		err = os.WriteFile(cacheFilename, marshal, 0644)
		if err != nil {
			tracelog.ErrorLogger.Printf("Failed to write MySQL binlog cache file: %v\n", err)
		}
	}
}

func lastOrDefault(data []string, defaultValue string) string {
	if len(data) > 0 {
		return data[len(data)-1]
	}
	return defaultValue
}
