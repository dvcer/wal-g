package mysql

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

const BinlogCacheFileName = ".walg_mysql_binlogs_cache"

type LogsCache struct {
	LastArchivedBinlog string `json:"LastArchivedBinlog"`
}

// BinlogUploadContext holds shared settings and storage handles for one binlog-push invocation.
type BinlogUploadContext struct {
	Uploader      internal.Uploader
	RootFolder    storage.Folder
	BinlogsFolder string
	Flavor        string
	CheckGTIDs    bool
	CacheFileName string
	BatchSize     int
	UntilBinlog   string
}

// binlogGtidFilter decides, based on gtid_archived, whether a binlog needs to be uploaded.
// Implementations exist per MySQL flavor (mysqlGtidFilter, mariadbGtidFilter).
type binlogGtidFilter interface {
	// isValid returns true if the filter is properly configured for the current flavor.
	isValid() bool
	// shouldUpload determines if a binlog should be uploaded based on GTID checkpoint comparison.
	shouldUpload(binlog, nextBinlog string) bool
}

func HandleBinlogPush(ctx context.Context, uploader internal.Uploader, untilBinlog string, checkGTIDs bool) {
	conn, err := getMySQLConnection(ctx)
	tracelog.ErrorLogger.FatalOnError(err)
	defer utility.LoggedClose(conn, "")

	binlogsFolder, err := getMySQLBinlogsFolder(conn)
	tracelog.ErrorLogger.FatalOnError(err)

	binlogs, err := getMySQLBinlogs(conn)
	tracelog.ErrorLogger.FatalOnError(err)

	flavor, err := getMySQLFlavor(conn)
	tracelog.ErrorLogger.FatalOnError(err)

	batchSize, err := getBinlogBatchSize()
	tracelog.ErrorLogger.FatalOnError(err)

	usr, err := user.Current()
	tracelog.ErrorLogger.FatalOnError(err)
	cacheFileName := filepath.Join(usr.HomeDir, BinlogCacheFileName)

	uploadCtx := &BinlogUploadContext{
		Uploader:      uploader,
		BinlogsFolder: binlogsFolder,
		Flavor:        flavor,
		CheckGTIDs:    checkGTIDs,
		CacheFileName: cacheFileName,
		BatchSize:     batchSize,
		UntilBinlog:   untilBinlog,
	}
	err = pushBinlogs(ctx, uploadCtx, binlogs)
	tracelog.ErrorLogger.FatalOnError(err)
}

//gocyclo:ignore
//nolint:funlen
func pushBinlogs(ctx context.Context, uploadCtx *BinlogUploadContext, binlogs []string) error {
	uploader := uploadCtx.Uploader
	binlogsFolder := uploadCtx.BinlogsFolder
	flavor := uploadCtx.Flavor

	rootFolder := uploader.Folder()
	uploadCtx.RootFolder = rootFolder
	uploader.ChangeDirectory(BinlogPath)

	binlogSentinelDto, err := readBinlogSentinel(ctx, rootFolder)
	if err != nil {
		return err
	}
	err = recoveryIfNeeded(ctx, rootFolder, &binlogSentinelDto)
	if err != nil {
		return err
	}

	cache := getCache(uploadCtx.CacheFileName)
	if len(binlogs) > 0 && cache.LastArchivedBinlog != "" {
		if BinlogPrefix(binlogs[0]) != BinlogPrefix(cache.LastArchivedBinlog) ||
			BinlogNum(binlogs[len(binlogs)-1]) < BinlogNum(cache.LastArchivedBinlog) {
			tracelog.WarningLogger.Printf("binlog was reset or naming (%s => %s), clearing cache",
				cache.LastArchivedBinlog, binlogs[0])
			cache = LogsCache{}
		}
	}

	var filter binlogGtidFilter
	if uploadCtx.CheckGTIDs {
		switch flavor {
		case mysql.MySQLFlavor:
			var gtidArchived *mysql.MysqlGTIDSet
			gtid, err := mysql.ParseMysqlGTIDSet(binlogSentinelDto.GTIDArchived)
			if err != nil {
				tracelog.WarningLogger.Printf(
					"Failed to parse MySQL GTID set '%s': %v. Uploading all binlogs and rebuilding the archived GTID checkpoint from scratch.",
					binlogSentinelDto.GTIDArchived, err)
				binlogSentinelDto.GTIDArchived = ""
			} else {
				var ok bool
				gtidArchived, ok = gtid.(*mysql.MysqlGTIDSet)
				if !ok {
					tracelog.WarningLogger.Printf(
						"Failed to convert GTID to MysqlGTIDSet type. Uploading all binlogs and rebuilding the archived GTID checkpoint from scratch.")
					gtidArchived = nil
				}
			}
			filter = &mysqlGtidFilter{
				binlogsFolder: binlogsFolder,
				flavor:        flavor,
				gtidArchived:  gtidArchived,
				lastGtidSeen:  nil,
			}
		case mysql.MariaDBFlavor:
			var mariadbGTID *mysql.MariadbGTIDSet
			if binlogSentinelDto.GTIDArchived != "" {
				gtid, err := mysql.ParseMariadbGTIDSet(binlogSentinelDto.GTIDArchived)
				if err != nil {
					tracelog.WarningLogger.Printf(
						"Failed to parse MariaDB GTID set '%s': %v. Uploading all binlogs and rebuilding the archived GTID checkpoint from scratch.",
						binlogSentinelDto.GTIDArchived, err)
					binlogSentinelDto.GTIDArchived = ""
				} else {
					var ok bool
					mariadbGTID, ok = gtid.(*mysql.MariadbGTIDSet)
					if !ok {
						tracelog.WarningLogger.Printf(
							"Failed to convert GTID to MariadbGTIDSet type. Uploading all binlogs and rebuilding the archived GTID checkpoint from scratch.")
						mariadbGTID = nil
					}
				}
			}
			filter = &mariadbGtidFilter{
				binlogsFolder: binlogsFolder,
				flavor:        flavor,
				gtidArchived:  mariadbGTID,
			}
			tracelog.InfoLogger.Printf("Using MariaDB GTID filter for binlog push")
		default:
			return fmt.Errorf("unsupported flavor type: %s. Disable WALG_MYSQL_CHECK_GTIDS for current database", flavor)
		}
	}

	// Non-empty results contain binlogs to upload plus one successor needed only for metadata.
	filteredBinlogs := filterBinlogsForUpload(binlogs, cache.LastArchivedBinlog, uploadCtx.UntilBinlog, filter)
	for start := 0; start < len(filteredBinlogs)-1; {
		effectiveBatchSize := min(uploadCtx.BatchSize, len(filteredBinlogs)-1-start)
		end := start + effectiveBatchSize

		batchBinlogs := filteredBinlogs[start:end]
		successor := filteredBinlogs[end]

		tracelog.InfoLogger.Printf("Starting batch processing for binlogs: %v", batchBinlogs)
		if err := processBinlogBatch(ctx, uploadCtx, batchBinlogs, successor, &binlogSentinelDto, &cache); err != nil {
			return err
		}
		tracelog.InfoLogger.Printf("Batch processing completed")

		start = end
	}
	return nil
}

// processBinlogBatch uploads and commits the files in binlogs.
// The separate successor file is read only when calculating GTID metadata.
func processBinlogBatch(ctx context.Context, uploadCtx *BinlogUploadContext, binlogs []string,
	batchSuccessorBinlog string, sentinel *BinlogSentinelDto, cache *LogsCache) error {
	uploader := uploadCtx.Uploader

	// Open the files
	files, err := openBinlogBatch(uploadCtx.BinlogsFolder, binlogs)
	if err != nil {
		return err
	}
	defer closeBinlogBatch(files)

	// Add the batch files to intent.
	sentinel.Intent = createUploadIntent(binlogs, uploader.Compression().FileExtension())
	tracelog.DebugLogger.Printf("Uploading batch intent: %s", sentinel)
	if err = UploadBinlogSentinel(ctx, uploadCtx.RootFolder, sentinel); err != nil {
		return err
	}

	// Upload the batch binlogs.
	if err = uploadBinlogs(ctx, uploader, files); err != nil {
		return err
	}

	// Try to update sentinel, continue uploding on failure
	err = updateSentinelMetadata(sentinel, files, path.Join(uploadCtx.BinlogsFolder, batchSuccessorBinlog), uploadCtx.Flavor, uploadCtx.CheckGTIDs)
	tracelog.WarningLogger.PrintOnError(err)

	// Commit the batch by publishing updated metadata
	sentinel.Intent = nil
	tracelog.DebugLogger.Printf("Batch commit, sentinel: %s", sentinel)
	if err = UploadBinlogSentinel(ctx, uploadCtx.RootFolder, sentinel); err != nil {
		return err
	}

	// Save the last uploaded filename in the cache.
	cache.LastArchivedBinlog = binlogs[len(binlogs)-1]
	putCache(*cache, uploadCtx.CacheFileName)
	return nil
}

func createUploadIntent(binlogs []string, compressionExtension string) *BinlogUploadIntent {
	intent := &BinlogUploadIntent{Files: make([]BinlogUploadFile, 0, len(binlogs))}
	for _, binlog := range binlogs {
		intent.Files = append(intent.Files, BinlogUploadFile{
			Name:       binlog,
			ObjectName: path.Join(BinlogPath, utility.AddFileExtension(binlog, compressionExtension)),
		})
	}
	return intent
}

func openBinlogBatch(dir string, binlogs []string) ([]*os.File, error) {
	files := make([]*os.File, 0, len(binlogs))
	for _, binlog := range binlogs {
		filename := path.Join(dir, binlog)
		file, err := os.Open(filename)
		if err != nil {
			closeBinlogBatch(files)
			return nil, fmt.Errorf("failed to open binlog %s: %w", filename, err)
		}
		files = append(files, file)
	}
	return files, nil
}

func closeBinlogBatch(files []*os.File) {
	for _, file := range files {
		if file != nil {
			utility.LoggedClose(file, "failed to close binlog")
		}
	}
}

func updateSentinelMetadata(sentinel *BinlogSentinelDto, files []*os.File, successor string, flavor string, checkGTIDs bool) error {
	if !checkGTIDs {
		sentinel.GTIDArchived = ""
		return nil
	}

	metadata, err := extractBatchMetadata(files, successor, flavor)
	if err != nil {
		return fmt.Errorf("failed to extract batch metadata: %w", err)
	}

	if err := sentinel.ApplyBatchMetadata(metadata, flavor); err != nil {
		return fmt.Errorf("failed to apply batch metadata: %w", err)
	}
	return nil
}

func extractBatchMetadata(files []*os.File, successor string, flavor string) (*BatchMetadata, error) {
	// Collect prev_gtid of uploaded binlogs
	metadata := &BatchMetadata{PreviousGTIDs: make([]mysql.GTIDSet, 0, len(files)+1)}
	for _, file := range files {
		gtids, err := readBinlogFilePreviousGTIDs(file, flavor)
		if err != nil {
			return nil, err
		}
		metadata.PreviousGTIDs = append(metadata.PreviousGTIDs, gtids)
	}
	// Append successor's prev_gtid
	gtids, err := GetBinlogPreviousGTIDs(successor, flavor)
	if err != nil {
		return nil, err
	}
	metadata.PreviousGTIDs = append(metadata.PreviousGTIDs, gtids)
	return metadata, nil
}

func uploadBinlogs(ctx context.Context, uploader internal.Uploader, files []*os.File) error {
	for _, file := range files {
		tracelog.InfoLogger.Printf("Archiving %s", path.Base(file.Name()))
		if err := uploader.UploadFile(ctx, file); err != nil {
			return fmt.Errorf("failed to upload binlog %s: %w", file.Name(), err)
		}
	}
	return nil
}

// filterBinlogsForUpload returns binlogs to upload plus their successor for metadata.
// It returns an empty slice when no binlogs need uploading.
func filterBinlogsForUpload(binlogs []string, lastArchivedBinlog string, untilBinlog string, filter binlogGtidFilter) []string {
	if len(binlogs) == 0 {
		return binlogs
	}

	// Exclude the active binlog from uploads.
	start, end := 0, len(binlogs)-1

	// If we have local cache - use it to skip all previously uploaded files
	if lastArchivedBinlog != "" {
		for start < end && BinlogNum(binlogs[start]) <= BinlogNum(lastArchivedBinlog) {
			tracelog.DebugLogger.Printf("Skip binlog %v (archived binlog check)\n", binlogs[start])
			start++
		}
	}

	// Apply the exclusive upper bound.
	if untilBinlog != "" {
		for i := start; i < end; i++ {
			if BinlogNum(binlogs[i]) >= BinlogNum(untilBinlog) {
				end = i
				break
			}
		}
	}

	// Skip the initial files whose GTIDs are already archived.
	if filter != nil && filter.isValid() {
		for start < end {
			if filter.shouldUpload(binlogs[start], binlogs[start+1]) {
				break
			}
			tracelog.DebugLogger.Printf("Skip binlog %v (gtid check)\n", binlogs[start])
			start++
		}
	}

	// return empty slice if nothing to upload
	if start == end {
		return []string{}
	}

	// Retain the final file only for reading the preceding file's end-state GTID header.
	return binlogs[start : end+1]
}

func readBinlogSentinel(ctx context.Context, folder storage.Folder) (BinlogSentinelDto, error) {
	var sentinel BinlogSentinelDto
	if err := FetchBinlogSentinel(ctx, folder, &sentinel); err != nil {
		// Use an empty sentinel if none exists.
		var notFound storage.ObjectNotFoundError
		if errors.As(err, &notFound) {
			return BinlogSentinelDto{}, nil
		}
		// Use an empty sentinel if JSON decoding fails.
		var syntaxError *json.SyntaxError
		var typeError *json.UnmarshalTypeError
		if errors.As(err, &syntaxError) || errors.As(err, &typeError) {
			tracelog.WarningLogger.Printf("Failed to parse binlog sentinel; using an empty sentinel: %v", err)
			return BinlogSentinelDto{}, nil
		}
		return BinlogSentinelDto{}, fmt.Errorf("failed to read binlog sentinel: %w", err)
	}

	tracelog.InfoLogger.Printf("Fetched binlog sentinel: %s", &sentinel)
	return sentinel, nil
}

// recoveryIfNeeded existing intent means last batch was not committed,
// so we just need to rollback the batch (delete all uploaded binlogs) and start again
// TODO we can try to finish batch upload instead of rollback if all files from the intent are present on the host
func recoveryIfNeeded(ctx context.Context, folder storage.Folder, sentinel *BinlogSentinelDto) error {
	if sentinel.Intent == nil {
		tracelog.InfoLogger.Println("Binlog upload recovery is not needed")
		return nil
	}
	tracelog.InfoLogger.Println("Uncommitted binlog upload intent found; starting recovery")

	// Delete every intended object; earlier recovery attempts may have removed some already.
	for _, file := range sentinel.Intent.Files {
		tracelog.InfoLogger.Printf("Removing uncommitted binlog object %s", file.ObjectName)
		object := storage.NewLocalObject(file.ObjectName, time.Time{}, 0)
		if err := folder.DeleteObjects(ctx, []storage.Object{object}); err != nil {
			var notFound storage.ObjectNotFoundError
			if !errors.As(err, &notFound) && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("failed to remove uncommitted binlog object %s: %w", file.ObjectName, err)
			}
		}
	}

	// Clear intent only after cleanup succeeds, preserving the committed metadata and cache.
	sentinel.Intent = nil
	if err := UploadBinlogSentinel(ctx, folder, sentinel); err != nil {
		return fmt.Errorf("failed to clear binlog upload intent after recovery: %w", err)
	}
	tracelog.InfoLogger.Println("Binlog upload recovery completed")
	return nil
}

func getBinlogBatchSize() (int, error) {
	batchSize := viper.GetInt(conf.MysqlBinlogBatchSize)
	if batchSize <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer, got %q",
			conf.MysqlBinlogBatchSize, viper.GetString(conf.MysqlBinlogBatchSize))
	}
	return batchSize, nil
}

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

func getCache(cacheFileName string) LogsCache {
	var cache LogsCache
	data, err := os.ReadFile(cacheFileName)
	if err == nil {
		err = json.Unmarshal(data, &cache)
		if err == nil {
			return cache
		}
	}
	if os.IsNotExist(err) {
		tracelog.InfoLogger.Println("MySQL binlog cache does not exist")
	} else {
		tracelog.ErrorLogger.Printf("%+v\n", err)
	}
	return LogsCache{}
}

func putCache(cache LogsCache, cacheFileName string) {
	marshal, err := json.Marshal(&cache)
	if err == nil {
		err = os.WriteFile(cacheFileName, marshal, 0644)
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
