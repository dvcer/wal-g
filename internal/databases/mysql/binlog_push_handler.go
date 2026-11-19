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

// binlogGtidFilter decides, based on gtid_archived, whether a binlog needs to be uploaded.
// Implementations exist per MySQL flavor (mysqlGtidFilter, mariadbGtidFilter).
type binlogGtidFilter interface {
	// isValid returns true if the filter is properly configured for the current flavor.
	isValid() bool
	// shouldUpload determines if a binlog should be uploaded based on GTID checkpoint comparison.
	shouldUpload(binlog, nextBinlog string) bool
}

//gocyclo:ignore
//nolint:funlen
func HandleBinlogPush(ctx context.Context, uploader internal.Uploader, untilBinlog string, checkGTIDs bool) {
	batchSize, err := getBinlogBatchSize()
	tracelog.ErrorLogger.FatalOnError(err)

	rootFolder := uploader.Folder()
	uploader.ChangeDirectory(BinlogPath)

	conn, err := getMySQLConnection(ctx)
	tracelog.ErrorLogger.FatalOnError(err)
	defer utility.LoggedClose(conn, "")

	flavor, err := getMySQLFlavor(conn)
	tracelog.ErrorLogger.FatalOnError(err)

	binlogsFolder, err := getMySQLBinlogsFolder(conn)
	tracelog.ErrorLogger.FatalOnError(err)

	binlogs, err := getMySQLBinlogs(conn)
	tracelog.ErrorLogger.FatalOnError(err)

	var binlogSentinelDto BinlogSentinelDto
	err = FetchBinlogSentinel(ctx, rootFolder, &binlogSentinelDto)
	if err == nil && binlogSentinelDto.GTIDArchived != "" {
		tracelog.InfoLogger.Printf("fetched binlog archived GTID SET: %s\n", binlogSentinelDto.GTIDArchived)
	}
	cache := getCache()
	if len(binlogs) > 0 && cache.LastArchivedBinlog != "" {
		if BinlogPrefix(binlogs[0]) != BinlogPrefix(cache.LastArchivedBinlog) ||
			BinlogNum(binlogs[len(binlogs)-1]) < BinlogNum(cache.LastArchivedBinlog) {
			tracelog.WarningLogger.Printf("binlog was reset or naming (%s => %s), clearing cache",
				cache.LastArchivedBinlog, binlogs[0])
			cache = LogsCache{}
		}
	}

	var filter binlogGtidFilter
	if checkGTIDs {
		switch flavor {
		case mysql.MySQLFlavor:
			var gtidArchived *mysql.MysqlGTIDSet
			gtid, err := mysql.ParseMysqlGTIDSet(binlogSentinelDto.GTIDArchived)
			if err != nil {
				tracelog.WarningLogger.Printf(
					"Failed to parse MySQL GTID set '%s': %v. Uploading all binlogs and rebuilding the archived GTID checkpoint from scratch.",
					binlogSentinelDto.GTIDArchived, err)
			} else {
				var ok bool
				gtidArchived, ok = gtid.(*mysql.MysqlGTIDSet)
				if !ok {
					tracelog.WarningLogger.Printf(
						"Failed to convert GTID to MysqlGTIDSet type. Uploading all binlogs and rebuilding the archived GTID checkpoint from scratch.")
					gtidArchived = nil
				}
			}
			if gtidArchived == nil {
				binlogSentinelDto.GTIDArchived = ""
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
			if mariadbGTID == nil {
				binlogSentinelDto.GTIDArchived = ""
			}
			filter = &mariadbGtidFilter{
				binlogsFolder: binlogsFolder,
				flavor:        flavor,
				gtidArchived:  mariadbGTID,
			}
			tracelog.InfoLogger.Printf("Using MariaDB GTID filter for binlog push")
		default:
			tracelog.ErrorLogger.Fatalf("Unsupported flavor type: %s. Disable WALG_MYSQL_CHECK_GTIDS for current database.", flavor)
		}
	}

	binlogsForUpload := filterBinlogsForUpload(binlogs, &cache, untilBinlog, filter)
	for start := 0; start < len(binlogsForUpload)-1; {
		end := start + min(batchSize, len(binlogsForUpload)-1-start)
		batchBinlogs := binlogsForUpload[start : end+1]

		processBinlogBatch(ctx, uploader, rootFolder, binlogsFolder,
			batchBinlogs, flavor, checkGTIDs, &binlogSentinelDto, &cache)

		start = end
	}

}

func processBinlogBatch(ctx context.Context, uploader internal.Uploader, rootFolder storage.Folder,
	binlogsFolder string, binlogs []string, flavor string, checkGTIDs bool,
	sentinel *BinlogSentinelDto, cache *LogsCache) {
	// Open the whole batch and its successor
	files, err := openBinlogBatch(binlogsFolder, binlogs)
	tracelog.ErrorLogger.FatalOnError(err)
	defer closeBinlogBatch(files)

	// Upload every file before calculating and publishing metadata.
	err = uploadBinlogs(ctx, uploader, files)
	tracelog.ErrorLogger.FatalOnError(err)

	if checkGTIDs {
		previousGTIDs, err := extractMetadata(files, flavor)
		tracelog.ErrorLogger.FatalOnError(err)
		err = sentinel.ApplyBatchMetadata(previousGTIDs, flavor)
		tracelog.ErrorLogger.FatalOnError(err)
		tracelog.InfoLogger.Printf("Uploading binlog sentinel: %s", sentinel)
		err = UploadBinlogSentinel(ctx, rootFolder, sentinel)
		tracelog.ErrorLogger.FatalOnError(err)
	}

	// Advance the cache once the entire batch is committed.
	cache.LastArchivedBinlog = binlogs[len(binlogs)-2]
	putCache(*cache)
}

func openBinlogBatch(dir string, binlogs []string) ([]*os.File, error) {
	files := make([]*os.File, 0, len(binlogs))
	for _, binlog := range binlogs {
		filename := path.Join(dir, binlog)
		file, err := os.Open(filename)
		if err != nil {
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

func extractMetadata(files []*os.File, flavor string) ([]mysql.GTIDSet, error) {
	// Include the first file and successor so adjacent headers describe each uploaded file.
	previousGTIDs := make([]mysql.GTIDSet, 0, len(files))
	for _, file := range files {
		gtids, err := readBinlogFilePreviousGTIDs(file, flavor)
		if err != nil {
			return nil, err
		}
		previousGTIDs = append(previousGTIDs, gtids)
	}
	return previousGTIDs, nil
}

func uploadBinlogs(ctx context.Context, uploader internal.Uploader, files []*os.File) error {
	// The final successor is used only for metadata.
	for i := 0; i < len(files)-1; i++ {
		file := files[i]
		tracelog.InfoLogger.Printf("Archiving %s", path.Base(file.Name()))
		if err := uploader.UploadFile(ctx, file); err != nil {
			return fmt.Errorf("failed to upload binlog %s: %w", file.Name(), err)
		}
	}
	return nil
}

func filterBinlogsForUpload(binlogs []string, cache *LogsCache, untilBinlog string, filter binlogGtidFilter) []string {
	// No files to filter.
	if len(binlogs) == 0 {
		return binlogs
	}

	// Exclude the active binlog from uploads.
	start, end := 0, len(binlogs)-1

	// Start after the last locally archived binlog.
	if cache.LastArchivedBinlog != "" {
		for start < end && BinlogNum(binlogs[start]) <= BinlogNum(cache.LastArchivedBinlog) {
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
			cache.LastArchivedBinlog = binlogs[start]
			start++
		}
	}

	// PITR replays in upload order, so once uploading starts, keep all remaining files.
	// Retain the final file only for reading the preceding file's end-state GTID header.
	return binlogs[start : end+1]
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
