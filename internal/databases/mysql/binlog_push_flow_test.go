package mysql

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression/lz4"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/utility"
)

func setBinlogPushTestSetting(t *testing.T, key string, value any) {
	t.Helper()
	previous := viper.Get(key)
	viper.Set(key, value)
	t.Cleanup(func() { viper.Set(key, previous) })
}

func TestPushBinlogs(t *testing.T) {
	cases := []struct {
		name             string
		batchSize        int
		cached           string
		archived         int
		until            int
		wantFiles        []int
		wantSequence     int
		wantCommits      int
		disableGTID      bool
		badHeader        int
		missingSuccessor bool
		pending          bool
		badJSON          bool
		badGTID          bool
		localCount       int
		noBinlogs        bool
	}{
		{name: "multiple batches and final partial batch", batchSize: 3, wantFiles: []int{1, 2, 3, 4}, wantSequence: 4, wantCommits: 2},
		{name: "one file per batch", batchSize: 1, wantFiles: []int{1, 2, 3, 4}, wantSequence: 4, wantCommits: 4},
		{name: "batch larger than backlog", batchSize: 10, wantFiles: []int{1, 2, 3, 4}, wantSequence: 4, wantCommits: 1},
		{name: "local cache skips committed prefix", cached: pushTestBinlogName(2), wantFiles: []int{3, 4}, wantSequence: 4, wantCommits: 1},
		{name: "cache with different prefix is cleared", cached: "old-bin.000002", wantFiles: []int{1, 2, 3, 4}, wantSequence: 4, wantCommits: 2},
		{name: "cache ahead of local files is cleared", cached: pushTestBinlogName(100), wantFiles: []int{1, 2, 3, 4}, wantSequence: 4, wantCommits: 2},
		{name: "GTID filter skips archived prefix", archived: 2, wantFiles: []int{3, 4}, wantSequence: 4, wantCommits: 1},
		{name: "GTIDs cover all files", archived: 4, wantSequence: 4},
		{name: "local cache covers all files", cached: pushTestBinlogName(4)},
		{name: "exclusive until", until: 3, wantFiles: []int{1, 2}, wantSequence: 2, wantCommits: 1},
		{name: "upload remaining files after archived prefix", archived: 1, wantFiles: []int{2, 3, 4}, wantSequence: 4, wantCommits: 2},
		{name: "GTID checking disabled", disableGTID: true, archived: 4, wantFiles: []int{1, 2, 3, 4}, wantCommits: 2},
		{name: "disabled GTID check does not parse batch headers", disableGTID: true, badHeader: 1, wantFiles: []int{1, 2, 3, 4}, wantCommits: 2},
		{name: "disabled GTID check does not open successor", disableGTID: true, missingSuccessor: true, wantFiles: []int{1, 2, 3, 4}, wantCommits: 2},
		{name: "metadata error does not stop later batches", badHeader: 1, wantFiles: []int{1, 2, 3, 4}, wantSequence: 4, wantCommits: 2},
		{name: "missing successor preserves preceding checkpoint", missingSuccessor: true, wantFiles: []int{1, 2, 3, 4}, wantSequence: 2, wantCommits: 2},
		{name: "recover partial upload before new batches", pending: true, archived: 2, wantFiles: []int{3, 4}, wantSequence: 4, wantCommits: 1},
		{name: "recovery runs with GTID check disabled", pending: true, disableGTID: true, archived: 2, wantFiles: []int{1, 2, 3, 4}, wantCommits: 2},
		{name: "recover even without new uploads", pending: true, archived: 4, wantSequence: 4},
		{name: "invalid JSON initializes checkpoint", badJSON: true, wantFiles: []int{1, 2, 3, 4}, wantSequence: 4, wantCommits: 2},
		{name: "invalid GTID initializes checkpoint", badGTID: true, wantFiles: []int{1, 2, 3, 4}, wantSequence: 4, wantCommits: 2},
		{name: "active file only", localCount: 1},
		{name: "no binlogs", noBinlogs: true},
		{
			name: "disabled GTID check preserves metadata without uploads", disableGTID: true,
			cached: pushTestBinlogName(4), archived: 2, wantSequence: 2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			cacheDir := t.TempDir()
			cacheFilename := filepath.Join(cacheDir, BinlogCacheFileName)
			count := tc.localCount
			if count == 0 && !tc.noBinlogs {
				count = 5
			}
			names := preparePushTestBinlogs(t, dir, count)
			if tc.badHeader > 0 {
				require.NoError(t, os.WriteFile(filepath.Join(dir, names[tc.badHeader-1]), []byte("invalid"), 0600))
			}
			if tc.missingSuccessor {
				require.NoError(t, os.Remove(filepath.Join(dir, names[len(names)-1])))
			}
			cache := LogsCache{LastArchivedBinlog: tc.cached}
			if tc.cached != "" {
				putCache(cache, cacheDir)
			}
			lastSavedCache := tc.cached
			batchSize := tc.batchSize
			if batchSize == 0 {
				batchSize = 2
			}
			base := memory.NewFolder("archive/", memory.NewKVS())
			sentinel := BinlogSentinelDto{GTIDArchived: pushTestPosition(tc.archived)}
			if tc.badGTID {
				sentinel.GTIDArchived = "invalid"
			}
			abandoned := BinlogPath + "old-bin.000001.br"
			if tc.pending {
				sentinel.Intent = &BinlogUploadIntent{Files: []BinlogUploadFile{
					{Name: "old-bin.000001", ObjectName: abandoned},
					{Name: "old-bin.000002", ObjectName: BinlogPath + "old-bin.000002.br"},
				}}
				require.NoError(t, base.PutObject(t.Context(), abandoned, bytes.NewBufferString("partial upload")))
			}
			// Leave the sentinel absent on first upload; exercise legacy and pending sentinels in other cases.
			if tc.archived > 0 || tc.pending || tc.badGTID {
				require.NoError(t, UploadBinlogSentinel(t.Context(), base, &sentinel))
			}
			if tc.badJSON {
				require.NoError(t, base.PutObject(t.Context(), BinlogSentinelPath, bytes.NewBufferString(`{"GtidArchived":"invalid"`)))
			}
			var events []string
			var uploaded []int
			var intents, commits int
			var activeIntent *BinlogUploadIntent
			folder := &binlogPushTestFolder{Folder: base}
			folder.beforePut = func(name string, data []byte) error {
				if name != "archive/"+BinlogSentinelPath {
					require.NotNil(t, activeIntent, "payload must have a published intent")
					var stored BinlogSentinelDto
					require.NoError(t, FetchBinlogSentinel(t.Context(), base, &stored))
					require.Equal(t, activeIntent, stored.Intent)
					matching := false
					for _, file := range activeIntent.Files {
						matching = matching || name == "archive/"+file.ObjectName
					}
					require.True(t, matching, "intent must record the actual compressed object name")
					if tc.pending {
						exists, err := base.Exists(t.Context(), abandoned)
						require.NoError(t, err)
						require.False(t, exists, "recovery must finish before new uploads")
					}
					uploaded = append(uploaded, BinlogNum(utility.TrimFileExtension(path.Base(name))))
					events = append(events, "payload")
					return nil
				}
				var next BinlogSentinelDto
				require.NoError(t, json.Unmarshal(data, &next))
				if next.Intent != nil {
					intents++
					activeIntent = next.Intent
					for _, file := range activeIntent.Files {
						require.NotEqual(t, names[len(names)-1], file.Name, "successor must not enter intent")
					}
					events = append(events, "intent")
				} else if activeIntent != nil {
					commits++
					for _, file := range activeIntent.Files {
						exists, err := base.Exists(t.Context(), file.ObjectName)
						require.NoError(t, err)
						require.True(t, exists, "every payload must exist before commit")
					}
					activeIntent = nil
					events = append(events, "commit")
				} else {
					require.True(t, tc.pending, "only recovery can clear intent before the first batch")
					events = append(events, "recovery")
				}
				// The cache must still describe the preceding batch while a sentinel write is in progress.
				if lastSavedCache == "" {
					_, err := os.Stat(cacheFilename)
					require.ErrorIs(t, err, os.ErrNotExist)
				} else {
					require.Equal(t, lastSavedCache, getCache(cacheDir).LastArchivedBinlog)
				}
				if next.Intent == nil && commits > 0 {
					lastSavedCache = pushTestBinlogName(uploaded[len(uploaded)-1])
				}
				return nil
			}
			until := ""
			if tc.until > 0 {
				until = pushTestBinlogName(tc.until)
			}
			err := pushBinlogs(t.Context(), internal.NewRegularUploader(lz4.Compressor{}, folder), dir, names,
				mysql.MySQLFlavor, until, !tc.disableGTID, cacheDir, batchSize)
			require.NoError(t, err)
			require.Equal(t, tc.wantFiles, uploaded)
			require.Equal(t, tc.wantCommits, intents)
			require.Equal(t, tc.wantCommits, commits)
			var expectedEvents []string
			if tc.pending {
				expectedEvents = append(expectedEvents, "recovery")
			}
			for start := 0; start < len(tc.wantFiles); start += batchSize {
				expectedEvents = append(expectedEvents, "intent")
				for i := start; i < min(start+batchSize, len(tc.wantFiles)); i++ {
					expectedEvents = append(expectedEvents, "payload")
				}
				expectedEvents = append(expectedEvents, "commit")
			}
			require.Equal(t, expectedEvents, events)
			if len(tc.wantFiles) > 0 || tc.archived > 0 || tc.pending {
				var committed BinlogSentinelDto
				require.NoError(t, FetchBinlogSentinel(t.Context(), base, &committed))
				require.Nil(t, committed.Intent)
				require.Equal(t, pushTestPosition(tc.wantSequence), committed.GTIDArchived)
			}
			if len(tc.wantFiles) > 0 {
				require.Equal(t, pushTestBinlogName(tc.wantFiles[len(tc.wantFiles)-1]), getCache(cacheDir).LastArchivedBinlog)
			} else if tc.cached == "" {
				_, err := os.Stat(cacheFilename)
				require.ErrorIs(t, err, os.ErrNotExist, "no upload must not create a cache file")
			} else {
				require.Equal(t, tc.cached, getCache(cacheDir).LastArchivedBinlog)
			}
			// Verify the real uploader preserved payloads, including their compressed representation.
			for _, number := range tc.wantFiles {
				name := pushTestBinlogName(number)
				reader, err := base.ReadObject(t.Context(), BinlogPath+name+".lz4")
				require.NoError(t, err)
				decoded, err := (lz4.Decompressor{}).Decompress(reader)
				require.NoError(t, err)
				got, err := io.ReadAll(decoded)
				require.NoError(t, err)
				require.NoError(t, decoded.Close())
				require.NoError(t, reader.Close())
				want, err := os.ReadFile(filepath.Join(dir, name))
				require.NoError(t, err)
				require.Equal(t, want, got)
			}
		})
	}
}

func TestGetBinlogBatchSize(t *testing.T) {
	for _, value := range []string{"1", "5", "10", "0", "-1", "aaa"} {
		t.Run(value, func(t *testing.T) {
			setBinlogPushTestSetting(t, conf.MysqlBinlogBatchSize, value)
			got, err := getBinlogBatchSize()
			want, parseErr := strconv.Atoi(value)
			if parseErr != nil || want <= 0 {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, want, got)
			}
		})
	}
}
