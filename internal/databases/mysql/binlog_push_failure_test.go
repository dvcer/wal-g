package mysql

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression/lz4"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestPushBinlogsFailuresAndRetry(t *testing.T) {
	cases := []struct {
		name          string
		pending       bool
		missingFile   bool
		wantFiles     []int
		wantIntent    []int
		wantSequence  int
		wantCache     string
		wantOldObject bool
	}{
		{name: "intent write", wantSequence: 0},
		{name: "intent response lost", wantIntent: []int{1, 2}, wantSequence: 0},
		{name: "first payload", wantIntent: []int{1, 2}, wantSequence: 0},
		{name: "second payload", wantFiles: []int{1}, wantIntent: []int{1, 2}, wantSequence: 0},
		{name: "payload response lost", wantFiles: []int{1, 2}, wantIntent: []int{1, 2}, wantSequence: 0},
		{name: "first commit", wantFiles: []int{1, 2}, wantIntent: []int{1, 2}, wantSequence: 0},
		{name: "commit response lost", wantFiles: []int{1, 2}, wantSequence: 2},
		{name: "second batch payload", wantFiles: []int{1, 2, 3}, wantIntent: []int{3, 4}, wantSequence: 2, wantCache: pushTestBinlogName(2)},
		{name: "second commit", wantFiles: []int{1, 2, 3, 4}, wantIntent: []int{3, 4}, wantSequence: 2, wantCache: pushTestBinlogName(2)},
		{name: "sentinel read", wantSequence: 0},
		{name: "recovery deletion", pending: true, wantSequence: 0, wantOldObject: true},
		{name: "recovery commit", pending: true, wantSequence: 0},
		{name: "batch open", missingFile: true, wantSequence: 0},
	}
	for _, checkGTIDs := range []bool{true, false} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("checkGTIDs=%t/%s", checkGTIDs, tc.name), func(t *testing.T) {
				localDir, cacheDir := t.TempDir(), t.TempDir()
				binlogs := preparePushTestBinlogs(t, localDir, 5)
				if tc.missingFile {
					require.NoError(t, os.Remove(filepath.Join(localDir, pushTestBinlogName(2))))
				}
				folder := memory.NewFolder("", memory.NewKVS())
				sentinel := BinlogSentinelDto{GTIDArchived: sidB + ":1"}
				abandoned := BinlogPath + "old-bin.000001.br"
				if tc.pending {
					sentinel.Intent = &BinlogUploadIntent{Files: []BinlogUploadFile{{Name: "old-bin.000001", ObjectName: abandoned}}}
					require.NoError(t, folder.PutObject(t.Context(), abandoned, bytes.NewBufferString("partial upload")))
				}
				require.NoError(t, UploadBinlogSentinel(t.Context(), folder, &sentinel))
				failing, injected := failBinlogPushStorage(t, folder, tc.name)
				err := pushBinlogs(t.Context(), internal.NewRegularUploader(lz4.Compressor{}, failing),
					localDir, binlogs, mysql.MySQLFlavor, "", checkGTIDs, cacheDir, 2)
				if tc.missingFile {
					require.ErrorContains(t, err, "failed to open binlog")
				} else {
					require.ErrorIs(t, err, injected)
				}
				var persisted BinlogSentinelDto
				require.NoError(t, FetchBinlogSentinel(t.Context(), folder, &persisted))
				wantSequence := tc.wantSequence
				wantArchived := sentinel.GTIDArchived
				if wantSequence > 0 {
					wantArchived = ""
					if checkGTIDs {
						wantArchived = sentinel.GTIDArchived + "," + pushTestPosition(wantSequence)
					}
				}
				require.Equal(t, wantArchived, persisted.GTIDArchived)
				if tc.pending {
					require.Equal(t, sentinel.Intent, persisted.Intent)
				} else if tc.wantIntent == nil {
					require.Nil(t, persisted.Intent)
				} else {
					var names []string
					for _, number := range tc.wantIntent {
						names = append(names, pushTestBinlogName(number))
					}
					require.Equal(t, createUploadIntent(names, "lz4"), persisted.Intent)
				}
				for i := 1; i <= 4; i++ {
					exists, err := folder.Exists(t.Context(), BinlogPath+pushTestBinlogName(i)+".lz4")
					require.NoError(t, err)
					require.Equal(t, slices.Contains(tc.wantFiles, i), exists, "payload %d", i)
				}
				exists, err := folder.Exists(t.Context(), abandoned)
				require.NoError(t, err)
				require.Equal(t, tc.wantOldObject, exists)
				if tc.wantCache == "" {
					_, err := os.Stat(filepath.Join(cacheDir, BinlogCacheFileName))
					require.ErrorIs(t, err, os.ErrNotExist, "failed batch must not advance the cache")
				} else {
					require.Equal(t, tc.wantCache, getCache(cacheDir).LastArchivedBinlog)
				}
				if tc.missingFile {
					copyPushTestBinlog(t, filepath.Join(localDir, pushTestBinlogName(2)))
				}
				// A subsequent invocation reloads intent, rolls back its objects, and finishes all batches.
				var retried []string
				retryFolder := &binlogPushTestFolder{Folder: folder, beforePut: func(name string, _ []byte) error {
					if name != BinlogSentinelPath {
						retried = append(retried, name)
					}
					return nil
				}}
				err = pushBinlogs(t.Context(), internal.NewRegularUploader(lz4.Compressor{}, retryFolder),
					localDir, binlogs, mysql.MySQLFlavor, "", checkGTIDs, cacheDir, 2)
				require.NoError(t, err)
				if tc.wantCache != "" || tc.name == "commit response lost" && checkGTIDs {
					require.NotContains(t, retried, BinlogPath+pushTestBinlogName(1)+".lz4")
					require.NotContains(t, retried, BinlogPath+pushTestBinlogName(2)+".lz4")
				}
				var committed BinlogSentinelDto
				require.NoError(t, FetchBinlogSentinel(t.Context(), folder, &committed))
				require.Nil(t, committed.Intent)
				wantArchived = ""
				if checkGTIDs {
					wantArchived = sentinel.GTIDArchived + "," + pushTestPosition(4)
				}
				require.Equal(t, wantArchived, committed.GTIDArchived)
				require.Equal(t, pushTestBinlogName(4), getCache(cacheDir).LastArchivedBinlog)
				for i := 1; i <= 4; i++ {
					exists, err := folder.Exists(t.Context(), BinlogPath+pushTestBinlogName(i)+".lz4")
					require.NoError(t, err)
					require.True(t, exists)
				}
				exists, err = folder.Exists(t.Context(), abandoned)
				require.NoError(t, err)
				require.False(t, exists)
			})
		}
	}
}

func failBinlogPushStorage(t *testing.T, base storage.Folder, failure string) (*binlogPushTestFolder, error) {
	t.Helper()
	injected := errors.New("injected " + failure)
	folder := &binlogPushTestFolder{Folder: base}
	if failure == "sentinel read" {
		folder.readError = injected
	}
	if failure == "recovery deletion" {
		folder.beforeDelete = func([]storage.Object) error { return injected }
	}
	var intents, commits int
	folder.beforePut = func(name string, data []byte) error {
		if name == BinlogSentinelPath {
			var sentinel BinlogSentinelDto
			require.NoError(t, json.Unmarshal(data, &sentinel))
			if sentinel.Intent != nil {
				intents++
				if failure == "intent write" {
					return injected
				}
				if failure == "intent response lost" {
					if err := base.PutObject(t.Context(), name, bytes.NewReader(data)); err != nil {
						return err
					}
					return injected
				}
			} else {
				commits++
				if failure == "commit response lost" && commits == 1 {
					if err := base.PutObject(t.Context(), name, bytes.NewReader(data)); err != nil {
						return err
					}
					return injected
				}
				if failure == "first commit" && commits == 1 ||
					failure == "second commit" && commits == 2 ||
					failure == "recovery commit" && intents == 0 {
					return injected
				}
			}
			return nil
		}
		switch {
		case failure == "first payload" && name == BinlogPath+pushTestBinlogName(1)+".lz4":
			return injected
		case failure == "second payload" && name == BinlogPath+pushTestBinlogName(2)+".lz4":
			return injected
		case failure == "second batch payload" && name == BinlogPath+pushTestBinlogName(4)+".lz4":
			return injected
		case failure == "payload response lost" && name == BinlogPath+pushTestBinlogName(2)+".lz4":
			// The object reached storage, but the client saw an error.
			if err := base.PutObject(t.Context(), name, bytes.NewReader(data)); err != nil {
				return err
			}
			return injected
		}
		return nil
	}
	return folder, injected
}
