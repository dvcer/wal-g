package mysql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression/lz4"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	memorymock "github.com/wal-g/wal-g/pkg/storages/memory/mock"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const (
	testDataBinlogServerUuid = "da187401-e4f3-11f1-9478-92bd74c99a74"
	binlog1                  = "mysql-bin.000001"
	binlog2                  = "mysql-bin.000002"
	binlog3                  = "mysql-bin.000003"
	binlog4                  = "mysql-bin.000004"
	binlog5                  = "mysql-bin.000005"
	binlog6                  = "mysql-bin.000006"
)

var allBinlogs = []string{binlog1, binlog2, binlog3, binlog4, binlog5, binlog6}

var (
	getErr    = errors.New("injected GET failure")
	putErr    = errors.New("injected PUT failure")
	deleteErr = errors.New("injected DELETE failure")
)

// Positive scenario: the sentinel file does not exist.
func TestPushBinlogsWithoutSentinel(t *testing.T) {
	folder := setupFolder(t, nil)
	cacheFileName := filepath.Join(t.TempDir(), BinlogCacheFileName)

	clearFolderMocks(folder)
	uploadCtx := setupUploadContext(folder, cacheFileName)

	err := pushBinlogs(t.Context(), uploadCtx, allBinlogs)

	require.NoError(t, err)
	assertBinlogsInStorage(t, folder, binlog1, binlog2, binlog3, binlog4, binlog5)
	assertSentinelGTIDArchived(t, folder, testDataBinlogServerUuid+":1-5")
	assertNoSentinelIntent(t, folder)
	assertSentinelVersion(t, folder, BinlogSentinelVersion)
}

// Positive scenario: update existing metadata in sentinel
func TestPushBinlogsPreservesAnotherUUID(t *testing.T) {
	initialGtidArchived := "00000000-0000-0000-0000-0000000000bb:1-3"
	initialSentinel := &BinlogSentinelDto{GTIDArchived: initialGtidArchived}
	folder := setupFolder(t, initialSentinel)
	cacheFileName := filepath.Join(t.TempDir(), BinlogCacheFileName)

	clearFolderMocks(folder)
	uploadCtx := setupUploadContext(folder, cacheFileName)

	err := pushBinlogs(t.Context(), uploadCtx, allBinlogs)

	require.NoError(t, err)
	assertBinlogsInStorage(t, folder, binlog1, binlog2, binlog3, binlog4, binlog5)
	assertSentinelGTIDArchived(t, folder, initialGtidArchived+","+testDataBinlogServerUuid+":1-5")
	assertNoSentinelIntent(t, folder)
	assertSentinelVersion(t, folder, BinlogSentinelVersion)
}

// Recovery from a failed binlog upload.
func TestPushBinlogsRecoveryFromUploadFailed(t *testing.T) {
	folder := setupFolder(t, &BinlogSentinelDto{})
	cacheFileName := filepath.Join(t.TempDir(), BinlogCacheFileName)

	// Fail uploading binlog 4, leaving the second batch uncommitted.
	clearFolderMocks(folder)
	uploadCtx := setupUploadContext(folder, cacheFileName)
	failOnPut(folder, binlog4)

	uploadErr := pushBinlogs(t.Context(), uploadCtx, allBinlogs)

	require.ErrorIs(t, uploadErr, putErr)
	assertBinlogsInStorage(t, folder, binlog1, binlog2, binlog3)
	assertSentinelGTIDArchived(t, folder, testDataBinlogServerUuid+":1-2")
	assertBinlogsInSentinelIntent(t, folder, binlog3, binlog4)
	assertSentinelVersion(t, folder, BinlogSentinelVersion)

	// Recover the unfinished batch and upload the remaining binlogs.
	clearFolderMocks(folder)
	uploadCtx = setupUploadContext(folder, cacheFileName)

	recoveryErr := pushBinlogs(t.Context(), uploadCtx, allBinlogs)

	require.NoError(t, recoveryErr)
	assertBinlogsInStorage(t, folder, binlog1, binlog2, binlog3, binlog4, binlog5)
	assertSentinelGTIDArchived(t, folder, testDataBinlogServerUuid+":1-5")
	assertNoSentinelIntent(t, folder)
	assertSentinelVersion(t, folder, BinlogSentinelVersion)
}

// Recovery with different input binlogs removes abandoned files and preserves committed files.
func TestPushBinlogsRecoveryFromUploadFailedWithDifferentBinlogs(t *testing.T) {
	folder := setupFolder(t, &BinlogSentinelDto{})
	cacheFileName := filepath.Join(t.TempDir(), BinlogCacheFileName)

	// Fail uploading binlog 4, leaving binlog 3 uncommitted.
	clearFolderMocks(folder)
	uploadCtx := setupUploadContext(folder, cacheFileName)
	failOnPut(folder, binlog4)

	uploadErr := pushBinlogs(t.Context(), uploadCtx, allBinlogs)

	require.ErrorIs(t, uploadErr, putErr)
	assertBinlogsInStorage(t, folder, binlog1, binlog2, binlog3)
	assertSentinelGTIDArchived(t, folder, testDataBinlogServerUuid+":1-2")
	assertBinlogsInSentinelIntent(t, folder, binlog3, binlog4)
	assertSentinelVersion(t, folder, BinlogSentinelVersion)

	// Retry with different input files; recovery must remove abandoned binlog 3.
	clearFolderMocks(folder)
	uploadCtx = setupUploadContext(folder, cacheFileName)

	recoveryErr := pushBinlogs(t.Context(), uploadCtx, []string{binlog4, binlog5, binlog6})

	require.NoError(t, recoveryErr)
	assertBinlogsInStorage(t, folder, binlog1, binlog2, binlog4, binlog5)
	assertSentinelGTIDArchived(t, folder, testDataBinlogServerUuid+":1-2:4-5")
	assertNoSentinelIntent(t, folder)
	assertSentinelVersion(t, folder, BinlogSentinelVersion)
}

// Recovery from PUT intent failed.
func TestPushBinlogsRecoveryFromPUTIntentFailed(t *testing.T) {
	folder := setupFolder(t, &BinlogSentinelDto{})
	cacheFileName := filepath.Join(t.TempDir(), BinlogCacheFileName)

	// Fail publishing intent before any binlogs are uploaded.
	clearFolderMocks(folder)
	uploadCtx := setupUploadContext(folder, cacheFileName)
	failOnPut(folder, BinlogSentinelPath)

	uploadErr := pushBinlogs(t.Context(), uploadCtx, allBinlogs)

	require.ErrorIs(t, uploadErr, putErr)
	assertBinlogsInStorage(t, folder)
	assertSentinelGTIDArchived(t, folder, "")
	assertNoSentinelIntent(t, folder)
	assertSentinelVersion(t, folder, BinlogSentinelVersion)

	// Retry publishing intent and upload all binlogs.
	clearFolderMocks(folder)
	uploadCtx = setupUploadContext(folder, cacheFileName)

	recoveryErr := pushBinlogs(t.Context(), uploadCtx, allBinlogs)

	require.NoError(t, recoveryErr)
	assertBinlogsInStorage(t, folder, binlog1, binlog2, binlog3, binlog4, binlog5)
	assertSentinelGTIDArchived(t, folder, testDataBinlogServerUuid+":1-5")
	assertNoSentinelIntent(t, folder)
	assertSentinelVersion(t, folder, BinlogSentinelVersion)
}

// Recovery from PUT sentinel commit failed.
func TestPushBinlogsRecoveryFromPUTSentinelCommitFailed(t *testing.T) {
	folder := setupFolder(t, &BinlogSentinelDto{})
	cacheFileName := filepath.Join(t.TempDir(), BinlogCacheFileName)

	// Upload the first batch but fail to commit its sentinel.
	clearFolderMocks(folder)
	uploadCtx := setupUploadContext(folder, cacheFileName)
	failOnSentinelCommit(folder)

	uploadErr := pushBinlogs(t.Context(), uploadCtx, allBinlogs)

	require.ErrorIs(t, uploadErr, putErr)
	assertBinlogsInStorage(t, folder, binlog1, binlog2)
	assertSentinelGTIDArchived(t, folder, "")
	assertBinlogsInSentinelIntent(t, folder, binlog1, binlog2)
	assertSentinelVersion(t, folder, BinlogSentinelVersion)

	// Roll back the uncommitted batch and retry the upload.
	clearFolderMocks(folder)
	uploadCtx = setupUploadContext(folder, cacheFileName)

	recoveryErr := pushBinlogs(t.Context(), uploadCtx, allBinlogs)

	require.NoError(t, recoveryErr)
	assertBinlogsInStorage(t, folder, binlog1, binlog2, binlog3, binlog4, binlog5)
	assertSentinelGTIDArchived(t, folder, testDataBinlogServerUuid+":1-5")
	assertNoSentinelIntent(t, folder)
	assertSentinelVersion(t, folder, BinlogSentinelVersion)
}

// Recovery from GET sentinel failed.
func TestPushBinlogsRecoveryFromGETSentinelFailed(t *testing.T) {
	folder := setupFolder(t, &BinlogSentinelDto{})
	cacheFileName := filepath.Join(t.TempDir(), BinlogCacheFileName)

	// Upload and commit binlogs 1 and 2, using binlog 3 as the successor.
	clearFolderMocks(folder)
	uploadCtx := setupUploadContext(folder, cacheFileName)

	err := pushBinlogs(t.Context(), uploadCtx, []string{binlog1, binlog2, binlog3})

	require.NoError(t, err)
	assertBinlogsInStorage(t, folder, binlog1, binlog2)
	assertSentinelGTIDArchived(t, folder, testDataBinlogServerUuid+":1-2")
	assertNoSentinelIntent(t, folder)
	assertSentinelVersion(t, folder, BinlogSentinelVersion)

	// Fail reading the sentinel before changing storage.
	clearFolderMocks(folder)
	uploadCtx = setupUploadContext(folder, cacheFileName)
	failOnGet(folder, BinlogSentinelPath)

	uploadErr := pushBinlogs(t.Context(), uploadCtx, allBinlogs)

	require.ErrorIs(t, uploadErr, getErr)
	assertBinlogsInStorage(t, folder.MemFolder, binlog1, binlog2)
	assertSentinelGTIDArchived(t, folder.MemFolder, testDataBinlogServerUuid+":1-2")
	assertNoSentinelIntent(t, folder.MemFolder)
	assertSentinelVersion(t, folder.MemFolder, BinlogSentinelVersion)

	// Read the sentinel successfully and upload the remaining binlogs.
	clearFolderMocks(folder)
	uploadCtx = setupUploadContext(folder, cacheFileName)

	recoveryErr := pushBinlogs(t.Context(), uploadCtx, allBinlogs)

	require.NoError(t, recoveryErr)
	assertBinlogsInStorage(t, folder, binlog1, binlog2, binlog3, binlog4, binlog5)
	assertSentinelGTIDArchived(t, folder, testDataBinlogServerUuid+":1-5")
	assertNoSentinelIntent(t, folder)
	assertSentinelVersion(t, folder, BinlogSentinelVersion)
}

// Recovery from DELETE during recovery failed.
func TestPushBinlogsRecoveryFromDELETEDuringRecoveryFailed(t *testing.T) {
	folder := setupFolder(t, &BinlogSentinelDto{})
	cacheFileName := filepath.Join(t.TempDir(), BinlogCacheFileName)

	// Fail uploading binlog 4, leaving an unfinished batch.
	clearFolderMocks(folder)
	uploadCtx := setupUploadContext(folder, cacheFileName)
	failOnPut(folder, binlog4)

	uploadErr := pushBinlogs(t.Context(), uploadCtx, allBinlogs)

	require.ErrorIs(t, uploadErr, putErr)
	assertBinlogsInStorage(t, folder, binlog1, binlog2, binlog3)
	assertSentinelGTIDArchived(t, folder, testDataBinlogServerUuid+":1-2")
	assertBinlogsInSentinelIntent(t, folder, binlog3, binlog4)
	assertSentinelVersion(t, folder, BinlogSentinelVersion)

	// Recover by deleting binlog 3, then fail deleting binlog 4.
	clearFolderMocks(folder)
	uploadCtx = setupUploadContext(folder, cacheFileName)
	failOnDelete(folder, binlog4)

	deleteFailure := pushBinlogs(t.Context(), uploadCtx, allBinlogs)

	require.ErrorIs(t, deleteFailure, deleteErr)
	assertBinlogsInStorage(t, folder, binlog1, binlog2)
	assertSentinelGTIDArchived(t, folder, testDataBinlogServerUuid+":1-2")
	assertBinlogsInSentinelIntent(t, folder, binlog3, binlog4)
	assertSentinelVersion(t, folder, BinlogSentinelVersion)

	// Repeat recovery and finish uploading the remaining binlogs.
	clearFolderMocks(folder)
	uploadCtx = setupUploadContext(folder, cacheFileName)

	recoveryErr := pushBinlogs(t.Context(), uploadCtx, allBinlogs)

	require.NoError(t, recoveryErr)
	assertBinlogsInStorage(t, folder, binlog1, binlog2, binlog3, binlog4, binlog5)
	assertSentinelGTIDArchived(t, folder, testDataBinlogServerUuid+":1-5")
	assertNoSentinelIntent(t, folder)
	assertSentinelVersion(t, folder, BinlogSentinelVersion)
}

// Recovery from PUT cleared intent failed during recovery.
func TestPushBinlogsRecoveryFromPUTDuringRecoveryFailed(t *testing.T) {
	folder := setupFolder(t, &BinlogSentinelDto{})
	cacheFileName := filepath.Join(t.TempDir(), BinlogCacheFileName)

	// Fail uploading binlog 4, leaving an unfinished batch.
	clearFolderMocks(folder)
	uploadCtx := setupUploadContext(folder, cacheFileName)
	failOnPut(folder, binlog4)

	uploadErr := pushBinlogs(t.Context(), uploadCtx, allBinlogs)

	require.ErrorIs(t, uploadErr, putErr)
	assertBinlogsInStorage(t, folder, binlog1, binlog2, binlog3)
	assertSentinelGTIDArchived(t, folder, testDataBinlogServerUuid+":1-2")
	assertBinlogsInSentinelIntent(t, folder, binlog3, binlog4)
	assertSentinelVersion(t, folder, BinlogSentinelVersion)

	// Delete uncommitted files but fail to clear the remote intent.
	clearFolderMocks(folder)
	uploadCtx = setupUploadContext(folder, cacheFileName)
	failOnPut(folder, BinlogSentinelPath)

	commitFailure := pushBinlogs(t.Context(), uploadCtx, allBinlogs)

	require.ErrorIs(t, commitFailure, putErr)
	assertBinlogsInStorage(t, folder, binlog1, binlog2)
	assertSentinelGTIDArchived(t, folder, testDataBinlogServerUuid+":1-2")
	assertBinlogsInSentinelIntent(t, folder, binlog3, binlog4)
	assertSentinelVersion(t, folder, BinlogSentinelVersion)

	// Repeat cleanup and upload the remaining binlogs.
	clearFolderMocks(folder)
	uploadCtx = setupUploadContext(folder, cacheFileName)

	recoveryErr := pushBinlogs(t.Context(), uploadCtx, allBinlogs)

	require.NoError(t, recoveryErr)
	assertBinlogsInStorage(t, folder, binlog1, binlog2, binlog3, binlog4, binlog5)
	assertSentinelGTIDArchived(t, folder, testDataBinlogServerUuid+":1-5")
	assertNoSentinelIntent(t, folder)
	assertSentinelVersion(t, folder, BinlogSentinelVersion)
}

// Recovery from a failed binlog upload with GTID checking disabled.
func TestPushBinlogsRecoveryFromUploadFailedWithGTIDCheckingDisabled(t *testing.T) {
	initialSentinel := &BinlogSentinelDto{GTIDArchived: testDataBinlogServerUuid + ":1-5"}
	folder := setupFolder(t, initialSentinel)
	cacheFileName := filepath.Join(t.TempDir(), BinlogCacheFileName)

	// Fail uploading binlog 4 with GTID checking disabled.
	clearFolderMocks(folder)
	uploadCtx := setupUploadContext(folder, cacheFileName)
	uploadCtx.CheckGTIDs = false
	failOnPut(folder, binlog4)

	uploadErr := pushBinlogs(t.Context(), uploadCtx, allBinlogs)

	require.ErrorIs(t, uploadErr, putErr)
	assertBinlogsInStorage(t, folder, binlog1, binlog2, binlog3)
	assertSentinelGTIDArchived(t, folder, "")
	assertBinlogsInSentinelIntent(t, folder, binlog3, binlog4)
	assertSentinelVersion(t, folder, BinlogSentinelVersion)

	// Recover the batch and continue without GTID metadata.
	clearFolderMocks(folder)
	uploadCtx = setupUploadContext(folder, cacheFileName)
	uploadCtx.CheckGTIDs = false

	recoveryErr := pushBinlogs(t.Context(), uploadCtx, allBinlogs)

	require.NoError(t, recoveryErr)
	assertBinlogsInStorage(t, folder, binlog1, binlog2, binlog3, binlog4, binlog5)
	assertSentinelGTIDArchived(t, folder, "")
	assertNoSentinelIntent(t, folder)
	assertSentinelVersion(t, folder, BinlogSentinelVersion)
}

// Recover an unfinished upload even when there are no new binlogs to upload.
func TestPushBinlogsRecoveryFromUploadFailedWithoutNewBinlogs(t *testing.T) {
	folder := setupFolder(t, &BinlogSentinelDto{})
	cacheFileName := filepath.Join(t.TempDir(), BinlogCacheFileName)

	// Fail uploading binlog 4, leaving binlog 3 uncommitted.
	clearFolderMocks(folder)
	uploadCtx := setupUploadContext(folder, cacheFileName)
	failOnPut(folder, binlog4)

	uploadErr := pushBinlogs(t.Context(), uploadCtx, allBinlogs)

	require.ErrorIs(t, uploadErr, putErr)
	assertBinlogsInStorage(t, folder, binlog1, binlog2, binlog3)
	assertSentinelGTIDArchived(t, folder, testDataBinlogServerUuid+":1-2")
	assertBinlogsInSentinelIntent(t, folder, binlog3, binlog4)
	assertSentinelVersion(t, folder, BinlogSentinelVersion)

	// Clean up the unfinished batch with no new binlogs to upload.
	clearFolderMocks(folder)
	uploadCtx = setupUploadContext(folder, cacheFileName)

	recoveryErr := pushBinlogs(t.Context(), uploadCtx, nil)

	require.NoError(t, recoveryErr)
	assertBinlogsInStorage(t, folder, binlog1, binlog2)
	assertSentinelGTIDArchived(t, folder, testDataBinlogServerUuid+":1-2")
	assertNoSentinelIntent(t, folder)
	assertSentinelVersion(t, folder, BinlogSentinelVersion)
}

// Positive scenario: upload all binlogs with GTID checking disabled.
func TestPushBinlogsWithGTIDCheckingDisabled(t *testing.T) {
	folder := setupFolder(t, nil)
	cacheFileName := filepath.Join(t.TempDir(), BinlogCacheFileName)

	clearFolderMocks(folder)
	uploadCtx := setupUploadContext(folder, cacheFileName)
	uploadCtx.CheckGTIDs = false

	err := pushBinlogs(t.Context(), uploadCtx, allBinlogs)

	require.NoError(t, err)
	assertBinlogsInStorage(t, folder, binlog1, binlog2, binlog3, binlog4, binlog5)
	assertSentinelGTIDArchived(t, folder, "")
	assertNoSentinelIntent(t, folder)
	assertSentinelVersion(t, folder, BinlogSentinelVersion)
}

func setupUploadContext(folder storage.Folder, cacheFileName string) *BinlogUploadContext {
	return &BinlogUploadContext{
		Uploader:      internal.NewRegularUploader(lz4.Compressor{}, folder),
		BinlogsFolder: "testdata/binlogs",
		Flavor:        mysql.MySQLFlavor,
		CheckGTIDs:    true,
		CacheFileName: cacheFileName,
		BatchSize:     2,
	}
}

func setupFolder(t *testing.T, initialSentinel *BinlogSentinelDto) *memorymock.Folder {
	t.Helper()
	folder := mockFolder(memory.NewFolder("", memory.NewKVS()))
	if initialSentinel != nil {
		require.NoError(t, UploadBinlogSentinel(t.Context(), folder, initialSentinel))
	}
	return folder
}

func getStorageName(binlog string) string {
	return path.Join(BinlogPath, binlog+".lz4")
}

func mockFolder(base *memory.Folder) *memorymock.Folder {
	folder := memorymock.NewFolder(base)
	folder.GetSubFolderMock = func(name string) storage.Folder {
		subfolder := mockFolder(memory.NewFolder(path.Join(base.GetPath(), name)+"/", base.KVS))
		subfolder.PutObjectMock = func(ctx context.Context, filename string, content io.Reader) error {
			return folder.PutObject(ctx, path.Join(name, filename), content)
		}
		subfolder.ReadObjectMock = func(ctx context.Context, filename string) (io.ReadCloser, error) {
			return folder.ReadObject(ctx, path.Join(name, filename))
		}
		return subfolder
	}
	return folder
}

func clearFolderMocks(folder *memorymock.Folder) {
	folder.PutObjectMock = nil
	folder.ReadObjectMock = nil
	folder.DeleteObjectsMock = nil
}

func failOnPut(folder *memorymock.Folder, filename string) {
	folder.PutObjectMock = func(ctx context.Context, name string, content io.Reader) error {
		if strings.Contains(name, filename) {
			return putErr
		}
		return folder.MemFolder.PutObject(ctx, name, content)
	}
}

func failOnGet(folder *memorymock.Folder, filename string) {
	folder.ReadObjectMock = func(ctx context.Context, name string) (io.ReadCloser, error) {
		if name == filename {
			return nil, getErr
		}
		return folder.MemFolder.ReadObject(ctx, name)
	}
}

func failOnDelete(folder *memorymock.Folder, filename string) {
	folder.DeleteObjectsMock = func(ctx context.Context, objects []storage.Object) error {
		for _, object := range objects {
			if strings.Contains(object.GetName(), filename) {
				return deleteErr
			}
		}
		return folder.MemFolder.DeleteObjects(ctx, objects)
	}
}

func failOnSentinelCommit(folder *memorymock.Folder) {
	folder.PutObjectMock = func(ctx context.Context, name string, content io.Reader) error {
		if name != BinlogSentinelPath {
			return folder.MemFolder.PutObject(ctx, name, content)
		}
		data, err := io.ReadAll(content)
		if err != nil {
			return err
		}
		var sentinel BinlogSentinelDto
		if err := json.Unmarshal(data, &sentinel); err != nil {
			return err
		}
		if sentinel.Intent == nil {
			return putErr
		}
		return folder.MemFolder.PutObject(ctx, name, bytes.NewReader(data))
	}
}

func assertBinlogsInStorage(t *testing.T, folder storage.Folder, binlogs ...string) {
	t.Helper()
	expected := make([]string, 0, len(binlogs))
	for _, binlog := range binlogs {
		expected = append(expected, path.Base(getStorageName(binlog)))
	}
	objects, subfolders, err := folder.GetSubFolder(BinlogPath).ListFolder(t.Context())
	require.NoError(t, err)
	require.Empty(t, subfolders)
	require.Len(t, objects, len(binlogs))
	actual := make([]string, 0, len(objects))
	for _, object := range objects {
		actual = append(actual, object.GetName())
	}
	require.ElementsMatch(t, expected, actual)
}

func readSentinel(t *testing.T, folder storage.Folder) BinlogSentinelDto {
	t.Helper()
	var sentinel BinlogSentinelDto
	require.NoError(t, FetchBinlogSentinel(t.Context(), folder, &sentinel))
	return sentinel
}

func assertSentinelGTIDArchived(t *testing.T, folder storage.Folder, expected string) {
	t.Helper()
	require.Equal(t, expected, readSentinel(t, folder).GTIDArchived)
}

func assertBinlogsInSentinelIntent(t *testing.T, folder storage.Folder, binlogs ...string) {
	t.Helper()
	intent := readSentinel(t, folder).Intent
	expected := &BinlogUploadIntent{Files: make([]BinlogUploadFile, 0, len(binlogs))}
	for _, binlog := range binlogs {
		expected.Files = append(expected.Files, BinlogUploadFile{Name: binlog, ObjectName: getStorageName(binlog)})
	}
	require.Equal(t, expected, intent)
}

func assertNoSentinelIntent(t *testing.T, folder storage.Folder) {
	t.Helper()
	require.Nil(t, readSentinel(t, folder).Intent)
}

func assertSentinelVersion(t *testing.T, folder storage.Folder, expected int) {
	t.Helper()
	require.Equal(t, expected, readSentinel(t, folder).Version)
}
