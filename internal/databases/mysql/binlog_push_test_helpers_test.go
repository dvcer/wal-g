package mysql

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const testDataBinlogServerUuid = "da187401-e4f3-11f1-9478-92bd74c99a74"

func copyPushTestBinlog(t *testing.T, filename string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata/binlogs", filepath.Base(filename)))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filename, data, 0600))
}

func openPushTestFiles(t *testing.T, filenames ...string) []*os.File {
	t.Helper()
	files := make([]*os.File, 0, len(filenames))
	for _, filename := range filenames {
		file, err := os.Open(filename)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, file.Close()) })
		files = append(files, file)
	}
	return files
}

type binlogPushTestFolder struct {
	storage.Folder
	beforePut    func(string, []byte) error
	beforeDelete func([]storage.Object) error
	readError    error
}

func (f *binlogPushTestFolder) PutObject(ctx context.Context, name string, reader io.Reader) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	if f.beforePut != nil {
		if err := f.beforePut(filepath.ToSlash(filepath.Join(f.GetPath(), name)), data); err != nil {
			return err
		}
	}
	return f.Folder.PutObject(ctx, name, bytes.NewReader(data))
}

func (f *binlogPushTestFolder) DeleteObjects(ctx context.Context, objects []storage.Object) error {
	if f.beforeDelete != nil {
		if err := f.beforeDelete(objects); err != nil {
			return err
		}
	}
	return f.Folder.DeleteObjects(ctx, objects)
}

func (f *binlogPushTestFolder) ReadObject(ctx context.Context, name string) (io.ReadCloser, error) {
	if f.readError != nil {
		return nil, f.readError
	}
	return f.Folder.ReadObject(ctx, name)
}

func (f *binlogPushTestFolder) GetSubFolder(name string) storage.Folder {
	child := *f
	child.Folder = f.Folder.GetSubFolder(name)
	return &child
}

func pushTestPosition(sequence int) string {
	if sequence == 0 {
		return ""
	}
	if sequence == 1 {
		return testDataBinlogServerUuid + ":1"
	}
	return fmt.Sprintf("%s:1-%d", testDataBinlogServerUuid, sequence)
}

func pushTestBinlogName(number int) string { return fmt.Sprintf("mysql-bin.%06d", number) }

func preparePushTestBinlogs(t *testing.T, dir string, count int) []string {
	t.Helper()
	names := make([]string, 0, count)
	for i := 1; i <= count; i++ {
		name := pushTestBinlogName(i)
		copyPushTestBinlog(t, filepath.Join(dir, name))
		names = append(names, name)
	}
	return names
}
