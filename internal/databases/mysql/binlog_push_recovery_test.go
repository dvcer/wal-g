package mysql

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestReadBinlogSentinel(t *testing.T) {
	readFailure := errors.New("storage unavailable")
	cases := []struct {
		name    string
		data    string
		missing bool
		readErr error
		want    BinlogSentinelDto
	}{
		{name: "missing sentinel", missing: true},
		{name: "legacy sentinel", data: `{"GtidArchived":"checkpoint"}`, want: BinlogSentinelDto{GTIDArchived: "checkpoint"}},
		{
			name: "sentinel with intent", data: `{"GtidArchived":"checkpoint","Intent":{"Files":[{"Name":"mysql-bin.000001","ObjectName":"binlog_005/mysql-bin.000001.br"}]}}`,
			want: BinlogSentinelDto{GTIDArchived: "checkpoint", Intent: &BinlogUploadIntent{Files: []BinlogUploadFile{{Name: "mysql-bin.000001", ObjectName: "binlog_005/mysql-bin.000001.br"}}}},
		},
		{name: "empty content", data: ""},
		{name: "missing closing brace", data: `{"GtidArchived":"checkpoint"`},
		{name: "extra closing brace", data: `{"GtidArchived":"checkpoint"}}`},
		{name: "invalid JSON", data: "not JSON"},
		{name: "wrong top level type", data: `[]`},
		{name: "wrong field type discards partially decoded metadata", data: `{"GtidArchived":"checkpoint","Intent":123}`},
		{name: "wrong intent file type", data: `{"Intent":{"Files":[123]}}`},
		{name: "storage error", readErr: readFailure},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			folder := memory.NewFolder("archive/", memory.NewKVS())
			if !tc.missing {
				require.NoError(t, folder.PutObject(t.Context(), BinlogSentinelPath, bytes.NewBufferString(tc.data)))
			}
			wrapped := &binlogPushTestFolder{Folder: folder, readError: tc.readErr}
			got, err := readBinlogSentinel(t.Context(), wrapped)
			if tc.readErr != nil {
				require.ErrorIs(t, err, tc.readErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, got)
		})
	}
}

func TestRecoveryIfNeeded(t *testing.T) {
	storageFailure := errors.New("storage unavailable")
	files := []BinlogUploadFile{
		{Name: "mysql-bin.000001", ObjectName: BinlogPath + "mysql-bin.000001.br"},
		{Name: "mysql-bin.000002", ObjectName: BinlogPath + "mysql-bin.000002.lz4"},
	}
	cases := []struct {
		name          string
		intent        *BinlogUploadIntent
		existing      []string
		deleteErrorAt int
		deleteErr     error
		commitErr     error
		wantErr       bool
	}{
		{name: "no intent"},
		{name: "empty intent", intent: &BinlogUploadIntent{}},
		{name: "no payload uploaded", intent: &BinlogUploadIntent{Files: files}},
		{name: "partial upload", intent: &BinlogUploadIntent{Files: files}, existing: []string{files[0].ObjectName}},
		{name: "whole batch uploaded", intent: &BinlogUploadIntent{Files: files}, existing: []string{files[0].ObjectName, files[1].ObjectName}},
		{
			name: "object not found is ignored", intent: &BinlogUploadIntent{Files: files}, existing: []string{files[1].ObjectName},
			deleteErrorAt: 1, deleteErr: storage.NewObjectNotFoundError(files[0].ObjectName),
		},
		{
			name: "filesystem not found is ignored", intent: &BinlogUploadIntent{Files: files}, existing: []string{files[1].ObjectName},
			deleteErrorAt: 1, deleteErr: os.ErrNotExist,
		},
		{
			name: "deletion failure retains intent", intent: &BinlogUploadIntent{Files: files}, existing: []string{files[0].ObjectName, files[1].ObjectName},
			deleteErrorAt: 1, deleteErr: storageFailure, wantErr: true,
		},
		{
			name: "partial deletion can be retried", intent: &BinlogUploadIntent{Files: files}, existing: []string{files[0].ObjectName, files[1].ObjectName},
			deleteErrorAt: 2, deleteErr: storageFailure, wantErr: true,
		},
		{
			name: "sentinel commit failure can be retried", intent: &BinlogUploadIntent{Files: files}, existing: []string{files[0].ObjectName, files[1].ObjectName},
			commitErr: storageFailure, wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			folder := memory.NewFolder("archive/", memory.NewKVS())
			sentinel := BinlogSentinelDto{GTIDArchived: sidA + ":1-10", Intent: tc.intent}
			require.NoError(t, UploadBinlogSentinel(t.Context(), folder, &sentinel))
			committedObject := BinlogPath + "mysql-bin.000000.br"
			for _, name := range append([]string{committedObject}, tc.existing...) {
				require.NoError(t, folder.PutObject(t.Context(), name, bytes.NewBufferString(name)))
			}
			var deleted []string
			var writes int
			wrapped := &binlogPushTestFolder{
				Folder: folder,
				beforeDelete: func(objects []storage.Object) error {
					require.Zero(t, writes, "all deletes must finish before clearing intent")
					for _, object := range objects {
						deleted = append(deleted, object.GetName())
					}
					if len(deleted) == tc.deleteErrorAt {
						return tc.deleteErr
					}
					return nil
				},
				beforePut: func(name string, data []byte) error {
					writes++
					require.Equal(t, "archive/"+BinlogSentinelPath, name)
					var committed BinlogSentinelDto
					require.NoError(t, json.Unmarshal(data, &committed))
					require.Nil(t, committed.Intent)
					require.Equal(t, sidA+":1-10", committed.GTIDArchived)
					return tc.commitErr
				},
			}
			err := recoveryIfNeeded(t.Context(), wrapped, &sentinel)
			var persisted BinlogSentinelDto
			require.NoError(t, FetchBinlogSentinel(t.Context(), folder, &persisted))
			if tc.wantErr {
				require.ErrorIs(t, err, storageFailure)
				require.Equal(t, tc.intent, persisted.Intent, "failed recovery must retain the remote intent")
				if tc.deleteErr != nil {
					require.Zero(t, writes, "do not commit after deletion failure")
				}
				// A new invocation reloads the persisted intent, even if the previous local DTO was cleared.
				require.NoError(t, recoveryIfNeeded(t.Context(), folder, &persisted))
			} else {
				require.NoError(t, err)
				require.Nil(t, sentinel.Intent)
				require.Nil(t, persisted.Intent)
				if tc.intent == nil {
					require.Empty(t, deleted)
					require.Zero(t, writes)
				} else {
					require.Len(t, deleted, len(tc.intent.Files))
					require.Equal(t, 1, writes)
				}
			}
			for _, file := range files {
				exists, err := folder.Exists(t.Context(), file.ObjectName)
				require.NoError(t, err)
				require.False(t, exists, file.ObjectName)
			}
			exists, err := folder.Exists(t.Context(), committedObject)
			require.NoError(t, err)
			require.True(t, exists, "recovery must leave committed files alone")
			require.NoError(t, FetchBinlogSentinel(t.Context(), folder, &persisted))
			require.Equal(t, sidA+":1-10", persisted.GTIDArchived)
			require.Nil(t, persisted.Intent)
		})
	}
}
