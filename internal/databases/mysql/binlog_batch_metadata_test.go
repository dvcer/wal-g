package mysql

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
)

func TestApplyBatchMetadata(t *testing.T) {
	var anotherServerUuid = "00000000-0000-0000-0000-0000000000bb"
	cases := []struct {
		name                 string
		gtidArchivedInitial  string
		uploadedBinlogFiles  []string
		successorFile        string
		expectedGtidArchived string
	}{
		{
			name:                 "initialize checkpoint",
			gtidArchivedInitial:  "",
			uploadedBinlogFiles:  []string{binlog3, binlog4},
			successorFile:        binlog5,
			expectedGtidArchived: testDataBinlogServerUuid + ":3-4",
		},
		{
			name:                 "advance checkpoint across uploaded batch",
			gtidArchivedInitial:  testDataBinlogServerUuid + ":1",
			uploadedBinlogFiles:  []string{binlog2, binlog3},
			successorFile:        binlog4,
			expectedGtidArchived: testDataBinlogServerUuid + ":1-3",
		},
		{
			name:                 "preserve gaps",
			gtidArchivedInitial:  testDataBinlogServerUuid + ":1",
			uploadedBinlogFiles:  []string{binlog4},
			successorFile:        binlog5,
			expectedGtidArchived: testDataBinlogServerUuid + ":1:4",
		},
		{
			name:                 "preserve transactions from another UUID",
			gtidArchivedInitial:  anotherServerUuid + ":1-5",
			uploadedBinlogFiles:  []string{binlog1, binlog2},
			successorFile:        binlog3,
			expectedGtidArchived: anotherServerUuid + ":1-5," + testDataBinlogServerUuid + ":1-2",
		},
		{
			name:                 "empty batch leaves checkpoint unchanged",
			gtidArchivedInitial:  testDataBinlogServerUuid + ":1",
			uploadedBinlogFiles:  []string{},
			successorFile:        binlog2,
			expectedGtidArchived: testDataBinlogServerUuid + ":1",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			filenames := make([]string, 0, len(tc.uploadedBinlogFiles))
			for _, filename := range tc.uploadedBinlogFiles {
				filenames = append(filenames, filepath.Join("testdata/binlogs", filename))
			}
			files := openPushTestFiles(t, filenames...)
			sentinel := BinlogSentinelDto{GTIDArchived: tc.gtidArchivedInitial}
			err := updateSentinelMetadata(&sentinel, files, filepath.Join("testdata/binlogs", tc.successorFile), mysql.MySQLFlavor, true)
			require.NoError(t, err)
			require.Equal(t, tc.expectedGtidArchived, sentinel.GTIDArchived)
		})
	}
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
